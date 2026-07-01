import os
import argparse
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient
from bson.objectid import ObjectId
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI_PROD", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGO_DB", "urbanrivers")

def get_db():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME]

def calculate_stats(observations):
    """
    Calculates user stats from a list of observation documents.
    """
    stats = {
        "imagesReviewed": 0,
        "animalsObserved": 0,
        "uniqueSpecies": 0,
        "blanksLogged": 0,
    }

    media_ids = set()
    unique_species_set = set()
    active_days = set()

    for obs in observations:
        # Images Reviewed (Unique mediaId)
        media_id = obs.get("mediaId")
        if media_id:
            media_ids.add(media_id)

        # Observation type specific logic
        obs_type = obs.get("observationType")
        if obs_type == "animal":
            stats["animalsObserved"] += obs.get("count", 0)
            sci_name = obs.get("scientificName")
            if sci_name:
                unique_species_set.add(sci_name)
        elif obs_type == "blank":
            stats["blanksLogged"] += 1

        # Track active days for streaks
        event_start = obs.get("eventStart")
        if isinstance(event_start, datetime):
            active_days.add(event_start.date())
        elif isinstance(event_start, dict) and "$date" in event_start:
            # Handle MongoDB JSON format if necessary (though pymongo returns datetime)
            dt_str = event_start["$date"]
            if dt_str.endswith("Z"):
                dt_str = dt_str[:-1]
            dt = datetime.fromisoformat(dt_str)
            active_days.add(dt.date())

    stats["imagesReviewed"] = len(media_ids)
    stats["uniqueSpecies"] = len(unique_species_set)

    # Calculate streaks
    sorted_days = sorted(list(active_days))
    longest_streak = 0
    current_streak = 0

    if sorted_days:
        temp_streak = 1
        prev_day = sorted_days[0]
        for i in range(1, len(sorted_days)):
            if (sorted_days[i] - prev_day).days == 1:
                temp_streak += 1
            else:
                longest_streak = max(longest_streak, temp_streak)
                temp_streak = 1
            prev_day = sorted_days[i]
        longest_streak = max(longest_streak, temp_streak)

        # Current streak logic:
        # If the last active day is today or yesterday, the current streak is the last temp_streak.
        # Otherwise it is 0.
        today = datetime.now(timezone.utc).date()
        if (today - sorted_days[-1]).days <= 1:
            current_streak = temp_streak
        else:
            current_streak = 0

    streaks = {
        "longest": longest_streak,
        "current": current_streak
    }

    return stats, streaks

def patch_user(user_id_str):
    db = get_db()
    try:
        user_oid = ObjectId(user_id_str)
    except Exception:
        print(f"Invalid User ID format: {user_id_str}")
        return

    print(f"Patching stats for user: {user_id_str}")

    # Fetch all observations for this user
    observations = list(db.observations.find({"creator": user_oid}))
    print(f"Found {len(observations)} observations.")

    if not observations:
        print("No observations found for this user. Nothing to patch.")
        return

    stats_update, streaks = calculate_stats(observations)

    print("Calculated Stats:")
    print(f"  Images Reviewed: {stats_update['imagesReviewed']}")
    print(f"  Animals Observed: {stats_update['animalsObserved']}")
    print(f"  Unique Species: {stats_update['uniqueSpecies']}")
    print(f"  Blanks Logged: {stats_update['blanksLogged']}")
    print(f"  Longest Streak: {streaks['longest']}")
    print(f"  Current Streak: {streaks['current']}")

    # Points logic (placeholder: 1 point per image reviewed)
    total_points = stats_update['imagesReviewed']

    # Update UserProgress
    update_data = {
        "$set": {
            "stats.imagesReviewed": stats_update["imagesReviewed"],
            "stats.animalsObserved": stats_update["animalsObserved"],
            "stats.uniqueSpecies": stats_update["uniqueSpecies"],
            "stats.blanksLogged": stats_update["blanksLogged"],
            "stats.consecutiveDays": streaks["current"],
            "streaks.longest": streaks["longest"],
            "streaks.current": streaks["current"],
            "totalPoints": total_points,
            "domainRanks.CAMERATRAP.points": total_points,
            "updatedAt": datetime.now(timezone.utc)
        }
    }

    result = db.userprogresses.update_one({"user": user_oid}, update_data)

    if result.matched_count > 0:
        print(f"Successfully updated userprogress for {user_id_str}")
    else:
        # If no document exists, we might want to create one?
        # For now, just report it.
        print(f"No userprogress document found for user {user_id_str}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Patch user progress stats from observation history.")
    parser.add_argument("user_id", help="The MongoDB ObjectId of the user to patch.")
    args = parser.parse_args()

    patch_user(args.user_id)
