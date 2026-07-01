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

def evaluate_achievements(stats, all_achievements, existing_achievements_map):
    """
    Evaluates achievement progress based on stats.
    Returns a list of achievement progress documents for UserProgress.
    """
    updated_achievements = []
    total_points = 0

    for ach in all_achievements:
        ach_id = ach["_id"]
        criteria = ach.get("criteria", [])

        # Calculate progress: minimum percentage towards all criteria
        # Or if we just need a absolute progress number, it's often the sum or min of raw values
        # Looking at the sample, "progress" seems to be a single number.
        # Let's assume progress is the minimum achievement of any single criteria threshold.

        meets_all = True
        progress_val = 100 # Default if no criteria?

        if criteria:
            progress_percentages = []
            for criterion in criteria:
                c_type = criterion["type"]
                threshold = criterion["threshold"]
                current_val = stats.get(c_type, 0)

                if current_val < threshold:
                    meets_all = False

                # Progress as a percentage capped at 100
                progress_percentages.append(min(100, (current_val / threshold) * 100))

            progress_val = min(progress_percentages) if progress_percentages else 100

        # Existing record
        existing = existing_achievements_map.get(str(ach_id), {})
        earned_at = existing.get("earnedAt")

        if meets_all and not earned_at:
            earned_at = datetime.now(timezone.utc)

        if earned_at:
            total_points += ach.get("points", 0)

        updated_achievements.append({
            "achievement": ach_id,
            "earnedAt": earned_at,
            "progress": round(progress_val, 2)
        })

    return updated_achievements, total_points

def patch_user(user_id_str):
    db = get_db()
    try:
        user_oid = ObjectId(user_id_str)
    except Exception:
        print(f"Invalid User ID format: {user_id_str}")
        return

    print(f"Patching stats for user: {user_id_str}")

    # 1. Fetch all observations for this user
    observations = list(db.observations.find({"creator": user_oid}))
    print(f"Found {len(observations)} observations.")

    if not observations:
        print("No observations found for this user. Nothing to patch.")
        # We might still want to reset their stats to 0 if they exist
        # but usually this indicates a new user or error
        return

    # 2. Calculate Stats
    stats_update, streaks = calculate_stats(observations)

    # 3. Fetch all active achievements
    all_achievements = list(db.achievements.find({"isActive": True}))
    print(f"Found {len(all_achievements)} active achievements.")

    # 4. Fetch existing UserProgress to preserve earnedAt if possible
    user_progress = db.userprogresses.find_one({"user": user_oid})
    existing_achievements_map = {}
    if user_progress:
        for ach_progress in user_progress.get("achievements", []):
            ach_id = ach_progress.get("achievement")
            if isinstance(ach_id, ObjectId):
                existing_achievements_map[str(ach_id)] = ach_progress
            elif isinstance(ach_id, dict) and "$oid" in ach_id:
                existing_achievements_map[ach_id["$oid"]] = ach_progress

    # 5. Evaluate Achievements
    updated_achievements, total_points = evaluate_achievements(stats_update, all_achievements, existing_achievements_map)

    print("Calculated Stats:")
    for k, v in stats_update.items():
        print(f"  {k}: {v}")
    print(f"  Longest Streak: {streaks['longest']}")
    print(f"  Current Streak: {streaks['current']}")
    print(f"  Total Points (from achievements): {total_points}")

    # 6. Update UserProgress
    update_data = {
        "$set": {
            "stats.imagesReviewed": stats_update["imagesReviewed"],
            "stats.animalsObserved": stats_update["animalsObserved"],
            "stats.uniqueSpecies": stats_update["uniqueSpecies"],
            "stats.blanksLogged": stats_update["blanksLogged"],
            "stats.consecutiveDays": streaks["current"],
            "streaks.longest": streaks["longest"],
            "streaks.current": streaks["current"],
            "achievements": updated_achievements,
            "totalPoints": total_points,
            "domainRanks.CAMERATRAP.points": total_points,
            "updatedAt": datetime.now(timezone.utc)
        }
    }

    result = db.userprogresses.update_one({"user": user_oid}, update_data, upsert=True)

    if result.matched_count > 0:
        print(f"Successfully updated userprogress for {user_id_str}")
    elif result.upserted_id:
        print(f"Successfully created new userprogress for {user_id_str}")
    else:
        print(f"No changes made for user {user_id_str}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Patch user progress stats from observation history and achievements.")
    parser.add_argument("user_id", help="The MongoDB ObjectId of the user to patch.")
    args = parser.parse_args()

    patch_user(args.user_id)
