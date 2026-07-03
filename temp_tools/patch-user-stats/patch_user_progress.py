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
    newly_earned = []

    for ach in all_achievements:
        ach_id = ach["_id"]
        criteria = ach.get("criteria", [])

        meets_all = True
        progress_percentages = []

        if criteria:
            for criterion in criteria:
                c_type = criterion["type"]
                threshold = criterion["threshold"]
                current_val = stats.get(c_type, 0)

                if current_val < threshold:
                    meets_all = False

                if threshold > 0:
                    progress_percentages.append(min(100, (current_val / threshold) * 100))
                else:
                    progress_percentages.append(100.0)

            progress_val = min(progress_percentages) if progress_percentages else 100
        else:
            progress_val = 100

        # Existing record
        existing = existing_achievements_map.get(str(ach_id), {})
        earned_at = existing.get("earnedAt")

        if meets_all and not earned_at:
            earned_at = datetime.now(timezone.utc)
            newly_earned.append(ach.get("name", str(ach_id)))

        if earned_at:
            total_points += ach.get("points", 0)

        updated_achievements.append({
            "achievement": ach_id,
            "earnedAt": earned_at,
            "progress": round(progress_val, 2)
        })

    return updated_achievements, total_points, newly_earned

def print_comparison(label, current, new):
    diff = new - current
    diff_str = f"(+{diff})" if diff >= 0 else f"({diff})"
    print(f"  {label:<20}: {current:>8} -> {new:>8} {diff_str:>8}")

def patch_user(user_id_str, auto_approve=False):
    db = get_db()
    try:
        user_oid = ObjectId(user_id_str)
    except Exception:
        print(f"Invalid User ID format: {user_id_str}")
        return

    print(f"\n--- Patching User: {user_id_str} ---")

    # 1. Fetch data
    observations = list(db.observations.find({"creator": user_oid}))
    if not observations:
        print("No observations found for this user. Nothing to patch.")
        return

    all_achievements = list(db.achievements.find({"isActive": True}))
    user_progress = db.userprogresses.find_one({"user": user_oid})

    # 2. Calculate New Stats
    stats_new, streaks_new = calculate_stats(observations)

    existing_achievements_map = {}
    if user_progress:
        for ach_progress in user_progress.get("achievements", []):
            ach_id = ach_progress.get("achievement")
            if isinstance(ach_id, ObjectId):
                existing_achievements_map[str(ach_id)] = ach_progress
            elif isinstance(ach_id, dict) and "$oid" in ach_id:
                existing_achievements_map[ach_id["$oid"]] = ach_progress

    updated_achievements, points_new, newly_earned = evaluate_achievements(stats_new, all_achievements, existing_achievements_map)

    # 3. Show Comparison
    print("\nSTATISTICS COMPARISON:")
    stats_old = user_progress.get("stats", {}) if user_progress else {}
    streaks_old = user_progress.get("streaks", {}) if user_progress else {}
    points_old = user_progress.get("totalPoints", 0) if user_progress else 0

    metrics = [
        ("Images Reviewed", stats_old.get("imagesReviewed", 0), stats_new["imagesReviewed"]),
        ("Animals Observed", stats_old.get("animalsObserved", 0), stats_new["animalsObserved"]),
        ("Unique Species", stats_old.get("uniqueSpecies", 0), stats_new["uniqueSpecies"]),
        ("Blanks Logged", stats_old.get("blanksLogged", 0), stats_new["blanksLogged"]),
        ("Current Streak", streaks_old.get("current", 0), streaks_new["current"]),
        ("Longest Streak", streaks_old.get("longest", 0), streaks_new["longest"]),
        ("Total Points", points_old, points_new),
    ]

    for label, old, new in metrics:
        print_comparison(label, old, new)

    if newly_earned:
        print("\nNEWLY EARNED ACHIEVEMENTS:")
        for name in newly_earned:
            print(f"  + {name}")
    else:
        print("\nNo new achievements earned.")

    # 4. Confirmation
    if not auto_approve:
        response = input("\nDo you want to apply these changes? [y/N]: ").lower()
        if response != 'y':
            print("Operation cancelled.")
            return

    # 5. Update Database
    update_data = {
        "$set": {
            "stats.imagesReviewed": stats_new["imagesReviewed"],
            "stats.animalsObserved": stats_new["animalsObserved"],
            "stats.uniqueSpecies": stats_new["uniqueSpecies"],
            "stats.blanksLogged": stats_new["blanksLogged"],
            "stats.consecutiveDays": streaks_new["current"],
            "streaks.longest": streaks_new["longest"],
            "streaks.current": streaks_new["current"],
            "achievements": updated_achievements,
            "totalPoints": points_new,
            "domainRanks.CAMERATRAP.points": points_new,
            "updatedAt": datetime.now(timezone.utc)
        }
    }

    result = db.userprogresses.update_one({"user": user_oid}, update_data, upsert=True)

    if result.matched_count > 0:
        print(f"\nSuccessfully updated userprogress for {user_id_str}")
    elif result.upserted_id:
        print(f"\nSuccessfully created new userprogress for {user_id_str}")
    else:
        print(f"\nNo changes needed for user {user_id_str}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Patch user progress stats from observation history and achievements.")
    parser.add_argument("user_id", help="The MongoDB ObjectId of the user to patch.")
    parser.add_argument("--yes", action="store_true", help="Automatically approve the changes.")
    args = parser.parse_args()

    patch_user(args.user_id, auto_approve=args.yes)
