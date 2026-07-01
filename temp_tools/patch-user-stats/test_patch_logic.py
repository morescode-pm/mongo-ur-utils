import json
from patch_user_progress import calculate_stats, evaluate_achievements

def test_patch_logic():
    # Load sample data
    with open("background/test.observations.json", "r") as f:
        observations = json.load(f)
    with open("background/test.achievements.json", "r") as f:
        all_achievements = json.load(f)

    # Filter for the test user
    user_id = "69fc34d704c822bc61075209"
    user_obs = [o for o in observations if o.get("creator", {}).get("$oid") == user_id]

    print(f"Testing logic with {len(user_obs)} observations...")

    stats, streaks = calculate_stats(user_obs)

    # Assertions based on manual analysis
    print(f"Calculated Stats: {stats}")
    print(f"Calculated Streaks: {streaks}")

    assert stats["imagesReviewed"] == 12178, f"Expected 12178 images, got {stats['imagesReviewed']}"
    assert stats["animalsObserved"] == 30307, f"Expected 30307 animals, got {stats['animalsObserved']}"
    assert stats["uniqueSpecies"] == 51, f"Expected 51 species, got {stats['uniqueSpecies']}"
    assert stats["blanksLogged"] == 1268, f"Expected 1268 blanks, got {stats['blanksLogged']}"
    assert streaks["longest"] == 9, f"Expected longest streak 9, got {streaks['longest']}"

    # Test Achievement evaluation
    updated_achievements, total_points = evaluate_achievements(stats, all_achievements, {})

    print(f"Total Points: {total_points}")
    # Expected points based on stats:
    # Virtue Signaler (100): images >= 100 and animals >= 3 -> EARNED
    # Ten-bagger (120): uniqueSpecies >= 10 -> EARNED
    # Digital Time-Waster (500): images >= 5000 -> EARNED
    # Hustler (300): images >= 1000 -> EARNED
    # Busy Beaver (700): images >= 9000 -> EARNED
    # River Guardian (900): images >= 15000 -> NOT EARNED (12178 < 15000)
    # Getting Started (10): images >= 10 and uniqueSpecies >= 5 -> EARNED
    # Candy Corn (100): blanks >= 1000 -> EARNED
    # Gummy Bear (200): blanks >= 2000 -> NOT EARNED (1268 < 2000)
    # Licorice Legend (400): blanks >= 4000 -> NOT EARNED
    # the G.O.A.T (1200): images >= 20000 -> NOT EARNED

    # Expected points: 100 + 120 + 500 + 300 + 700 + 10 + 100 = 1830
    assert total_points == 1830, f"Expected 1830 points, got {total_points}"

    # Test ZeroDivisionError fix with a mock zero-threshold achievement
    zero_ach = {
        "_id": "zero_id",
        "points": 50,
        "criteria": [{"type": "imagesReviewed", "threshold": 0}]
    }
    updated_ach_zero, total_points_zero = evaluate_achievements(stats, all_achievements + [zero_ach], {})
    assert total_points_zero == 1830 + 50
    zero_entry = next(a for a in updated_ach_zero if a["achievement"] == "zero_id")
    assert zero_entry["progress"] == 100.0
    assert zero_entry["earnedAt"] is not None

    # Check progress for a partially completed one (River Guardian)
    def get_id_str(ach_id):
        if isinstance(ach_id, dict) and "$oid" in ach_id:
            return ach_id["$oid"]
        return str(ach_id)

    river_guardian = next(a for a in updated_achievements if get_id_str(a["achievement"]) == "67f184860fe8cfe6859dc421")
    expected_progress = round((12178 / 15000) * 100, 2)
    assert river_guardian["progress"] == expected_progress, f"Expected {expected_progress}% for River Guardian, got {river_guardian['progress']}%"

    print("All test assertions passed!")

if __name__ == "__main__":
    test_patch_logic()
