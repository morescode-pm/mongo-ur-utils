import json
from patch_user_progress import calculate_stats

def test_patch_logic():
    # Load sample data
    with open("background/test.observations.json", "r") as f:
        observations = json.load(f)

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

    print("All test assertions passed!")

if __name__ == "__main__":
    test_patch_logic()
