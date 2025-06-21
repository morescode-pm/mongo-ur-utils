"""
Adds rundate to all fields in json file if json file is a speciesnet predictions output.

Usage:
    python add_date.py

Output:
    - JSON file specified in json_file_path will have hardcoded rundate added
"""

import json
from pathlib import Path

# === USER INPUTS ===
json_file_path = "predictions_dict_master.json"  # <-- Update this
hardcoded_rundate = "2025-05-24"  # <-- Date only

# Load existing predictions
with open(json_file_path, "r") as f:
    data = json.load(f)

# Ensure the expected structure
if "predictions" not in data:
    raise ValueError(f"'predictions' key not found in {json_file_path}")

# Add rundate if missing
updated = 0
for pred in data["predictions"]:
    if "run_date" not in pred:
        pred["run_date"] = hardcoded_rundate
        updated += 1

# Save the updated file
with open(json_file_path, "w") as f:
    json.dump(data, f, indent=2)

print(f"Updated {updated} predictions in {json_file_path} with rundate = {hardcoded_rundate}")