import json
from pathlib import Path
from typing import Dict, Optional
import sys
from datetime import datetime

def parse_detections(json_file: str, num_samples: Optional[int] = None) -> Dict[str, Dict]:
    """
    Parse detection results from the master JSON file and format for MongoDB upload.

    Args:
        json_file (str): Path to the master JSON file
        num_samples (int, optional): Number of samples to process. If None, process all.

    Returns:
        Dict[str, Dict]: Dictionary formatted for MongoDB with mediaIDs as keys
    """
    # Validate the file exists
    json_path = Path(json_file)
    if not json_path.exists():
        raise FileNotFoundError(f"Input file not found: {json_file}")

    # Initialize results dictionary
    results = {}

    # Read JSON file
    with open(json_path, "r") as f:
        data = json.load(f)

    # Get predictions to process
    predictions = data["predictions"][:num_samples] if num_samples else data["predictions"]

    for pred in predictions:
        media_id = Path(pred["filepath"]).name.split(".")[0]

        conf_blank = 0.0
        conf_human = 0.0
        conf_animal = 0.0

        detections = pred.get("detections", [])
        run_date = pred.get("run_date", "unknown")

        animal_confs = []
        human_confs = []

        for det in detections:
            if det["label"] == "animal":
                animal_confs.append(float(det["conf"]))
            elif det["label"] == "human":
                human_confs.append(float(det["conf"]))

        if animal_confs:
            max_animal_conf = max(animal_confs)
            conf_animal = round(max_animal_conf, 2)
            # conf_blank = round(1 - max_animal_conf, 2)

        if human_confs:
            max_human_conf = max(human_confs)
            conf_human = round(max_human_conf, 2)
            # conf_blank = round(1 - max_human_conf, 2)

        # Assign conf_blank if both human and animal confs exist
        conf_blank = max(0.0, round(1 - max(conf_human, conf_animal), 2))

        if not animal_confs and not human_confs:
            conf_blank = 1.0

        ai_model_result = {
            "modelName": "speciesnet/PyTorch/v4.0.1a",
            "runDate": run_date,
            "confBlank": conf_blank,
            "confHuman": conf_human,
            "confAnimal": conf_animal,
        }

        results[media_id] = {
            "aiResults": [ai_model_result]
        }

    return results

def main():
    """Main function for parsing detection results"""
    if len(sys.argv) != 2:
        print("Usage: python detection_parser.py <path_to_json_file>")
        sys.exit(1)

    json_file = sys.argv[1]
    if not Path(json_file).exists():
        print(f"Error: File not found: {json_file}")
        sys.exit(1)

    try:
        print("\nTesting with 5 sample records...")
        results = parse_detections(json_file, num_samples=5)

        print("\nSample outputs:")
        for media_id, result in results.items():
            print(f"\nmediaID: {media_id}")
            print(json.dumps(result, indent=2))

        save_all = input("\nDo you want to process and save all records? (y/n): ")
        if save_all.lower() == "y":
            print("\nProcessing all records...")
            full_results = parse_detections(json_file)
            output_file = "mongodb_formatted_detections.json"
            with open(output_file, "w") as f:
                json.dump(full_results, f, indent=2)
            print(f"\nResults saved to {output_file}")

    except json.JSONDecodeError:
        print(f"Error: {json_file} is not a valid JSON file")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
