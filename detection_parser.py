import json
from pathlib import Path
from typing import Dict, Optional
import sys
from datetime import datetime

def parse_detections(json_file: str, num_samples: Optional[int] = None, run_date: str = None) -> Dict[str, Dict]:
    """
    Parse detection results from the master JSON file and format for MongoDB upload.

    Args:
        json_file (str): Path to the master JSON file
        num_samples (int, optional): Number of samples to process. If None, process all.
        run_date (str): Date when the model was run (YYYY-MM-DD format), required

    Returns:
        Dict[str, Dict]: Dictionary formatted for MongoDB with mediaIDs as keys
    """
    if not run_date:
        raise ValueError("run_date is required")

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

    # Process each prediction
    for pred in predictions:
        # Extract mediaID from filepath (filename without extension)
        media_id = Path(pred["filepath"]).name.split(".")[0]

        # Initialize confidence scores
        conf_blank = 0.0
        conf_human = 0.0
        conf_animal = 0.0

        detections = pred.get("detections", [])

        # Count detections by type
        animal_confs = []
        human_confs = []

        for det in detections:
            if det["label"] == "animal":
                animal_confs.append(float(det["conf"]))
            elif det["label"] == "human":
                human_confs.append(float(det["conf"]))        
        
        # Process animal detections
        if animal_confs:
            max_animal_conf = max(animal_confs)
            conf_animal = round(max_animal_conf, 2)
            conf_blank = round(1 - max_animal_conf, 2)

        # Process human detections
        if human_confs:
            max_human_conf = max(human_confs)
            conf_human = round(max_human_conf, 2)
            conf_blank = round(1 - max_human_conf, 2) # Overwrites conf_blank if there is a human
        
        # If no detections, set blank confidence to 1
        if not animal_confs and not human_confs:
            conf_blank = round(1.0, 2)

        # Create MongoDB document with camelCase keys
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
    # Check command line arguments
    if len(sys.argv) != 2:
        print("Usage: python detection_parser.py <path_to_json_file>")
        print("\nExample:")
        print("python detection_parser.py ./cv_results/2025-05-24_speciesnet_v4.0.1a_nogeo.json")
        sys.exit(1)

    # Get file path from command line and validate it exists
    json_file = sys.argv[1]
    if not Path(json_file).exists():
        print(f"Error: File not found: {json_file}")
        sys.exit(1)

    # Get run date (required)
    while True:
        run_date = input("\nEnter the model run date (YYYY-MM-DD): ")
        try:
            # Validate date format
            run_date = datetime.strptime(run_date, "%Y-%m-%d").strftime("%Y-%m-%d")
            break
        except ValueError:
            print("Error: Please enter the date in YYYY-MM-DD format")

    try:
        # First test with 5 samples
        print("\nTesting with 5 sample records...")
        results = parse_detections(json_file, num_samples=5, run_date=run_date)

        # Print sample outputs
        print("\nSample outputs:")
        for media_id, result in results.items():
            print(f"\nmediaID: {media_id}")
            print(json.dumps(result, indent=2))

        # Prompt to process all records
        save_all = input("\nDo you want to process and save all records? (y/n): ")
        if save_all.lower() == "y":
            print("\nProcessing all records...")
            full_results = parse_detections(json_file, run_date=run_date)
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
