import json
import sys
from pathlib import Path

def parse_predictions_to_custom_format(json_file: str, output_file: str):
    """
    Parses AI detections from a JSON file and outputs them in a specific text format.

    Format:
    media_id: <media_id>
    prediction: "<prediction>"
    prediction_score: <prediction_score>,
    """
    json_path = Path(json_file)
    if not json_path.exists():
        print(f"Error: Input file {json_file} not found.")
        return

    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error: Failed to decode JSON from {json_file}: {e}")
        return

    # Handle both a single list of predictions or a dict with a 'predictions' key
    if isinstance(data, dict):
        predictions = data.get('predictions', [])
    elif isinstance(data, list):
        predictions = data
    else:
        print(f"Error: Unexpected JSON structure in {json_file}")
        return

    with open(output_file, 'w') as f:
        for pred in predictions:
            filepath = pred.get('filepath', '')
            # media_id is the filename without extension
            media_id = Path(filepath).stem
            prediction = pred.get('prediction', '')
            prediction_score = pred.get('prediction_score', '')

            f.write(f"media_id: {media_id}\n")
            f.write(f"prediction: \"{prediction}\"\n")
            f.write(f"prediction_score: {prediction_score}, \n")

def main():
    if len(sys.argv) < 2:
        print("Usage: python ai_prediction_to_csv.py <path_to_json_file> [output_file]")
        sys.exit(1)

    json_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else "parsed_predictions.txt"

    parse_predictions_to_custom_format(json_file, output_file)
    print(f"Done! Results saved to {output_file}")

if __name__ == "__main__":
    main()
