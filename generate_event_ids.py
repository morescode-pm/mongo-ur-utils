import pandas as pd
import argparse
import uuid
from datetime import timedelta
import os # Added import

def generate_event_ids(input_file_path, output_file_path_detailed, output_file_path_summary, time_threshold_seconds):
    """
    Generates eventIDs for observations based on species and time proximity.

    Args:
        input_file_path (str): Path to the input CSV file (observations).
        output_file_path_detailed (str): Path to save the detailed output CSV with eventIDs.
        output_file_path_summary (str): Path to save the summary output CSV (optional).
        time_threshold_seconds (int): Time threshold in seconds to group observations.
    """
    print(f"Starting event ID generation with threshold: {time_threshold_seconds} seconds.")
    print(f"Input file: {input_file_path}")
    print(f"Detailed output file: {output_file_path_detailed}")
    if output_file_path_summary:
        print(f"Summary output file: {output_file_path_summary}")

    # Load the observations CSV
    try:
        df = pd.read_csv(input_file_path, low_memory=False)
        print(f"Successfully loaded {input_file_path}. Shape: {df.shape}")
    except FileNotFoundError:
        print(f"Error: Input file not found at {input_file_path}")
        return
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return

    # --- Start of Step 2: Main Logic Implementation ---

    # **Preprocessing**
    print("Preprocessing data...")
    # Convert eventStart and eventEnd to datetime objects
    # Using errors='coerce' will turn unparseable dates into NaT (Not a Time)
    df['eventStart'] = pd.to_datetime(df['eventStart'], errors='coerce')
    df['eventEnd'] = pd.to_datetime(df['eventEnd'], errors='coerce')

    # Drop rows where eventStart or eventEnd could not be parsed
    df.dropna(subset=['eventStart', 'eventEnd'], inplace=True)
    print(f"Shape after datetime conversion and dropping NaT: {df.shape}")

    # Filter out rows:
    # - Where scientificName is empty/NaN
    # - Where observationType is not 'animal' (case-insensitive)
    # - Where deploymentID is empty/NaN (important for grouping)
    df.dropna(subset=['scientificName', 'deploymentID'], inplace=True)
    df = df[df['scientificName'].str.strip() != '']
    # Ensure observationType is string before .str.lower()
    df['observationType'] = df['observationType'].astype(str)
    df = df[df['observationType'].str.lower() == 'animal']
    print(f"Shape after filtering for animal observations with scientificName and deploymentID: {df.shape}")

    if df.empty:
        print("No valid animal observations found after preprocessing. Exiting.")
        # Save empty detailed output
        df['eventID'] = None # Ensure column exists
        df.to_csv(output_file_path_detailed, index=False)
        print(f"Empty detailed output saved to {output_file_path_detailed}")
        if output_file_path_summary:
            summary_df = pd.DataFrame(columns=[
                'eventID', 'deploymentID', 'scientificName',
                'min_event_start', 'max_event_end', 'observation_count'
            ])
            summary_df.to_csv(output_file_path_summary, index=False)
            print(f"Empty summary output saved to {output_file_path_summary}")
        return

    # **Sort Data**
    # Sort by deploymentID, scientificName, and then eventStart
    print("Sorting data...")
    df.sort_values(by=['deploymentID', 'scientificName', 'eventStart'], inplace=True)

    # **Event Grouping and eventID Generation**
    print("Grouping events and generating eventIDs...")
    df['eventID'] = None  # Initialize eventID column
    current_event_id = None
    last_event_end_time = pd.NaT
    last_deployment_id = None
    last_scientific_name = None

    # Create a list to store the eventID for each row
    event_ids_list = []
    # Convert time_threshold_seconds to a timedelta object for comparison
    time_threshold_delta = timedelta(seconds=time_threshold_seconds)

    for index, row in df.iterrows():
        # Check if we are starting a new group (different deployment or species)
        if row['deploymentID'] != last_deployment_id or row['scientificName'] != last_scientific_name:
            # Start of a new group (different deployment or species), so definitely a new event
            current_event_id = str(uuid.uuid4())[:8]
            last_event_end_time = row['eventEnd']
        elif pd.isna(last_event_end_time) or (row['eventStart'] - last_event_end_time > time_threshold_delta):
            # Same deployment and species, but time difference is too large, so new event
            current_event_id = str(uuid.uuid4())[:8]
            last_event_end_time = row['eventEnd']
        else:
            # Part of the current event, update the event's end time if current observation ends later
            if row['eventEnd'] > last_event_end_time:
                last_event_end_time = row['eventEnd']

        event_ids_list.append(current_event_id)
        last_deployment_id = row['deploymentID']
        last_scientific_name = row['scientificName']

    df['eventID'] = event_ids_list
    print("EventID generation complete.")

    # Save the detailed output (moved before summary generation for clarity)
    try:
        abs_detailed_path = os.path.abspath(output_file_path_detailed)
        print(f"Attempting to save detailed output to absolute path: {abs_detailed_path}")
        df.to_csv(output_file_path_detailed, index=False)
        print(f"Detailed output saved to {output_file_path_detailed} (absolute: {abs_detailed_path})")
    except Exception as e:
        print(f"Error saving detailed output: {e}")

    # --- End of Step 2 ---

    # --- Start of Step 3: Summary Logic Implementation ---
    if output_file_path_summary:
        if df.empty or 'eventID' not in df.columns or df['eventID'].isna().all():
            print("No events to summarize (DataFrame is empty or eventIDs are all NaN).")
            # Create an empty summary DataFrame with correct columns
            summary_df = pd.DataFrame(columns=[
                'eventID', 'deploymentID', 'scientificName',
                'min_event_start', 'max_event_end', 'observation_count'
            ])
        else:
            print("Generating event summary...")
            # Group by eventID and aggregate
            summary_df = df.groupby('eventID').agg(
                deploymentID=('deploymentID', 'first'),  # Should be the same for all rows in an event
                scientificName=('scientificName', 'first'), # Should be the same
                min_event_start=('eventStart', 'min'),
                max_event_end=('eventEnd', 'max'),
                observation_count=('observationID', 'count') # Count unique observationIDs within the event
            ).reset_index()
            print("Event summary generation complete.")

        try:
            abs_summary_path = os.path.abspath(output_file_path_summary)
            print(f"Attempting to save summary output to absolute path: {abs_summary_path}")
            summary_df.to_csv(output_file_path_summary, index=False)
            print(f"Summary output saved to {output_file_path_summary} (absolute: {abs_summary_path})")
        except Exception as e:
            print(f"Error saving summary output: {e}")
    # --- End of Step 3 ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Event IDs for animal observations.")
    parser.add_argument("input_file",
                        help="Path to the input observations CSV file.",
                        default="output/observations.csv",
                        nargs='?') # Makes it optional, using default if not provided
    parser.add_argument("--output_detailed",
                        help="Path to save the detailed output CSV file with eventIDs.",
                        default="output/observations_with_eventIDs.csv")
    parser.add_argument("--output_summary",
                        help="Path to save the summary event CSV file (optional).",
                        default=None, # No summary by default
                        nargs='?') # Makes it optional
    parser.add_argument("--threshold",
                        type=int,
                        help="Time threshold in seconds for grouping events.",
                        default=180) # 3 minutes default

    args = parser.parse_args()

    # Use a default for output_summary if the user provides the flag without a value
    # (which argparse interprets as None, but we might want a default filename)
    # However, the current setup is fine: if --output_summary is given, args.output_summary will be its value.
    # If not given, it's None. If given without value, it's also None (if nargs='?').
    # For simplicity, if user wants summary, they should provide a path.
    # The problem statement implies it's an "optional script", so making it generate a default name
    # if flag is present but no value is given might be overcomplicating.
    # The current default=None means it won't run unless specified.

    # Let's refine the summary output path. If the flag is present, use a default name if no specific name is provided.
    # No, the current argparse setup is:
    # - Not providing --output_summary: args.output_summary is None
    # - Providing --output_summary some_path: args.output_summary is "some_path"
    # - Providing --output_summary (with no path following it, if that's how it's called):
    #   This actually depends on how it's called. If it's `python script.py --output_summary`,
    #   argparse might expect a value. If `nargs='?'` and `const` is set, it can take a value if provided,
    #   or `const` if flag is present without value.
    # For now, the simplest is: if output_summary is not None, then user provided a path.
    # The prompt said "optional script that creates another csv file", implying it's a deliberate action.
    # Let's stick to `default=None` for `output_summary` and if the user wants it, they provide the path.
    # The plan says "controlled by whether output_file_path_summary is provided".

    summary_output_path = args.output_summary
    # If user *wants* a summary but doesn't specify a name, we can default it.
    # However, the current setup requires them to specify a name if they use the flag.
    # Let's make it so if they *just* type `--output_summary` without a path, it defaults.
    # This requires `nargs='?'` and a `const` value.

    # Re-evaluating: The plan step says "controlled by whether output_file_path_summary is provided".
    # The current `default=None` and `nargs='?'` for `output_summary` means:
    #   - if `--output_summary` is not used, `args.output_summary` is `None`.
    #   - if `--output_summary my_summary.csv` is used, `args.output_summary` is `my_summary.csv`.
    #   - if `--output_summary` is used alone at the end of the command, `args.output_summary` is `None`.
    # This last case is tricky. Let's make it simpler: if they want a summary, they provide the path.
    # So `default=None` without `nargs='?'` or with `nargs='?'` but expecting a value.
    # The current `nargs='?'` and `default=None` is fine. `args.output_summary` will be `None` unless a path is given.

    generate_event_ids(args.input_file,
                         args.output_detailed,
                         args.output_summary,
                         args.threshold)

print("Script finished.")
