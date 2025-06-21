import pandas as pd
import argparse
import uuid
from datetime import timedelta
import os # Added import
import hashlib # For deterministic IDs

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

    # --- Preprocessing ---
    print("Preprocessing data...")
    # Convert eventStart and eventEnd to datetime objects.
    # Rows that fail conversion will have NaT in these columns.
    df['eventStart'] = pd.to_datetime(df['eventStart'], errors='coerce')
    df['eventEnd'] = pd.to_datetime(df['eventEnd'], errors='coerce')

    # Initialize eventID column for all rows in the main DataFrame.
    # It will remain None for rows not meeting the event criteria.
    df['eventID'] = None

    # Define criteria for observations that can be part of an animal event.
    # These are: valid eventStart, observationType 'animal', and present scientificName & deploymentID.
    # Ensure observationType is string to use .str methods.
    df['observationType'] = df['observationType'].astype(str)

    animal_criteria = (
        df['eventStart'].notna() &
        df['observationType'].str.lower().eq('animal') &
        df['scientificName'].notna() & (df['scientificName'].str.strip() != '') &
        df['deploymentID'].notna() & (df['deploymentID'].astype(str).str.strip() != '')
    )

    # Create 'animal_df' containing only rows eligible for event processing.
    # A copy is used to avoid SettingWithCopyWarning on subsequent modifications.
    animal_df = df[animal_criteria].copy()
    print(f"Total rows in input: {df.shape[0]}")
    print(f"Rows eligible for event processing: {animal_df.shape[0]}")

    if animal_df.empty:
        print("No observations eligible for event ID generation were found.")
        # Save the main DataFrame (all rows, but no eventIDs generated)
        try:
            abs_detailed_path = os.path.abspath(output_file_path_detailed)
            print(f"Attempting to save detailed output (no animal events) to: {abs_detailed_path}")
            df.to_csv(output_file_path_detailed, index=False)
            print(f"Detailed output saved to {output_file_path_detailed}")
        except Exception as e:
            print(f"Error saving detailed output: {e}")

        if output_file_path_summary:
            # If summary output is requested, save an empty summary file.
            summary_df = pd.DataFrame(columns=[
                'eventID', 'deploymentID', 'scientificName',
                'min_event_start', 'max_event_end', 'observation_count'
            ])
            try:
                abs_summary_path = os.path.abspath(output_file_path_summary)
                print(f"Attempting to save empty summary output to: {abs_summary_path}")
                summary_df.to_csv(output_file_path_summary, index=False)
                print(f"Empty summary output saved to {output_file_path_summary}")
            except Exception as e:
                print(f"Error saving summary output: {e}")
        return

    # Sort 'animal_df' to process observations chronologically per deployment/species.
    # This is crucial for correct event grouping.
    print("Sorting eligible animal observations for event processing...")
    animal_df.sort_values(by=['deploymentID', 'scientificName', 'eventStart'], inplace=True)

    # --- Event Grouping and Deterministic EventID Generation (operates on 'animal_df') ---
    print("Generating eventIDs for eligible animal observations...")

    # animal_event_ids_map stores {original_df_index: event_id} for rows in animal_df.
    # This allows updating the main 'df' correctly after processing 'animal_df'.
    animal_event_ids_map = {}
    current_event_id = None
    last_event_end_time = pd.NaT
    last_deployment_id = None
    last_scientific_name = None

    time_threshold_delta = timedelta(seconds=time_threshold_seconds)

    for index, row in animal_df.iterrows(): # 'index' is the original index from the main 'df'.
        new_event_initiated = False
        # Determine if a new event should start:
        # - If deploymentID or scientificName changes.
        # - Or, if the time since the last observation's end exceeds the defined threshold.
        if row['deploymentID'] != last_deployment_id or row['scientificName'] != last_scientific_name:
            new_event_initiated = True
        elif pd.isna(last_event_end_time) or \
             (row['eventStart'] - last_event_end_time > time_threshold_delta):
            new_event_initiated = True

        if new_event_initiated:
            # Generate a deterministic eventID.
            # Based on: deploymentID, scientificName, and eventStart of the first observation in this event.
            # Using .isoformat() for the timestamp ensures a consistent string representation.
            timestamp_str = row['eventStart'].isoformat()
            id_components = f"{row['deploymentID']}_{row['scientificName']}_{timestamp_str}"

            sha256_hash = hashlib.sha256(id_components.encode('utf-8')).hexdigest()
            current_event_id = sha256_hash[:8] # Use the first 8 characters of the hash.

            last_event_end_time = row['eventEnd'] # Initialize/reset the end time for this new event.
        else:
            # This observation is part of the ongoing event.
            # Update the event's overall end time if this observation ends later.
            if row['eventEnd'] > last_event_end_time:
                last_event_end_time = row['eventEnd']

        animal_event_ids_map[index] = current_event_id
        last_deployment_id = row['deploymentID']
        last_scientific_name = row['scientificName']

    # Apply the generated eventIDs back to the main DataFrame 'df'.
    # Rows not included in 'animal_df' (e.g., blanks, humans) will retain their initial None eventID.
    for original_idx, event_id_val in animal_event_ids_map.items():
        df.loc[original_idx, 'eventID'] = event_id_val

    print("EventID assignment to main DataFrame complete.")

    # --- Save Detailed Output ---
    # The main DataFrame 'df' now contains all original (parseable) rows,
    # with 'eventID' populated for relevant animal observations.
    try:
        abs_detailed_path = os.path.abspath(output_file_path_detailed)
        print(f"Attempting to save detailed output to: {abs_detailed_path}")
        df.to_csv(output_file_path_detailed, index=False)
        print(f"Detailed output saved to {output_file_path_detailed}")
    except Exception as e:
        print(f"Error saving detailed output: {e}")

    # --- Summary File Logic (Optional) ---
    if output_file_path_summary:
        # Create the summary only from rows that were successfully assigned an eventID.
        summary_source_df = df[df['eventID'].notna()].copy()

        if summary_source_df.empty:
            print("No events to summarize (no rows were assigned an eventID).")
            summary_df = pd.DataFrame(columns=[
                'eventID', 'deploymentID', 'scientificName',
                'min_event_start', 'max_event_end', 'observation_count'
            ])
        else:
            print("Generating event summary...")
            # Group by the generated eventID and aggregate required information.
            summary_df = summary_source_df.groupby('eventID').agg(
                deploymentID=('deploymentID', 'first'),
                scientificName=('scientificName', 'first'),
                min_event_start=('eventStart', 'min'),
                max_event_end=('eventEnd', 'max'),
                observation_count=('observationID', 'count') # Count of original observations in this event.
            ).reset_index()
            print("Event summary generation complete.")

        try:
            abs_summary_path = os.path.abspath(output_file_path_summary)
            print(f"Attempting to save summary output to: {abs_summary_path}")
            summary_df.to_csv(output_file_path_summary, index=False)
            print(f"Summary output saved to {output_file_path_summary}")
        except Exception as e:
            print(f"Error saving summary output: {e}")

if __name__ == "__main__":
    # --- Command-Line Argument Parsing ---
    # Comments for argparse are minimal as the help strings are descriptive.
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
