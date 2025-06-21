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
    print(f"Starting event ID generation. Input file (will be overwritten): {input_file_path}")
    print(f"Time threshold for event grouping: {time_threshold_seconds} seconds.")
    # Removed print statements for output_file_path_detailed and output_file_path_summary
    # as detailed is same as input, and summary is removed.

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
        # If no animal events, save the original DataFrame (all rows, eventID column is None)
        # This effectively means overwriting the input file with itself but with an eventID column.
        try:
            abs_output_path = os.path.abspath(output_file_path_detailed) # This is input_file_path
            print(f"No eligible animal observations. Overwriting input file with an added (empty) eventID column: {abs_output_path}")
            df.to_csv(output_file_path_detailed, index=False)
            print(f"File overwritten at {output_file_path_detailed}")
        except Exception as e:
            print(f"Error saving output file: {e}")
        # Removed summary file logic for this case.
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

    # --- Save Output ---
    # The main DataFrame 'df' now contains all original (parseable) rows,
    # with 'eventID' populated for relevant animal observations.
    # This DataFrame will overwrite the original input file.
    try:
        abs_output_path = os.path.abspath(output_file_path_detailed) # output_file_path_detailed is now same as input_file_path
        print(f"Attempting to overwrite input file with processed data at: {abs_output_path}")
        df.to_csv(output_file_path_detailed, index=False)
        print(f"Successfully overwrote file at {output_file_path_detailed}")
    except Exception as e:
        print(f"Error saving output file: {e}")

    # Summary logic has been removed as per new requirements.

if __name__ == "__main__":
    # --- Command-Line Argument Parsing ---
    parser = argparse.ArgumentParser(
        description="Generate Event IDs for animal observations and overwrite the input file."
    )
    parser.add_argument("--input_file",  # Changed from positional to named argument
                        help="Path to the observations CSV file. This file will be overwritten.",
                        required=True)
    parser.add_argument("--threshold",
                        type=int,
                        help="Time threshold in seconds for grouping events (default: 180).",
                        default=180)

    args = parser.parse_args()

    # Call the main function with the file path for both input and detailed output,
    # and None for the summary output path as it's no longer generated.
    generate_event_ids(args.input_file,
                         args.input_file,  # Output detailed path is same as input
                         None,             # No summary output
                         args.threshold)

print("Script finished.")
