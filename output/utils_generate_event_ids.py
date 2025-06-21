import pandas as pd
import argparse
import uuid
from datetime import timedelta
import os # Added import
import hashlib # For deterministic IDs

def generate_event_ids(input_file_path, time_threshold_seconds):
    """
    Generates eventIDs for observations based on species and time proximity.

    Args:
        input_file_path (str): Path to the input CSV file (observations).
        time_threshold_seconds (int): Time threshold in seconds to group observations.
    """
    print(f"Starting event ID generation for input file: {input_file_path}")

    # Load the observations CSV
    try:
        df = pd.read_csv(input_file_path)
    except FileNotFoundError:
        print(f"Error: Input file not found at {input_file_path}")
        return
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return

    # --- Preprocessing ---
    # Convert eventStart and eventEnd to datetime objects.
    # Rows that fail conversion will have NaT in these columns.
    df['eventStart'] = pd.to_datetime(df['eventStart'], errors='coerce')
    df['eventEnd'] = pd.to_datetime(df['eventEnd'], errors='coerce')

    # Initialize eventID column for all rows in the main DataFrame.
    # It will remain None for rows not meeting the event criteria.
    df['eventID'] = None

    # Define criteria for observations that can be part of an event.
    # These are: valid eventStart, specific observationType, and present deploymentID.
    # For 'animal' type, scientificName is also required.
    # Ensure observationType is string to use .str methods.
    df['observationType'] = df['observationType'].astype(str).str.lower()

    # Common criteria for all event types
    common_event_criteria = (
        df['eventStart'].notna() &
        df['deploymentID'].notna() & (df['deploymentID'].astype(str).str.strip() != '')
    )

    # Criteria specific to 'animal'
    animal_specific_criteria = (
        df['observationType'].eq('animal') &
        df['scientificName'].notna() & (df['scientificName'].str.strip() != '')
    )

    # Criteria for other event types ('human', 'blank', 'vehicle')
    # These types do not require scientificName for event grouping.
    other_event_types_criteria = (
        df['observationType'].isin(['human', 'blank', 'vehicle'])
    )

    # Combine criteria:
    # An observation is eligible if it meets common criteria AND
    # (it's an animal with specific criteria OR it's one of the other specified types)
    event_criteria = common_event_criteria & (animal_specific_criteria | other_event_types_criteria)


    # Create 'eligible_df' containing only rows eligible for event processing.
    # A copy is used to avoid SettingWithCopyWarning on subsequent modifications.
    eligible_df = df[event_criteria].copy()

    if eligible_df.empty:
        # If no eligible events, save the original DataFrame (all rows, eventID column is None)
        # This effectively means overwriting the input file with itself but with an eventID column.
        try:
            df.to_csv(input_file_path, index=False)
        except Exception as e:
            print(f"Error saving output file when no eligible observations: {e}")
        return

    # --- Pre-sorting data fill for grouping ---
    # For 'human', 'blank', 'vehicle' types, if scientificName is NaN or empty,
    # fill it with the observationType to allow consistent grouping.
    # This new column 'grouping_key_col' will be used for sorting and ID generation.
    eligible_df['grouping_key_col'] = eligible_df['scientificName']
    non_animal_types = ['human', 'blank', 'vehicle']
    condition = eligible_df['observationType'].isin(non_animal_types) & \
                (eligible_df['scientificName'].isna() | eligible_df['scientificName'].str.strip().eq(''))

    # Use .loc with boolean indexing for cleaner assignment
    eligible_df.loc[condition, 'grouping_key_col'] = eligible_df.loc[condition, 'observationType']


    # Sort 'eligible_df' to process observations chronologically per deployment/grouping_key_col.
    eligible_df.sort_values(by=['deploymentID', 'grouping_key_col', 'eventStart'], inplace=True)

    # --- Event Grouping and Deterministic EventID Generation (operates on 'eligible_df') ---
    event_ids_map = {}
    current_event_id = None
    last_event_end_time = pd.NaT
    last_deployment_id = None
    last_grouping_key_value = None # Stores the actual value of the grouping key (scientificName or observationType)

    time_threshold_delta = timedelta(seconds=time_threshold_seconds)

    for index, row in eligible_df.iterrows(): # 'index' is the original index from the main 'df'.
        new_event_initiated = False
        current_grouping_key_value = row['grouping_key_col']

        # Determine if a new event should start:
        # - If deploymentID or the value of grouping_key_col changes.
        # - Or, if the time since the last observation's end exceeds the defined threshold.
        if row['deploymentID'] != last_deployment_id or \
           current_grouping_key_value != last_grouping_key_value:
            new_event_initiated = True
        elif pd.isna(last_event_end_time) or \
             (row['eventStart'] - last_event_end_time > time_threshold_delta):
            new_event_initiated = True

        if new_event_initiated:
            # Generate a deterministic eventID.
            # Based on: deploymentID, grouping_key_col, and eventStart of the first observation in this event.
            timestamp_str = row['eventStart'].isoformat()
            # Use grouping_key_col for ID generation consistency
            id_components = f"{row['deploymentID']}_{current_grouping_key_value}_{timestamp_str}"

            sha256_hash = hashlib.sha256(id_components.encode('utf-8')).hexdigest()
            current_event_id = sha256_hash[:8] # Use the first 8 characters of the hash.

            last_event_end_time = row['eventEnd'] # Initialize/reset the end time for this new event.
        else:
            # This observation is part of the ongoing event.
            # Update the event's overall end time if this observation ends later.
            if pd.notna(row['eventEnd']) and (pd.isna(last_event_end_time) or row['eventEnd'] > last_event_end_time) : # Ensure row['eventEnd'] is not NaT
                last_event_end_time = row['eventEnd']

        event_ids_map[index] = current_event_id
        last_deployment_id = row['deploymentID']
        last_grouping_key_value = current_grouping_key_value # Update with the new grouping key value

    # Apply the generated eventIDs back to the main DataFrame 'df'.
    # Rows not included in 'eligible_df' will retain their initial None eventID.
    for original_idx, event_id_val in event_ids_map.items():
        df.loc[original_idx, 'eventID'] = event_id_val

    # --- Save Output ---
    try:
        df.to_csv(input_file_path, index=False)
    except Exception as e:
        print(f"Error saving output file: {e}")

    # Summary logic has been removed.

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate Event IDs for observations and overwrite the input file."
    )
    parser.add_argument("--input_file",
                        help="Path to the observations CSV file. This file will be overwritten.",
                        required=True)
    parser.add_argument("--threshold",
                        type=int,
                        help="Time threshold in seconds for grouping events (default: 180).",
                        default=180)

    args = parser.parse_args()

    generate_event_ids(args.input_file,
                         args.threshold)
    print(f"Event ID generation complete for {args.input_file}.")
