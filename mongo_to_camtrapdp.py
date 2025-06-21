import json
import csv
from datetime import datetime, timezone
import os

# --- Camtrap DP Data Structures ---

# These will be lists of dictionaries, where each dictionary represents a row.
# The keys of the dictionary will correspond to the Camtrap DP field names.

# Example structure (fields will be populated based on schema):
# deployments_headers = ["deploymentID", "locationID", "locationName", ...]
# media_headers = ["mediaID", "deploymentID", "timestamp", ...]
# observations_headers = ["observationID", "deploymentID", "mediaID", ...]


# --- Helper Functions ---

def format_datetime_iso(dt_str):
    """
    Converts a datetime string (potentially with or without milliseconds or timezone)
    to an ISO 8601 string with a 'Z' timezone designator (UTC).
    Handles various common datetime formats found in the source data.
    If conversion fails, returns None.
    """
    if not dt_str:
        return None
    try:
        # Common formats to try
        formats_to_try = [
            "%Y-%m-%d %H:%M:%S.%f",  # e.g., "2024-10-24 23:03:13.917000"
            "%Y-%m-%d %H:%M:%S",      # e.g., "2024-01-12 06:03:50"
            "%Y-%m-%dT%H:%M:%S%z",   # ISO with offset
            "%Y-%m-%dT%H:%M:%SZ",    # ISO with Z
        ]

        dt_obj = None
        for fmt in formats_to_try:
            try:
                dt_obj = datetime.strptime(dt_str, fmt)
                break
            except ValueError:
                continue

        if dt_obj is None:
            # Fallback to dateutil.parser was removed to avoid new dependency.
            # If more complex date parsing is needed, consider adding 'python-dateutil' to requirements
            # and re-enabling that logic.
            print(f"Warning: Could not parse datetime string with predefined formats: {dt_str}")
            return None

        # If the datetime object is naive, assume it's UTC.
        if dt_obj.tzinfo is None or dt_obj.tzinfo.utcoffset(dt_obj) is None:
            dt_obj = dt_obj.replace(tzinfo=timezone.utc)
        else:
            # Convert to UTC
            dt_obj = dt_obj.astimezone(timezone.utc)

        return dt_obj.strftime("%Y-%m-%dT%H:%M:%SZ")

    except Exception as e:
        print(f"Error converting datetime string '{dt_str}': {e}")
        return None

# --- Conversion Functions ---

def convert_deploymentlocations_to_camtrapdp(mongo_deployment_locations, dp_headers, default_deployment_id_prefix="dep"):
    """
    Converts MongoDB deploymentlocations data to Camtrap DP deployments format.
    Note: The source data 'deploymentlocations' is more about locations than specific deployments
    (which have start/end times). We will create one 'deployment' entry per location.
    `deploymentStart` and `deploymentEnd` will be left blank as they are not in the source.
    """
    deployments_data = []
    for i, loc_data in enumerate(mongo_deployment_locations):
        if not loc_data or not isinstance(loc_data, dict): # Skip if loc_data is None or not a dict
            print(f"Skipping invalid location data: {loc_data}")
            continue

        # Create a base dictionary with all possible headers, initialized to None or empty string
        # This ensures all columns are present in the CSV even if data is missing.
        deployment_entry = {header: "" for header in dp_headers}

        deployment_entry["deploymentID"] = loc_data.get("_id", f"{default_deployment_id_prefix}{i+1}")
        deployment_entry["locationID"] = loc_data.get("_id") # Using the same ID for locationID
        deployment_entry["locationName"] = loc_data.get("locationName")

        location_coords = loc_data.get("location", {}).get("coordinates")
        if location_coords and len(location_coords) == 2:
            deployment_entry["longitude"] = location_coords[0] # longitude is typically first
            deployment_entry["latitude"] = location_coords[1]  # latitude is typically second

        # deploymentStart and deploymentEnd are required by Camtrap DP schema but not in source.
        # Leaving them blank means they will be output as empty strings.
        # Camtrap DP specifies ISO 8601 format for these.
        # For this exercise, we leave them blank. A real implementation might need a strategy.
        deployment_entry["deploymentStart"] = "" # Placeholder
        deployment_entry["deploymentEnd"] = ""   # Placeholder

        deployment_entry["setupBy"] = loc_data.get("creator")

        tags = loc_data.get("tags")
        if isinstance(tags, list):
            deployment_entry["deploymentTags"] = "|".join(tags)
        elif isinstance(tags, str): # if it's already a string
            deployment_entry["deploymentTags"] = tags

        deployment_entry["deploymentComments"] = loc_data.get("notes")
        # projectArea is not a standard CamtrapDP field for deployments, so it's ignored unless custom tags are used.
        # mount is not a standard CamtrapDP field.

        deployments_data.append(deployment_entry)
    return deployments_data

def convert_cameratrapmedias_to_camtrapdp(mongo_media_list, dp_headers, default_deployment_id="unknown_deployment_1"):
    """
    Converts MongoDB cameratrapmedias data to Camtrap DP media format.
    """
    media_output_data = []
    for media_item in mongo_media_list:
        if not media_item or not isinstance(media_item, dict):
            print(f"Skipping invalid media item: {media_item}")
            continue

        media_entry = {header: "" for header in dp_headers}

        media_entry["mediaID"] = media_item.get("mediaID")

        # deploymentID is a major challenge as it's not in samples_cameratrapmedias.json
        # Using a default or passed-in ID. This is a significant simplification.
        # A more robust solution would require linking media to deployments through other means
        # or having this info in the source data.
        media_entry["deploymentID"] = media_item.get("deploymentID", default_deployment_id) # Placeholder

        media_entry["timestamp"] = format_datetime_iso(media_item.get("timestamp"))

        # filePath: CamtrapDP expects a relative path or URL. publicURL seems suitable.
        media_entry["filePath"] = media_item.get("publicURL")
        media_entry["filePublic"] = media_item.get("filePublic", False) # Default to False if not present
        media_entry["fileName"] = media_item.get("fileName")
        media_entry["fileMediatype"] = media_item.get("fileMediatype")

        exif = media_item.get("exifData")
        if exif and isinstance(exif, dict):
            try:
                media_entry["exifData"] = json.dumps(exif) # CamtrapDP expects a JSON string
            except TypeError:
                media_entry["exifData"] = "{}" # or some error placeholder
        else:
            media_entry["exifData"] = ""

        media_entry["favorite"] = media_item.get("favorite", False)

        comments = media_item.get("mediaComments")
        if isinstance(comments, list):
            # Assuming comments are strings; join if multiple, though sample is empty
            media_entry["mediaComments"] = "|".join(str(c) for c in comments)
        elif isinstance(comments, str):
            media_entry["mediaComments"] = comments

        # Other fields from source like imageHash, relativePath, fileLocations, __v, accepted, etc.
        # are not directly mapped to standard CamtrapDP media fields.

        media_output_data.append(media_entry)
    return media_output_data

def convert_observations_to_camtrapdp(mongo_observations_list, dp_headers, media_to_deployment_map=None, default_deployment_id="unknown_deployment_1"):
    """
    Converts MongoDB observations data to Camtrap DP observations format.
    """
    observations_output_data = []
    if media_to_deployment_map is None:
        media_to_deployment_map = {}

    for i, obs_item in enumerate(mongo_observations_list):
        if not obs_item or not isinstance(obs_item, dict):
            print(f"Skipping invalid observation item: {obs_item}")
            continue

        observation_entry = {header: "" for header in dp_headers}

        observation_entry["observationID"] = obs_item.get("_id", f"obs{i+1}")

        mongo_media_id = obs_item.get("mediaId") # Note: 'mediaId' (lowercase d) in source
        observation_entry["mediaID"] = mongo_media_id

        # Attempt to get deploymentID from the media_to_deployment_map
        # If not found, use the default_deployment_id
        observation_entry["deploymentID"] = media_to_deployment_map.get(mongo_media_id, default_deployment_id)

        observation_entry["eventStart"] = format_datetime_iso(obs_item.get("eventStart"))
        observation_entry["eventEnd"] = format_datetime_iso(obs_item.get("eventEnd"))
        observation_entry["observationLevel"] = obs_item.get("observationLevel")
        observation_entry["observationType"] = obs_item.get("observationType")
        observation_entry["scientificName"] = obs_item.get("scientificName")
        observation_entry["count"] = obs_item.get("count")

        # 'creator' in mongo observations could map to 'classifiedBy' in CamtrapDP
        observation_entry["classifiedBy"] = obs_item.get("creator")

        # classificationTimestamp could be mapped from 'updatedAt' or 'createdAt' if appropriate
        # For now, let's use 'updatedAt' as a proxy for classification timestamp
        observation_entry["classificationTimestamp"] = format_datetime_iso(obs_item.get("updatedAt"))

        # Fields like taxonId, mediaInfo, __v are not directly in standard CamtrapDP observations
        # but could be added to observationComments or observationTags if needed.

        observations_output_data.append(observation_entry)
    return observations_output_data


# --- Main Processing ---

def main():
    # Define input file paths
    deployments_in_file = "mongo_exports/samples_deploymentlocations.json"
    media_in_file = "mongo_exports/samples_cameratrapmedias.json"
    observations_in_file = "mongo_exports/samples_observations.json"

    # Define output directory and file paths
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    deployments_out_csv = os.path.join(output_dir, "deployments.csv")
    media_out_csv = os.path.join(output_dir, "media.csv")
    observations_out_csv = os.path.join(output_dir, "observations.csv")

    # Load data from MongoDB JSON exports
    try:
        with open(deployments_in_file, 'r') as f:
            raw_deployments_data = json.load(f)
        with open(media_in_file, 'r') as f:
            raw_media_data = json.load(f)
        with open(observations_in_file, 'r') as f:
            raw_observations_data = json.load(f)
    except FileNotFoundError as e:
        print(f"Error: Input file not found: {e.filename}")
        return
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from file: {e}")
        return

    # The sample JSON files have a top-level key (e.g., "unknown") which wraps the actual list/object.
    # Adjusting to extract the actual data. This might need to be more robust if the key varies.

    # Assuming the actual data is the value of the first key if it's a dictionary
    # Or if it's a list directly, use it as is.
    # For the provided samples, it seems to be a dictionary with one key holding the object.

    actual_deployments_data = list(raw_deployments_data.values())[0] if isinstance(raw_deployments_data, dict) and raw_deployments_data else raw_deployments_data
    actual_media_data = list(raw_media_data.values())[0] if isinstance(raw_media_data, dict) and raw_media_data else raw_media_data
    actual_observations_data = list(raw_observations_data.values())[0] if isinstance(raw_observations_data, dict) and raw_observations_data else raw_observations_data

    # If the data is still not a list (e.g. a single object was exported), wrap it in a list.
    # This is common if the sample JSON represents a single document.
    if not isinstance(actual_deployments_data, list):
        actual_deployments_data = [actual_deployments_data]
    if not isinstance(actual_media_data, list):
        actual_media_data = [actual_media_data]
    if not isinstance(actual_observations_data, list):
        actual_observations_data = [actual_observations_data]


    # --- Get Camtrap DP field names from schema definitions ---
    # This helps ensure we generate CSVs with the correct headers in the correct order.

    def get_ordered_field_names(schema_file_path):
        try:
            with open(schema_file_path, 'r') as f:
                schema = json.load(f)
            return [field['name'] for field in schema.get('fields', [])]
        except FileNotFoundError:
            print(f"Warning: Schema file not found: {schema_file_path}. CSV headers may be incomplete or unordered.")
            return []
        except json.JSONDecodeError:
            print(f"Warning: Could not decode JSON from schema file: {schema_file_path}.")
            return []

    deployments_headers = get_ordered_field_names("camtrap_standards/definitions/deployments-table-schema.json")
    media_headers = get_ordered_field_names("camtrap_standards/definitions/media-table-schema.json")
    observations_headers = get_ordered_field_names("camtrap_standards/definitions/observations-table-schema.json")

    # --- Perform conversions ---
    # For now, these will return empty lists as the core logic is not yet implemented.

    # Placeholder for deployment ID generation/lookup logic
    # In a real scenario, we need a robust way to link media/observations to deployments.
    # The `samples_deploymentlocations.json` represents locations, not deployments directly.
    # A deployment has a start/end time at a location.
    # For now, let's assume the first location's ID can be a placeholder `deploymentID`
    # or we generate one if needed.

    # Pass the headers to the conversion functions so they know what fields to create.
    # And use the actual_deployments_data (which is a list of dicts)
    deployments_camtrapdp = convert_deploymentlocations_to_camtrapdp(
        actual_deployments_data,
        deployments_headers
    )

    # Determine a fallback_deployment_id.
    # If deployments were successfully converted, use the ID of the first one.
    # Otherwise, use a generic placeholder.
    # This is crucial because the media sample data does not link to deployments.
    fallback_deployment_id = "unknown_deployment_1" # Default if no deployments processed
    if deployments_camtrapdp and isinstance(deployments_camtrapdp, list) and len(deployments_camtrapdp) > 0:
        first_deployment = deployments_camtrapdp[0]
        if isinstance(first_deployment, dict) and "deploymentID" in first_deployment:
            fallback_deployment_id = first_deployment.get("deploymentID", "unknown_deployment_1")


    media_camtrapdp = convert_cameratrapmedias_to_camtrapdp(
        actual_media_data,
        media_headers,
        default_deployment_id=fallback_deployment_id
    )

    # Populate media_to_deployment_id_map based on the converted media data.
    # This map is essential for linking observations to their respective deployments via media.
    media_to_deployment_id_map = {}
    if media_camtrapdp and isinstance(media_camtrapdp, list):
        for m_row in media_camtrapdp:
            if isinstance(m_row, dict) and m_row.get("mediaID") and m_row.get("deploymentID"):
                media_to_deployment_id_map[m_row.get("mediaID")] = m_row.get("deploymentID")

    observations_camtrapdp = convert_observations_to_camtrapdp(
        actual_observations_data,
        observations_headers,
        media_to_deployment_map=media_to_deployment_id_map,
        default_deployment_id=fallback_deployment_id # Pass fallback here too for observations whose media might not be in media_camtrapdp
    )


    # --- Write to CSV files ---
    def write_to_csv(data, filepath, headers):
        if not headers and data: # If headers are empty but data exists, use keys from first item
            headers = list(data[0].keys())

        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers, extrasaction='ignore', quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            if data: # Ensure data is not empty before writing
                writer.writerows(data)
        print(f"Successfully wrote Camtrap DP data to {filepath}")

    if deployments_headers: # Only write if headers were successfully loaded
        write_to_csv(deployments_camtrapdp, deployments_out_csv, deployments_headers)
    else:
        print(f"Skipping write for {deployments_out_csv} due to missing headers.")

    if media_headers:
        write_to_csv(media_camtrapdp, media_out_csv, media_headers)
    else:
        print(f"Skipping write for {media_out_csv} due to missing headers.")

    if observations_headers:
        write_to_csv(observations_camtrapdp, observations_out_csv, observations_headers)
    else:
        print(f"Skipping write for {observations_out_csv} due to missing headers.")


if __name__ == "__main__":
    main()
