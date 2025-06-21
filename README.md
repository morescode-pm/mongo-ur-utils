# MongoDB Utilities for Urban Rivers Project

This folder contains utilities for processing AI detection results and managing MongoDB records for the Urban Rivers project. The workflow involves parsing detection data from a JSON file and then uploading these results to a MongoDB database.

## Files
- `ai_detection_parser.py`: Script for parsing detection results from a source JSON file into a MongoDB-compatible format. Outputs `mongodb_formatted_detections.json`.
- `ai_mongo_operations.py`: Script for handling MongoDB operations (updating or replacing AI results in existing documents).
- `mongo_to_exports.py`: Script for downloading collections from mongodb to json for parsing by mongo_exports_to_camtrapdp.py.
- `mongo_exports_to_camtrapdp.py`: Script for converting downloaded document jsons to camtrap dp csv files.
- `utils_generate_event_ids.py`: Script for adding unique event ids to observations.csv file.
- `add_date.py`: Temp script for hardcoding rundates into a speciesnet predictions json.
- `.env.example`: Example configuration file for MongoDB credentials. Create a `.env` file from this example.
- `requirements.txt`: Lists necessary Python packages.

## Requirements
```bash
pip install -r requirements.txt
```
Dependencies include:
- `pymongo`: For MongoDB operations.
- `python-dotenv`: For managing environment variables.
- Standard Python libraries like `json`, `pathlib`, `typing`, `sys`, `datetime`.

## Environment Variables
The `ai_mongo_operations.py` and `mongo_to_exports` scripts use the following environment variables (defaults are shown if not set in `.env`):
- `MONGO_URI`: MongoDB connection string (e.g., 'mongodb://localhost:27017')
- `MONGO_DB`: Database name (e.g., 'urbanrivers')  

The `ai_mongo_operations.py` script also uses the environment variable:
- `MONGO_COLLECTION`: Collection name (e.g., 'medias')

## aiResults Usage

The aiResults workflow is split into two main steps:

### 1. Parse Detection Results (`ai_detection_parser.py`)
This script takes a JSON file containing detection results, processes it, and creates a new file named `mongodb_formatted_detections.json` which is structured for upload to MongoDB.

**Command:**
```bash
python detection_parser.py <path_to_source_json_file>
```
**Example:**
```bash
python detection_parser.py ./2025-05-24_speciesnet_v4.0.1a_nogeo.json
```
You will be prompted for:
- Confirmation to process all records after a sample of 5 is shown.

The output file (`mongodb_formatted_detections.json`) will contain entries where each key is a `mediaID` and the value contains the `aiResults`.

### 2. Update MongoDB Records (`ai_mongo_operations.py`)
After `mongodb_formatted_detections.json` has been generated, this script is used to update your MongoDB database. It assumes that documents corresponding to each `mediaID` in the JSON file already exist in the database.

**Command:**
```bash
python ai_mongo_operations.py
```
You will be prompted to choose an operation:
- **`update`**: Adds new AI results to the `aiResults` array in existing documents. This is useful for adding results from a new model run without removing previous ones.
- **`replace`**: Replaces the entire `aiResults` array in existing documents with the new results from the JSON file. This is useful if you want to overwrite previous results with a new, complete set.

## MongoDB Document Structure for AI Results
The `ai_mongo_operations.py` script will add or update an `aiResults` field in your MongoDB documents. This field is an array of objects, where each object represents a set of AI analysis results.

**Example snippet of a document in MongoDB after an update:**
```json
{
  "_id": "...",
  "mediaID": "c89e327383d91bdaadda59e65c57eec8",
  // ... other fields like timestamp, filePath, etc. ...
  "aiResults": [
    {
      "modelName": "speciesnet/PyTorch/v4.0.1a",
      "runDate": "2025-06-08",
      "confBlank": 0.0,
      "confHuman": 0.0,
      "confAnimal": 0.95
    }
    // ... other AI results may be present if using 'update' ...
  ]
}
```

## Detailed Operations in `ai_mongo_operations.py`
- **`update`**: Appends new entries to the `aiResults` array of existing documents. If the document doesn't exist (which is not expected per current workflow assumptions but handled by `upsert=True`), it would create a new document with the `mediaID` and `aiResults`.
- **`replace`**: Overwrites the existing `aiResults` array with the new entries. If the document doesn't exist (again, not expected but handled by `upsert=True`), it would create a new document.  

---
---  

## Mongo Exports Usage

The mongo exports to camtrapdp workflow is split into three steps:

### 1. Download documents from your MongoDB (`mongo_to_exports.py`)
This script will download a sample or the full documents collection from cameratrapmedias, deploymentlocations, and observations collections assuming they exist.

**Command:**
```bash
python mongo_to_exports.py --download-type ["sample", "full"] (default="sample")
```
**Example:**
```bash
python mongo_to_exports.py --download-type sample
```
- **`sample`**: Downloads a single 'longest' sample of a document from the selected collections after scanning 1000 files. This is useful for comparing the existing schema to the desired camtrapdp schema without downloading all records. Files saved with `samples_<collection-name>.json`
- **`full`**: Downloads all documents from the selected collections. This is ultimately what will be parsed into the output camtrap dp structure. Files saved with `all_docs_<collection-name>.json`

### 2. Process Exports to CamtrapDP structure (`mongo_exports_to_camtrapdp`)
This script will take the downloaded sample or full documents and convert them into deployments.csv, media.csv, and observations.csv files. The workflow is split into two steps to allow previewing json files before processing.

**Command:**
```bash
python mongo_exports_to_camtrapdp.py --mode ["sample", "full"] (default="sample")
```
**Example:**
```bash
python mongo_to_exports.py --mode sample
```
- **`sample`**: Processes the files saved as `samples_<collection-name>.json` into the output camtrap dp csv files. There will be only 1 row of data from the sample in each table for previewing.
- **`full`**: Processes the files saved as `all_docs_<collection-name>.json` into the output camtrap dp csv files. This is the target final data structure and will contain all records found.

### 3. [Optional] Add an eventID field to observations (`utils_generate_event_ids.py`)
This script will group the deploymentID, observationType, and scientificName fields as appropriate to group together observations with close proximity time stamps. It takes two arguments --input-file and --threshold (default 180seconds). 

**Command:**
```bash
python utils_generate_event_ids.py --input-file <file-path-to-observations.csv> --threshold <seconds-between-observations-considered-one-event>
```
**Example:**
```bash
python utils_generate_event_ids.py --input-file observations.csv --threshold 180
```
Specifying the input file will allow you to copy & create a specific observations file for eventIDs if desired. For example, copy the observations.csv and rename it to observations_with_eventIDs.csv and run utils_generate_event_ids.py on that copy.  
*Note: Specifying observations.csv will overwrite the previously generated file.*
