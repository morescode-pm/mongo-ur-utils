# MongoDB Utilities for Urban Rivers Project

This folder contains utilities for processing AI detection results and managing MongoDB records for the Urban Rivers project. The workflow involves parsing detection data from a JSON file and then uploading these results to a MongoDB database.

## Files
- `detection_parser.py`: Script for parsing detection results from a source JSON file into a MongoDB-compatible format. Outputs `mongodb_formatted_detections.json`.
- `mongo_operations.py`: Script for handling MongoDB operations (updating or replacing AI results in existing documents).
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
The `mongo_operations.py` script uses the following environment variables (defaults are shown if not set in `.env`):
- `MONGO_URI`: MongoDB connection string (e.g., 'mongodb://localhost:27017')
- `MONGO_DB`: Database name (e.g., 'urbanrivers')
- `MONGO_COLLECTION`: Collection name (e.g., 'medias')

## Usage

The workflow is split into two main steps:

### 1. Parse Detection Results (`detection_parser.py`)
This script takes a JSON file containing detection results, processes it, and creates a new file named `mongodb_formatted_detections.json` which is structured for upload to MongoDB.

**Command:**
```bash
python detection_parser.py <path_to_source_json_file>
```
**Example:**
```bash
python detection_parser.py ./cv_results/2025-05-24_speciesnet_v4.0.1a_nogeo.json
```
You will be prompted for:
- The **model run date** (in YYYY-MM-DD format).
- Confirmation to process all records after a sample of 5 is shown.

The output file (`mongodb_formatted_detections.json`) will contain entries where each key is a `mediaID` and the value contains the `aiResults`.

### 2. Update MongoDB Records (`mongo_operations.py`)
After `mongodb_formatted_detections.json` has been generated, this script is used to update your MongoDB database. It assumes that documents corresponding to each `mediaID` in the JSON file already exist in the database.

**Command:**
```bash
python mongo_operations.py
```
You will be prompted to choose an operation:
- **`update`**: Adds new AI results to the `aiResults` array in existing documents. This is useful for adding results from a new model run without removing previous ones.
- **`replace`**: Replaces the entire `aiResults` array in existing documents with the new results from the JSON file. This is useful if you want to overwrite previous results with a new, complete set.

## MongoDB Document Structure for AI Results
The `mongo_operations.py` script will add or update an `aiResults` field in your MongoDB documents. This field is an array of objects, where each object represents a set of AI analysis results.

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

## Detailed Operations in `mongo_operations.py`
- **`update`**: Appends new entries to the `aiResults` array of existing documents. If the document doesn't exist (which is not expected per current workflow assumptions but handled by `upsert=True`), it would create a new document with the `mediaID` and `aiResults`.
- **`replace`**: Overwrites the existing `aiResults` array with the new entries. If the document doesn't exist (again, not expected but handled by `upsert=True`), it would create a new document.
