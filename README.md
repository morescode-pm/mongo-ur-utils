# MongoDB Utilities for Urban Rivers Project

This folder contains utilities for processing SpeciesNet detection results and managing MongoDB records. The workflow is split into two independent scripts for better maintainability.

## Files
- `detection_parser.py`: Script for parsing detection results into MongoDB-compatible format
- `mongo_operations.py`: Script for handling MongoDB operations
- `predictions_dict_master_fullnogeo.json`: Source JSON file with detection results
- `.env`: Configuration file for MongoDB credentials (create from `.env.example`)

## Requirements
```bash
pip install -r requirements.txt
```

Dependencies include:
- `black`: Code formatting
- `pymongo`: MongoDB operations
- `python-dotenv`: Environment variable management
- Other utility packages (pathlib, typing)

## Environment Variables
The script uses the following environment variables (with defaults):
- `MONGO_URI`: MongoDB connection string (ex: 'mongodb://localhost:27017')
- `MONGO_DB`: Database name (ex: 'test')
- `MONGO_COLLECTION`: Collection name (ex: 'cameratrapmedias')

## Usage

The workflow is split into two steps:

### 1. Parse Detection Results (`detection_parser.py`)
```bash
# Test with 5 sample records first
python detection_parser.py

# When prompted, choose 'y' to process all records
# This will create mongodb_formatted_detections.json
```

### 2. Update MongoDB (`mongo_operations.py`)
```bash
# After parsing is complete, run the MongoDB operations script
python mongo_operations.py

# Choose one of three operations when prompted:
# - create: Insert new documents only
# - update: Add new AI model results to existing documents
# - replace: Replace existing AI model results
```

## MongoDB Document Structure
```json
{
  "mediaID": "c89e327383d91bdaadda59e65c57eec8",
  "aiModel": [
    {
      "modelName": "speciesnet/PyTorch/v4.0.1a",
      "runDate": "2025-06-08",
      "confBlank": 0.0,
      "confHuman": 0.0,
      "confAnimal": 0.95
    }
  ]
}
```

## Operations
- **create**: Only creates new documents, skips if they exist
- **update**: Adds new AI model results to existing documents (or creates new ones)
- **replace**: Replaces existing AI model results with new ones (or creates new documents)
