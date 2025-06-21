"""
Downloads documents (samples or full) from specified MongoDB collections.

Usage:
    python mongo_to_exports.py [--download-type <type>]

Arguments:
    --download-type: Optional. Specifies the type of download.
                     Choices: "sample", "full". Default: "sample".
                     - "sample": Downloads one representative sample document
                                 for each identified type within each target collection.
                                 The sample is chosen as the largest document of its type,
                                 based on a scan of up to 'sample_scan_limit' documents.
                     - "full": Downloads all documents from each target collection.

Environment Variables:
    MONGO_URI_PROD: MongoDB connection URI. Defaults to "mongodb://localhost:27017".
    MONGO_DB: MongoDB database name. Defaults to "urbanrivers".

Output:
    - JSON files for documents:
        - If "sample": samples_collectionname.json (dictionary of samples by type)
        - If "full": all_docs_collectionname.json (list of all documents)
"""
import os
import json
import argparse
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MongoDB connection configuration
MONGO_URI = os.getenv("MONGO_URI_PROD", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGO_DB", "urbanrivers")

# --- Configuration ---
# Define which collections to process
TARGET_COLLECTIONS = ["cameratrapmedias", "deploymentlocations", "observations"]
# Max documents to scan when in "sample" mode to find representative docs by type
SAMPLE_MODE_SCAN_LIMIT = 1000

def setup_arg_parser():
    """Sets up the argument parser for command-line options."""
    parser = argparse.ArgumentParser(
        description="Download MongoDB schema and/or documents (samples or full set).",
        formatter_class=argparse.RawTextHelpFormatter # To allow for multiline help text
    )
    parser.add_argument(
        "--download-type",
        type=str,
        choices=["sample", "full"],
        default="sample",
        help="Specify 'sample' to download one sample of each collection type, or 'full' to download all documents. Defaults to 'sample'."
    )
    return parser

def get_sample_docs_by_type(collection, download_type="sample", type_field="type"):
    """
    Fetches documents from a collection based on the download_type.
    - "sample": Fetches the largest sample document for each 'type_field' value,
                scanning up to 'SAMPLE_MODE_SCAN_LIMIT' documents.
    - "full": Fetches all documents from the collection.
    """
    if download_type == "sample":
        type_to_doc = {}
        # Limit the number of documents scanned for "sample" mode
        for doc in collection.find().limit(SAMPLE_MODE_SCAN_LIMIT):
            # Ensure _id is serializable, often it's an ObjectId
            doc['_id'] = str(doc['_id']) if '_id' in doc else None
            doc_type = doc.get(type_field) or doc.get("record_type") or "unknown"
            doc_len = len(json.dumps(doc, default=str))
            if doc_type not in type_to_doc or doc_len > type_to_doc[doc_type][0]:
                type_to_doc[doc_type] = (doc_len, doc)
        return {k: v[1] for k, v in type_to_doc.items()}
    elif download_type == "full":
        all_docs = []
        for doc in collection.find():
            # Ensure _id is serializable
            doc['_id'] = str(doc['_id']) if '_id' in doc else None
            all_docs.append(doc)
        return all_docs # Returns a list of all documents
    else:
        raise ValueError("Invalid download_type. Choose 'sample' or 'full'.")

def export_collection(db, collection_name, download_type):
    print(f"Processing: {collection_name} (mode: {download_type})")
    collection = db[collection_name]

    print(f"Fetching documents for {collection_name} with download type: {download_type}...")
    if download_type == "full":
        # For "full" mode, the output is a list of all documents
        all_docs = get_sample_docs_by_type(collection, download_type=download_type)
        output_data = all_docs
        # Update filename to reflect it might contain all documents, not just samples
        sample_filename = f"all_docs_{collection_name}.json" if download_type == "full" else f"samples_{collection_name}.json"

    elif download_type == "sample":
        # For "sample" mode, the output is a dictionary of sample documents by type
        samples_by_type = get_sample_docs_by_type(collection, download_type=download_type)
        output_data = samples_by_type
        sample_filename = f"samples_{collection_name}.json"
    else:
        # This case should ideally be prevented by argparse choices, but good for safety
        print(f"Error: Invalid download_type '{download_type}' in export_collection.")
        return

    with open(sample_filename, "w") as f:
        json.dump(output_data, f, indent=2, default=str) # Ensure default=str for any other non-serializable types

    print(f"Successfully exported documents for {collection_name}")

# --- Mongo connection ---
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# --- Argument Parsing ---
parser = setup_arg_parser()
args = parser.parse_args()

print(f"Script initiated. Download type: '{args.download_type}'")

# --- Process Collections ---
# Loop through the target collections defined in TARGET_COLLECTIONS
for coll_name in TARGET_COLLECTIONS:
    if coll_name in db.list_collection_names():
        export_collection(db, coll_name, args.download_type)
    else:
        print(f"⚠️ Collection not found: {coll_name}")

