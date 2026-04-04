import os
import argparse
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv()

# MongoDB connection configuration
MONGO_URI = os.getenv("MONGO_URI_PROD", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGO_DB", "urbanrivers")
COLLECTION_NAME = "cameratrapmedias"

def export_animal_media(output_file):
    """
    Exports media information from MongoDB where an animal is detected in frame.
    Filter: speciesConsensus.scientificName is not null.
    Fields: timestamp, speciesConsensus.scientificName, consensusStatus, publicURL, videoUrl, aiResults.confHuman.
    """
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        # Filter for documents where at least one speciesConsensus entry has a non-null scientificName
        query = {
            "speciesConsensus.scientificName": {"$ne": None}
        }

        # Project only the requested fields
        projection = {
            "_id": 0,
            "timestamp": 1,
            "speciesConsensus.scientificName": 1,
            "consensusStatus": 1,
            "publicURL": 1,
            "videoUrl": 1,
            "aiResults.confHuman": 1
        }

        print(f"Connecting to database: {DB_NAME}")
        print(f"Querying {COLLECTION_NAME} for records where animal is in frame...")

        results = []
        # collection.find returns a cursor
        cursor = collection.find(query, projection)

        for doc in cursor:
            results.append(doc)

        pd.DataFrame(results).to_csv("animal_media_export.csv", index=False)
        print(f"Successfully exported {len(results)} records to {output_file}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export media information where an animal is in frame.")
    parser.add_argument(
        "--output",
        type=str,
        default="animal_media_export.json",
        help="Output JSON file path (default: animal_media_export.json)"
    )
    args = parser.parse_args()

    export_animal_media(args.output)
