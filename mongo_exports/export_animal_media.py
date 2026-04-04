import os
import csv
import argparse
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MongoDB connection configuration
MONGO_URI = os.getenv("MONGO_URI_PROD", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGO_DB", "urbanrivers")
COLLECTION_NAME = "cameratrapmedias"

def export_animal_media_csv(output_file):
    """
    Exports media information from MongoDB where an animal is detected in frame to a CSV file.
    Filter: speciesConsensus.scientificName is not null.
    Columns: timestamp, speciesIdentification (scientificName || observationType), consensusStatus, publicURL, videoUrl, confHuman, observationCount.
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
            "speciesConsensus": 1,
            "consensusStatus": 1,
            "publicURL": 1,
            "videoUrl": 1,
            "aiResults": 1
        }

        print(f"Connecting to {MONGO_URI}, database: {DB_NAME}")
        print(f"Querying {COLLECTION_NAME} for records where animal is in frame...")

        # collection.find returns a cursor
        cursor = collection.find(query, projection)

        headers = [
            "timestamp",
            "speciesIdentification",
            "consensusStatus",
            "publicURL",
            "videoUrl",
            "confHuman",
            "observationCount"
        ]

        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()

            count = 0
            for doc in cursor:
                # Extract most recent aiResults.confHuman (last in the array if it exists)
                ai_results = doc.get("aiResults", [])
                conf_human = ""
                if isinstance(ai_results, list) and ai_results:
                    # Taking the last element as the most recent result
                    conf_human = ai_results[-1].get("confHuman", "")

                # Iterate through speciesConsensus to create rows for each scientificName entry
                species_list = doc.get("speciesConsensus", [])
                if not isinstance(species_list, list):
                    continue

                for species in species_list:
                    sci_name = species.get("scientificName")
                    if sci_name is not None:
                        # Coalesce scientificName and observationType
                        species_id = sci_name or species.get("observationType", "")

                        row = {
                            "timestamp": doc.get("timestamp"),
                            "speciesIdentification": species_id,
                            "consensusStatus": doc.get("consensusStatus"),
                            "publicURL": doc.get("publicURL"),
                            "videoUrl": doc.get("videoUrl"),
                            "confHuman": conf_human,
                            "observationCount": species.get("observationCount", "")
                        }
                        writer.writerow(row)
                        count += 1

        print(f"Successfully exported {count} records to {output_file}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export media information where an animal is in frame to CSV.")
    parser.add_argument(
        "--output",
        type=str,
        default="animal_media_export.csv",
        help="Output CSV file path (default: animal_media_export.csv)"
    )
    args = parser.parse_args()

    export_animal_media_csv(args.output)
