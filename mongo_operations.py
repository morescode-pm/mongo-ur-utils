import json
import os
from typing import Tuple
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

# Load environment variables
load_dotenv()

# MongoDB connection configuration
MONGO_URI = os.getenv("MONGO_URI"")
DB_NAME = os.getenv("MONGO_DB")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION")

def get_mongo_connection() -> Tuple[MongoClient, Database, Collection]:
    """Get MongoDB connection, database and collection"""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    return client, db, collection

def update_mongo_records(json_file: str, operation: str = "update") -> None:
    """
    Update MongoDB records with AI model results

    Args:
        json_file (str): Path to the JSON file with detection results
        operation (str): One of 'update', or 'replace'
    """
    # Get MongoDB connection
    client, db, collection = get_mongo_connection()

    try:
        # Load the JSON file
        with open(json_file, "r") as f:
            results = json.load(f)

        # Track statistics
        total = len(results)
        processed = 0
        errors = 0

        # Process each record
        for media_id, data in results.items():
            try:
                if operation == "update":
                    # Update existing document, adding new AI model results
                    collection.update_one(
                        {"mediaID": media_id},
                        {"$push": {"aiModel": {"$each": data["aiModel"]}}},
                        upsert=True,
                    )

                elif operation == "replace":
                    # Replace existing AI model results with new ones
                    collection.update_one(
                        {"mediaId": media_id},
                        {"$set": {"aiModel": data["aiModel"]}},
                        upsert=True,
                    )

                processed += 1
                if processed % 1000 == 0:
                    print(f"Processed {processed}/{total} records")

            except Exception as e:
                print(f"Error processing {media_id}: {str(e)}")
                errors += 1

        print(f"\nOperation complete:")
        print(f"Total records: {total}")
        print(f"Successfully processed: {processed}")
        print(f"Errors: {errors}")

    finally:
        client.close()

def main():
    """Main function to demonstrate usage"""
    # Check if the formatted detections file exists
    json_file = "mongodb_formatted_detections.json"
    if not os.path.exists(json_file):
        print(f"Error: {json_file} not found.")
        print("Please run detection_parser.py first to generate the file.")
        return

    # Ask for operation type
    print("\nAvailable operations:")
    print("1. update - Add new AI model results to existing documents")
    print("2. replace - Replace existing AI model results")
    
    operation = input("\nSelect operation type (update/replace): ").lower()
    if operation not in ["update", "replace"]:
        print("Invalid operation type. Please choose 'update', or 'replace'.")
        return

    # Process the records
    update_mongo_records(json_file, operation)

if __name__ == "__main__":
    main()
