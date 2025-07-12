import json
import os
from typing import Tuple
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import BulkWriteError

# Load environment variables
load_dotenv()

# MongoDB connection configuration
MONGO_URI = os.getenv("MONGO_URI_PROD", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGO_DB", "urbanrivers")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION", "medias")

def get_mongo_connection() -> Tuple[MongoClient, Database, Collection]:
    """Get MongoDB connection, database and collection"""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    return client, db, collection

def update_mongo_records(json_file: str, operation: str = "append") -> None:
    """
    Update MongoDB records with AI results using bulk operations.

    Args:
        json_file (str): Path to the JSON file with detection results
        operation (str): One of 'append', or 'replace'
    """
    # Get MongoDB connection
    client, db, collection = get_mongo_connection()

    BATCH_SIZE = 10000
    operations = []
    processed = 0
    errors = 0
    total = 0


    try:
        # Load the JSON file
        with open(json_file, "r") as f:
            results = json.load(f)

        total = len(results)
        item_count = 0

        media_ids = list(results.keys())
        print(f"\nTotal mediaIDs in {json_file}: {len(media_ids)}")

        if operation == "append":
            # Step 1: Query DB for all mediaIDs in the file
            print(f"\nStarting Append Operations...\n")
            db_docs = collection.find({"mediaID": {"$in": media_ids}}, {"mediaID": 1, "aiResults": 1})
            db_map = {doc["mediaID"]: doc for doc in db_docs}

            not_found = []
            already_has_airesults = []
            to_update = []

            for media_id in media_ids:
                doc = db_map.get(media_id)
                if not doc:
                    not_found.append(media_id)
                elif "aiResults" in doc:
                    already_has_airesults.append(media_id)
                else:
                    to_update.append(media_id)

            # Print stats before proceeding
            print(f"\nAppend operation pre-check:")
            print(f"MediaIDs not found in DB: {len(not_found)}")
            print(f"MediaIDs already with aiResults: {len(already_has_airesults)}")
            print(f"MediaIDs to be updated (no aiResults): {len(to_update)}")

            if not to_update:
                print("No records to update. Exiting.")
                return

            # Only process those to be updated
            print(f"\nStarting append Operation for {len(to_update)} records\n")
            for media_id in to_update:
                data = results[media_id]
                op = UpdateOne(
                    {"mediaID": media_id},
                    {"$addToSet": {"aiResults": {"$each": data["aiResults"]}}},
                    upsert=False,
                )
                operations.append(op)
                item_count += 1

                if len(operations) == BATCH_SIZE or item_count == len(to_update):
                    try:
                        if operations:
                            bulk_result = collection.bulk_write(operations)
                            processed += bulk_result.matched_count + bulk_result.upserted_count
                            print(f"Processed batch of {len(operations)}. Total processed: {processed}/{len(to_update)}")
                            operations = []
                    except BulkWriteError as bwe:
                        print(f"Bulk write error: {bwe.details}")
                        errors += len(bwe.details.get('writeErrors', []))
                    except Exception as e:
                        print(f"Error during bulk write: {str(e)}")
                        errors += len(operations)
                        operations = []

            print(f"\nAppend operation complete:")
            print(f"Records not found in DB: {len(not_found)}")
            print(f"Records already with aiResults: {len(already_has_airesults)}")
            print(f"Records updated: {processed}")
            print(f"Errors: {errors}")
            return

        # ...existing code for replace operation...
        print(f'Starting {operation} Operation\n')
        for media_id, data in results.items():
            item_count +=1
            if operation == "replace":
                op = UpdateOne(
                    {"mediaID": media_id},
                    {"$set": {"aiResults": data["aiResults"]}},
                    upsert=False,
                )
            else:
                print(f"Unknown operation: {operation} for mediaID: {media_id}")
                errors += 1
                continue

            operations.append(op)

            if len(operations) == BATCH_SIZE or item_count == total:
                try:
                    if operations:
                        bulk_result = collection.bulk_write(operations)
                        processed += bulk_result.matched_count + bulk_result.upserted_count
                        print(f"Processed batch of {len(operations)}. Total processed: {processed}/{total}")
                        operations = []
                except BulkWriteError as bwe:
                    print(f"Bulk write error: {bwe.details}")
                    errors += len(bwe.details.get('writeErrors', []))
                except Exception as e:
                    print(f"Error during bulk write: {str(e)}")
                    errors += len(operations)
                    operations = []

        print(f"\nOperation complete:")
        print(f"Total records to process: {total}")
        print(f"Successfully processed (based on matched/upserted): {processed}")
        print(f"Errors: {errors}")

    except FileNotFoundError:
        print(f"Error: JSON file {json_file} not found.")
        # No client.close() here as it's handled in finally
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {json_file}.")
        # No client.close() here
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        # errors += total - processed # Or some other logic for unhandled exceptions
    finally:
        if 'client' in locals() and client: #Ensure client exists before closing
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
    print("1. append - Add new AI results to existing documents if they don't exist")
    print("2. replace - Replace existing AI results")
    
    operation = input("\nSelect operation type (append/replace): ").lower()
    if operation not in ["append", "replace"]:
        print("Invalid operation type. Please choose 'append', or 'replace'.")
        return

    # Process the records
    update_mongo_records(json_file, operation)

if __name__ == "__main__":
    main()
