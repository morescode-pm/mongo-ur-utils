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
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGO_DB", "urbanrivers")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION", "medias")

def get_mongo_connection() -> Tuple[MongoClient, Database, Collection]:
    """Get MongoDB connection, database and collection"""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    return client, db, collection

def update_mongo_records(json_file: str, operation: str = "update") -> None:
    """
    Update MongoDB records with AI results using bulk operations.

    Args:
        json_file (str): Path to the JSON file with detection results
        operation (str): One of 'update', or 'replace'
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

        # Process each record
        for media_id, data in results.items():
            item_count +=1
            if operation == "update":
                print('Starting UPDATE Operation')
                op = UpdateOne(
                    {"mediaID": media_id},
                    {"$push": {"aiResults": {"$each": data["aiResults"]}}},
                    upsert=True,
                )
            elif operation == "replace":
                print('Starting REPLACE operation')
                # Standardizing to mediaID as discussed in the problem description
                op = UpdateOne(
                    {"mediaID": media_id},
                    {"$set": {"aiResults": data["aiResults"]}},
                    upsert=True,
                )
            else:
                # Should not happen if input is validated, but good practice
                print(f"Unknown operation: {operation} for mediaID: {media_id}")
                errors += 1
                continue

            operations.append(op)

            if len(operations) == BATCH_SIZE or item_count == total:
                try:
                    if operations: # ensure operations list is not empty
                        bulk_result = collection.bulk_write(operations)
                        # Count processed based on matched, modified and upserted
                        # For $push, matched_count is more relevant if items are not always new
                        # For $set, modified_count or upserted_count are relevant
                        processed += bulk_result.matched_count + bulk_result.upserted_count
                        print(f"Processed batch of {len(operations)}. Total processed: {processed}/{total}")
                        operations = []  # Reset for the next batch
                except BulkWriteError as bwe:
                    print(f"Bulk write error: {bwe.details}")
                    # Increment errors by the number of write errors
                    errors += len(bwe.details.get('writeErrors', []))
                    # If you need to count how many operations succeeded despite the error,
                    # you might need to inspect bwe.details further, e.g. result.nModified, result.nUpserted
                    # For simplicity, we're counting successful batches' effects on `processed`
                    # and failed ones are captured in `errors`.
                    # If an entire batch fails, `processed` won't be updated for that batch here.
                    # Depending on desired atomicity, this might need adjustment.
                    # The current `processed` count relies on `bulk_result` which is only available on success.
                    # A more accurate processed count in case of BulkWriteError would be:
                    # processed += bulk_result.matched_count + bulk_result.upserted_count before error
                    # And then for the error case:
                    # successful_in_error = bwe.details['nModified'] + bwe.details['nUpserted'] # etc.
                    # For now, we assume a batch either largely succeeds or its errors are counted.
                except Exception as e:
                    print(f"Error during bulk write: {str(e)}")
                    errors += len(operations) # Assuming all operations in the batch failed
                    operations = [] # Reset for the next batch

        # Final print to show completion, even if total is 0 or last batch was smaller
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
    print("1. update - Add new AI results to existing documents")
    print("2. replace - Replace existing AI results")
    
    operation = input("\nSelect operation type (update/replace): ").lower()
    if operation not in ["update", "replace"]:
        print("Invalid operation type. Please choose 'update', or 'replace'.")
        return

    # Process the records
    update_mongo_records(json_file, operation)

if __name__ == "__main__":
    main()
