from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

def migrate_emails_to_lowercase(mongo_uri, db_name, collection_name='users'):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    users_collection = db[collection_name]

    try:
        print("Connected to database.")

        users = list(users_collection.find({}))
        updated_count = 0

        for user in users:
            email = user.get("email")
            if email and email != email.lower():
                new_email = email.lower()
                print(f"User ID: {user['_id']}, Email: {email} -> {new_email}")
                users_collection.update_one(
                    {"_id": user["_id"]},
                    {"$set": {"email": new_email}}
                )
                updated_count += 1

        print(f"Migration complete. {updated_count} user(s) updated.")
    except Exception as e:
        print("Error during migration:", str(e))
    finally:
        client.close()
        print("Disconnected from database.")

if __name__ == "__main__":
    db_name = os.getenv('MONGO_DB')
    mongo_uri = os.getenv('MONGO_URI_DEV')
    migrate_emails_to_lowercase(
        mongo_uri=mongo_uri, 
        db_name=db_name, 
        collection_name="users"
    )
