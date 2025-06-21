import os
import json
import shutil
from pymongo import MongoClient
from collections import defaultdict
from zipfile import ZipFile
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MongoDB connection configuration
MONGO_URI = os.getenv("MONGO_URI_DEV", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGO_DB", "urbanrivers")
# COLLECTION_NAME = os.getenv("MONGO_COLLECTION", "medias")

OUTPUT_DIR = "mongo_exports"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def merge_types(existing, new):
    if isinstance(existing, set):
        existing.add(new)
        return existing
    return {existing, new}

def infer_schema(doc, schema=None):
    if schema is None:
        schema = {}
    for key, value in doc.items():
        if isinstance(value, dict):
            schema.setdefault(key, {})
            infer_schema(value, schema[key])
        elif isinstance(value, list):
            schema.setdefault(key, {"type": "list", "items": {}})
            for item in value:
                if isinstance(item, dict):
                    infer_schema(item, schema[key]["items"])
                else:
                    item_type = type(item).__name__
                    existing_type = schema[key].get("item_type")
                    schema[key]["item_type"] = merge_types(existing_type, item_type) if existing_type else item_type
        else:
            value_type = type(value).__name__
            existing_type = schema.get(key)
            schema[key] = merge_types(existing_type, value_type) if existing_type else value_type
    return schema

def get_schema_recursive(collection, sample_size=200):
    schema = {}
    for doc in collection.find().limit(sample_size):
        schema = infer_schema(doc, schema)
    return schema

def get_sample_docs_by_type(collection, sample_size=1000, type_field="type"):
    type_to_doc = {}
    for doc in collection.find().limit(sample_size):
        doc_type = doc.get(type_field) or doc.get("record_type") or "unknown"
        doc_len = len(json.dumps(doc, default=str))
        if doc_type not in type_to_doc or doc_len > type_to_doc[doc_type][0]:
            type_to_doc[doc_type] = (doc_len, doc)
    return {k: v[1] for k, v in type_to_doc.items()}

def export_collection(db, collection_name):
    print(f"Processing: {collection_name}")
    collection = db[collection_name]

    schema = get_schema_recursive(collection)
    samples = get_sample_docs_by_type(collection)

    with open(os.path.join(OUTPUT_DIR, f"schema_{collection_name}.json"), "w") as f:
        json.dump(convert_sets_to_lists(schema), f, indent=2)

    with open(os.path.join(OUTPUT_DIR, f"samples_{collection_name}.json"), "w") as f:
        json.dump(samples, f, indent=2, default=str)

def zip_output_files(zip_name="mongo_exports.zip"):
    with ZipFile(zip_name, 'w') as zipf:
        for root, _, files in os.walk(OUTPUT_DIR):
            for file in files:
                zipf.write(os.path.join(root, file), arcname=file)

def convert_sets_to_lists(obj):
    if isinstance(obj, dict):
        return {k: convert_sets_to_lists(v) for k, v in obj.items()}
    elif isinstance(obj, set):
        return list(obj)
    elif isinstance(obj, list):
        return [convert_sets_to_lists(i) for i in obj]
    else:
        return obj

# --- Mongo connection ---
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# --- Process all collections ---
for coll_name in db.list_collection_names():
    export_collection(db, coll_name)

# --- Zip everything ---
zip_output_files()
print("✅ Done! Exported schemas and samples to mongo_exports.zip")
