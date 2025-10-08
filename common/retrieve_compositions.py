import os
import json
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from dotenv import load_dotenv
import sys

load_dotenv()

MONGO_URI = os.getenv("MONGODB_URI")
MONGO_DATABASE = "openehr_db"
MONGO_COLLECTION = "samples"
OUTPUT_DIR = "sample_compositions"
MAX_WORKERS = 5


def process_document(doc):
    try:
        if '_id' not in doc or 'canonicalJSON' not in doc:
            return('skipped', 'Missing required fields')
        
        # Get the content from canonicalJSON
        canonical_json = doc["canonicalJSON"]
        doc_id = str(doc['_id'])

        file_path = os.path.join(OUTPUT_DIR, f"{doc_id}.json")

        with open(file_path, 'w', encoding='utf-8') as json_file:
            json.dump(canonical_json, json_file, indent=4, ensure_ascii=False)
        
        return ('Success', file_path)

    except Exception as e:
        return ('error', str(e))

def extract_in_parallel():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Output directory '{OUTPUT_DIR}' is ready.")

    client = None
    successful_saves = 0
    failed_saves = 0

    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DATABASE]
        coll = db[MONGO_COLLECTION]

        total_docs = coll.count_documents({})

        if total_docs == 0:
            print("No documents found in the collection")
            return 
        
        print(f"Found {total_docs} documents. Starting parallel extraction with {MAX_WORKERS} workers.")

        # Use a projection to only extract the files we need
        project_stage = {
            "_id": 1,
            "canonicalJSON": 1
        }

        cursor = coll.find({}, project_stage)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit each document from the cursor to the executor
            futures = (executor.submit(process_document, composition_doc) for composition_doc in cursor)

            # With tqdm create the progress bar as tasks complete
            for future in tqdm(as_completed(futures), total=total_docs, desc="Exporting documents", file=sys.stdout):
                status, detail = future.result()
                if status == 'Success':
                    successful_saves += 1
                else:
                    failed_saves += 1

    except Exception as e:
        print(f"A critical error occurred: {e}")
    finally:
        if client:
            client.close()
            print("MongoDB connection closed.")

    print("\n--- Extraction Complete ---")
    print(f"Successfully saved: {successful_saves} files")
    print(f"Failed or skipped: {failed_saves} files")


if __name__ == "__main__":
    extract_in_parallel()