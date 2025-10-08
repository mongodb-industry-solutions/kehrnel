import json
from glob import glob
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

KEHRNEL_BASE_URL = 'http://localhost:9000/v1'
EHRBASE_URL = 'http://localhost:8080/ehrbase/rest/openehr/v1'

# This is the directory where we want to take the compositions
# In this initial case we use the hads directory
PATH_TO_JSON_FILES = './sample_compositions/*.json'

# Ideally it should be the number of CPUs in the Macbook
MAX_WORKERS = 5

# Create a session object for the requests
session = requests.Session()

# The headers always contain application/json since the composition is a json file
session.headers.update({"Content-Type": "application/json"})


def process_composition_file(file_path):
    try:
        # Open the composition file
        with open(file_path, 'r') as file:
            composition_data = json.load(file)
            if composition_data:
                # Create the EHR using KEHRNEL
                create_ehr = session.post(f'{KEHRNEL_BASE_URL}/ehr')
                
                # Stores the created ehr_id in order to use it to create the EHR in EHRBase
                ehr_id = str(create_ehr.headers["etag"].strip('"'))

                # Create the EHR using EHRBase
                create_ehr_base = session.put(f'{EHRBASE_URL}/ehr/{ehr_id}')

                print("EHRBase headers:", create_ehr_base.headers)
                
                # Creates the composition, the endpoint creates the Canonical and Semi-Flatten using KEHRNEL
                create_composition_response = session.post(
                    f'{KEHRNEL_BASE_URL}/ehr/{ehr_id}/composition',
                    json=composition_data
                )

                # Creates the composition, using eht EHRBase endpoint
                create_composition_ehrbase_response = session.post(
                    f'{EHRBASE_URL}/ehr/{ehr_id}/composition',
                    json=composition_data
                )
                
                create_composition_response.raise_for_status()
                create_composition_ehrbase_response.raise_for_status()

                composition_id = create_composition_response.headers["etag"].strip('"')
                composition_ehrbase_id = create_composition_ehrbase_response.headers["etag"].strip('"')
                return (file_path, 'success', ehr_id, composition_id, composition_ehrbase_id)
            else:
                return (file_path, 'skipped', 'Empty file')
    except FileNotFoundError:
        return (file_path, 'error', 'File not found')
    except json.JSONDecodeError:
        return (file_path, 'error', 'Invalid JSON format')
    except requests.exceptions.RequestException as e:
        return (file_path, 'error', f'Network error: {e}')
    except KeyError as e:
        return (file_path, 'error', f'Missing expected header in response: {e}')
    except Exception as e:
        return (file_path, 'error', f'An unexpected error occurred: {e}')

def main():
    composition_files = glob(PATH_TO_JSON_FILES)
    if not composition_files:
        print("No Composition files found in the specified path.")
        return

    print(f"Found {len(composition_files)} files to process with up to {MAX_WORKERS} parallel workers.")

    successful_inserts = []
    failed_inserts = []

    # Use ThreadPoolExecutor to process files concurrently
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_file = {executor.submit(process_composition_file, file_path): file_path for file_path in composition_files}

        for future in tqdm(as_completed(future_to_file), total=len(composition_files), desc="Processing compositions"):
            result = future.result()

            # Store the returned values from the process_composition_file function
            file_path, status, *details = result
            if status == "success":
                ehr_id, composition_id, composition_ehrbase_id = details
                successful_inserts.append(result)
                print("EHR ID: ", ehr_id)
                print("KERHNEL Composition ID: ", composition_id)
                print("EHR ID: ", ehr_id)
                break
            else:
                error_message = details[0]
                print(f"FAILED: {file_path} -> Reason: {error_message}")
                failed_inserts.append(result)
            break

    print("\n--- Processing Complete ---")
    print(f"Total Successful: {len(successful_inserts)}")
    print(f"Total Failed: {len(failed_inserts)}")

if __name__ == "__main__":
    main()