from datetime import datetime
import time 
from utilities import fetch_data_from_minio, save_data_to_sqlite, create_metadata, get_all_storage_from_db
from utilities import save_data_to_sqlite

def fetch_data_from_minio_and_create_metadata():
    print("Starting data fetching and metadata creation process...")

    actual_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_time = time.time()

    print("Fetching storage information from SQLite database...")
    all_storage_info = get_all_storage_from_db.get_all_storage_from_db()

    print(f"Storage Info: {all_storage_info}")
    print(f'Total number of objects to receive: {len(all_storage_info)}')

    for storage_info in all_storage_info:
        print(f"Fetching data from Minio for storage: {storage_info}...")
        
        # Update dictionary keys to match function parameters
        storage_info_updated = {
            'distributed_storage_address': storage_info['distributedStorageAddress'],
            'minio_access_key': storage_info['minio_access_key'],
            'minio_secret_key': storage_info['minio_secret_key'],
            'bucket_name': storage_info['bucket_name'],
            'object_name': storage_info['object_name']
        }
        
        data_str = fetch_data_from_minio.fetch_data_from_minio(**storage_info_updated)
        print(f"Saving data from storage {storage_info} to SQLite...")
        save_data_to_sqlite.save_data_to_sqlite(data_str, 'weather-domain-data.db')


    processing_duration = time.time() - start_time
    print(f"Creating metadata... (Processing duration: {processing_duration} seconds)")
    metadata = create_metadata.create_metadata(actual_time, processing_duration, data_str)

    print("Data fetching and metadata creation process completed.")
    return metadata




