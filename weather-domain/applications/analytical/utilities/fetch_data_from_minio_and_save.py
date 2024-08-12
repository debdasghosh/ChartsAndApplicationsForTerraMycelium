from utilities import fetch_data_from_minio, create_metadata, save_data_to_sqlite
import requests
import time 


def fetch_data_from_minio_and_save(actual_time):
    start_time = time.time()
    data_str = fetch_data_from_minio.fetch_data_from_minio(
        storage_info["distributedStorageAddress"],
        storage_info["minio_access_key"],
        storage_info["minio_secret_key"],
        storage_info["bucket_name"],
        storage_info["object_name"]
    )

    save_data_to_sqlite.save_data_to_sqlite(data_str, 'weather_data.db')
    processing_duration = time.time() - start_time

    metadata = create_metadata.create_metadata(actual_time, processing_duration, data_str)
    print(metadata)

    # Send metadata to Data Lichen
    response = requests.post('http://localhost:3000/register', json=metadata)
    if response.status_code == 200:
        print(response.json()['message'])
    else:
        print("Failed to register metadata with Data Lichen.")
