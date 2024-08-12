import requests
from utilities import fetch_data_from_minio_and_create_metadata

def register_metadata_to_data_lichen():
    metadata = fetch_data_from_minio_and_create_metadata.fetch_data_from_minio_and_create_metadata()
    
    # Send metadata to Data Lichen
    response = requests.post('http://localhost:3001/register', json=metadata)
    if response.status_code == 200:
        print(response.json()['message'])
    else:
        print("Failed to register metadata with Data Lichen.")