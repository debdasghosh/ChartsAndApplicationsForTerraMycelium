import requests
import json
import base64
from utilities import insert_into_db

def consume_records(url, headers, storage_info={}):

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"GET /records/ did not succeed: {response.text}")
    else:
        records = response.json()
        for record in records:
            print(record)
            decoded_key = base64.b64decode(record['key']).decode('utf-8') if record['key'] else None
            decoded_value_json = base64.b64decode(record['value']).decode('utf-8')
            value_obj = json.loads(decoded_value_json)

            storage_info = {
            "distributedStorageAddress": value_obj.get('distributedStorageAddress', ''),
            "minio_access_key": value_obj.get('minio_access_key', ''),
            "minio_secret_key": value_obj.get('minio_secret_key', ''),
            "bucket_name": value_obj.get('bucket_name', ''),
            "object_name": value_obj.get('object_name', '')
            }

            # Insert the storage info into the SQLite database
            insert_into_db.insert_into_db(storage_info)

            print(f"Consumed record with key {decoded_key} and value {value_obj['message']} from topic {record['topic']}")
            if 'distributedStorageAddress' in value_obj:
                print(f"Distributed storage address: {value_obj['distributedStorageAddress']}")
                print(f"Minio access key: {value_obj['minio_access_key']}")
                print(f"Minio secret key: {value_obj['minio_secret_key']}")
                print(f"Bucket name: {value_obj['bucket_name']}")
                print(f"Object name: {value_obj['object_name']}")

