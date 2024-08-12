import sqlite3
from utilities import record_exists

def insert_into_db(storage_info):
    if not record_exists.record_exists(storage_info):
        # Your existing insertion code goes here...
        # For instance:
        try:
            conn = sqlite3.connect('object_storage_address.db')
            cursor = conn.cursor()

            query = """
                INSERT INTO storage_info(distributedStorageAddress, minio_access_key, minio_secret_key, bucket_name, object_name) 
                VALUES (?, ?, ?, ?, ?)
            """
            cursor.execute(query, (
                storage_info['distributedStorageAddress'],
                storage_info['minio_access_key'],
                storage_info['minio_secret_key'],
                storage_info['bucket_name'],
                storage_info['object_name']
            ))
            conn.commit()

        finally:
            conn.close()
