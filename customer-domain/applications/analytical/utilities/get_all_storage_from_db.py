import sqlite3

def get_all_storage_from_db():
    """Fetch all object information from the SQLite database."""
    conn = sqlite3.connect('object_storage_address.db')
    cursor = conn.cursor()
    
    # Fetch all records from storage_info table
    cursor.execute("SELECT distributedStorageAddress, minio_access_key, minio_secret_key, bucket_name, object_name FROM storage_info")
    records = cursor.fetchall()
    
    # If there are no records, return an empty list
    if not records:
        return []

    storage_info_list = [
        {
            "distributedStorageAddress": record[0],
            "minio_access_key": record[1],
            "minio_secret_key": record[2],
            "bucket_name": record[3],
            "object_name": record[4]
        }
        for record in records if all(record)
    ]
    
    conn.close()
    
    return storage_info_list
