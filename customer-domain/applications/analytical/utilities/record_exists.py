import sqlite3

def record_exists(storage_info):
    try:
        conn = sqlite3.connect('object_storage_address.db')
        cursor = conn.cursor()
        
        query = """
            SELECT EXISTS(SELECT 1 
                          FROM storage_info 
                          WHERE distributedStorageAddress=? 
                          AND minio_access_key=? 
                          AND minio_secret_key=? 
                          AND bucket_name=? 
                          AND object_name=?)
        """
        
        cursor.execute(query, (
            storage_info['distributedStorageAddress'],
            storage_info['minio_access_key'],
            storage_info['minio_secret_key'],
            storage_info['bucket_name'],
            storage_info['object_name']
        ))
        
        exists = cursor.fetchone()[0]
        return exists == 1

    finally:
        conn.close()
