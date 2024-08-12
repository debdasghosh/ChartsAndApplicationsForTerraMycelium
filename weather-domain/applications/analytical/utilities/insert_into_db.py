import sqlite3

def insert_into_db(storage_info, db_name, table_name="storage_info"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Check if table exists; if not, create it.
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        distributedStorageAddress TEXT,
        minio_access_key TEXT,
        minio_secret_key TEXT,
        bucket_name TEXT,
        object_name TEXT
    )
    """)
    
    # Insert into table
    cursor.execute(f"""
    INSERT INTO {table_name} (distributedStorageAddress, minio_access_key, minio_secret_key, bucket_name, object_name)
    VALUES (?, ?, ?, ?, ?)
    """, (
        storage_info["distributedStorageAddress"],
        storage_info["minio_access_key"],
        storage_info["minio_secret_key"],
        storage_info["bucket_name"],
        storage_info["object_name"]
    ))
    
    conn.commit()
    conn.close()