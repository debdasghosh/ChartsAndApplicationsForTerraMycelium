import sqlite3

def ensure_table_exists(table_name):
    conn = sqlite3.connect(table_name)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS storage_info (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        distributedStorageAddress TEXT NOT NULL,
        minio_access_key TEXT NOT NULL,
        minio_secret_key TEXT NOT NULL,
        bucket_name TEXT NOT NULL,
        object_name TEXT NOT NULL
    )
    """)
    conn.commit()
    conn.close()