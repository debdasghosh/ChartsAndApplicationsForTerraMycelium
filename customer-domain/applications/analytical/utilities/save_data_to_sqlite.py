import sqlite3
import json
import hashlib

def compute_hash(data_str):
    return hashlib.sha256(data_str.encode('utf-8')).hexdigest()

def save_data_to_sqlite(data_str):
    conn = sqlite3.connect('customer_data.db')
    cursor = conn.cursor()

    # Compute the hash of the data
    data_hash = compute_hash(data_str)

    # Ensure table exists
    cursor.execute("CREATE TABLE IF NOT EXISTS customer_data (id INTEGER PRIMARY KEY, data TEXT, data_hash TEXT UNIQUE)")

    # Check if data_hash already exists
    cursor.execute("SELECT * FROM customer_data WHERE data_hash=?", (data_hash,))
    existing_data = cursor.fetchone()

    # If the data does not already exist, save it
    if not existing_data:
        try:
            cursor.execute("INSERT INTO customer_data (data, data_hash) VALUES (?, ?)", (data_str, data_hash))
        except Exception as e:
            print(f"Error while inserting data into SQLite: {e}")
    else:
        print(f"Data with hash {data_hash} already exists. Skipping.")

    conn.commit()
    conn.close()


# def save_data_to_sqlite(data_str):
#     conn = sqlite3.connect('customer_data.db')
#     cursor = conn.cursor()
    
#     lines = data_str.strip().split('\n')
#     data_json = [json.loads(line) for line in lines]

#     if not data_json:
#         print("No data to save!")
#         return

#     first_item = data_json[0]

#     # Ensure table exists
#     columns = ', '.join([f'"{col}" TEXT' for col in first_item.keys()])
#     cursor.execute(f"CREATE TABLE IF NOT EXISTS customer_data ({columns})")

#     # Check each item in data_json
#     for item in data_json:
#         keys = [f'"{k}"' for k in item.keys()]

#         # Check if columns exist, if not, add them
#         for k in keys:
#             cursor.execute("PRAGMA table_info(customer_data)")
#             columns_info = cursor.fetchall()
#             column_names = [column[1] for column in columns_info]
#             if k.replace('"', '') not in column_names:
#                 cursor.execute(f"ALTER TABLE customer_data ADD COLUMN {k} TEXT")

#         # Convert nested dictionary values to string
#         values = [json.dumps(value) if isinstance(value, dict) else value for value in item.values()]

#         question_marks = ', '.join(['?' for _ in values])
#         try:
#             cursor.execute(f"INSERT INTO customer_data ({', '.join(keys)}) VALUES ({question_marks})", values)
#         except Exception as e:
#             print(f"Error while inserting data into SQLite: {e}")

#     conn.commit()
#     conn.close()
