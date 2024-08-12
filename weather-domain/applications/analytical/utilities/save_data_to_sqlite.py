import sqlite3
import csv
from io import StringIO

CHUNK_SIZE = 500  # For example, save 1000 rows at a time

def save_data_to_sqlite(data_str, db_path):
    # Convert string data into a file-like object for csv reader
    csv_file = StringIO(data_str)
    reader = csv.reader(csv_file)
    
    # Extract headers (column names) from the first row
    headers = next(reader)

    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table if it doesn't exist
    columns = ', '.join([f'"{col}" TEXT' for col in headers])
    table_name = "weather_data"
    sql_create_table_command = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
    cursor.execute(sql_create_table_command)

    # Create a list to store rows in a chunk
    chunk_data = []

    for row in reader:
        chunk_data.append(row)
        
        # If the chunk size is reached, save the chunk to the database
        if len(chunk_data) == CHUNK_SIZE:
            placeholders = ', '.join(['?'] * len(headers))
            sql_insert_command = f"INSERT INTO {table_name} VALUES ({placeholders})"
            cursor.executemany(sql_insert_command, chunk_data)
            chunk_data = []

    # Save any remaining rows that didn't form a complete chunk
    if chunk_data:
        placeholders = ', '.join(['?'] * len(headers))
        sql_insert_command = f"INSERT INTO {table_name} VALUES ({placeholders})"
        cursor.executemany(sql_insert_command, chunk_data)

    # Commit the changes and close the connection
    conn.commit()
    conn.close()
