import sqlite3

def fetch_all_customer_data_from_sqlite():
    conn = sqlite3.connect('customer_data.db')
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM customer_data")
    data = cursor.fetchall()

    conn.close()
    return data
