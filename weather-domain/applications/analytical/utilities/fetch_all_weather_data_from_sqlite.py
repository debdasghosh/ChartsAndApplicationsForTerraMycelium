import sqlite3

def fetch_all_weather_data_from_sqlite():
    conn = sqlite3.connect('weather-domain-data.db')
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM weather_data")
    data = cursor.fetchall()

    conn.close()
    return data