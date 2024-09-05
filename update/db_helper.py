from dotenv import load_dotenv
import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.abspath('__file__'))
load_dotenv(os.path.join(BASE_DIR, '.env'))

SQL_DATABASE = os.getenv("SQL_DATABASE")

# Connect to the db
with sqlite3.connect(SQL_DATABASE) as conn:
    cursor = conn.cursor()

    # Create sections table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sections (
            id BIGINT PRIMARY KEY,
            section VARCHAR(150) UNIQUE NOT NULL,
            url VARCHAR(1000) NOT NULL,
            created TIMESTAMP
        );
    ''')
    conn.commit()
    print("Sections table created successfully.")
    cursor.close()
