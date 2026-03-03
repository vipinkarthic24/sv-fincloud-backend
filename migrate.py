import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / 'sv_fincloud.db'

def add_income_column():
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    try:
        # Add the column to the loans table
        cursor.execute("ALTER TABLE loans ADD COLUMN monthly_income REAL")
        print("Success: monthly_income added to loans table")
    except sqlite3.OperationalError:
        print("Column already exists or table not found")
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    add_income_column()