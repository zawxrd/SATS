import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path("sats_bot.db")

def check_db():
    if not DB_PATH.exists():
        print(f"❌ DB not found at {DB_PATH}")
        return
    
    conn = sqlite3.connect(DB_PATH)
    
    tables = ['signals', 'tp_sl_events', 'trade_closes', 'symbol_stats']
    for table in tables:
        try:
            count = pd.read_sql_query(f"SELECT COUNT(*) as cnt FROM {table}", conn).iloc[0]['cnt']
            print(f"Table {table}: {count} records")
            if count > 0:
                print(f"Last 3 records from {table}:")
                print(pd.read_sql_query(f"SELECT * FROM {table} ORDER BY id DESC LIMIT 3", conn))
                print("-" * 50)
        except Exception as e:
            print(f"Error checking {table}: {e}")
            
    conn.close()

if __name__ == "__main__":
    check_db()
