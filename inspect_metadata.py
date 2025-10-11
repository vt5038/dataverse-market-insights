import sqlite3
import pandas as pd

# Connect to your metadata database
conn = sqlite3.connect("metadata.db")

# List all tables in the database
tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
print("📂 Tables in metadata.db:\n", tables, "\n")

# For example, view the extraction logs (replace table name if different)
try:
    logs = pd.read_sql_query("SELECT * FROM extraction_logs;", conn)
    print("🪵 Extraction Logs:\n", logs)
except Exception as e:
    print("⚠️ Couldn't read 'extraction_logs' table:", e)

conn.close()
