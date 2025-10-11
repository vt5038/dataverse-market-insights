"""
metadata_logger.py
------------------
Handles logging of extraction metadata into a local SQLite database.
Tracks record counts, runtime, status, and S3 paths for auditability.
"""

import sqlite3
import os
from datetime import datetime

DB_PATH = "metadata.db"

# Ensure database exists
def init_metadata_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS extraction_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_timestamp TEXT,
            source_api TEXT,
            record_count INTEGER,
            status TEXT,
            s3_path TEXT,
            runtime_seconds REAL
        )
    """)
    conn.commit()
    conn.close()

def log_metadata(source_api, record_count, status, s3_path, runtime_seconds):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO extraction_logs 
        (run_timestamp, source_api, record_count, status, s3_path, runtime_seconds)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        source_api,
        record_count,
        status,
        s3_path,
        runtime_seconds
    ))
    conn.commit()
    conn.close()
