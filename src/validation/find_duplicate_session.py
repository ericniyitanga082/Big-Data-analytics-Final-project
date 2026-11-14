import json
import sqlite3
from pathlib import Path

DATA_DIR = Path(r"D:\AUCA_Ecommerce_BigData\data")
DB_PATH = DATA_DIR / "session_duplicate_check.sqlite"

files = sorted(DATA_DIR.glob("sessions_*.json"))

with sqlite3.connect(DB_PATH) as conn:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS seen_sessions (
            session_id TEXT PRIMARY KEY,
            source_file TEXT
        )
    """)

    for number, path in enumerate(files, 1):
        with open(path, "r", encoding="utf-8") as f:
            sessions = json.load(f)

        for session in sessions:
            session_id = session["session_id"]

            try:
                conn.execute(
                    "INSERT INTO seen_sessions VALUES (?, ?)",
                    (session_id, path.name)
                )
            except sqlite3.IntegrityError:
                original = conn.execute(
                    "SELECT source_file FROM seen_sessions WHERE session_id = ?",
                    (session_id,)
                ).fetchone()

                print("DUPLICATE FOUND")
                print("Session ID:", session_id)
                print("First file:", original[0])
                print("Repeated in:", path.name)

        conn.commit()

        if number % 20 == 0:
            print(f"Checked {number}/{len(files)} files")

    print("Duplicate search completed.")