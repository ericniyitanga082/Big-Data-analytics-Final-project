
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone

import happybase

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"

HBASE_HOST = "localhost"
HBASE_PORT = 9091
TABLE_NAME = "user_activity"

MAX_TIMESTAMP_MS = 9999999999999


def encode_value(value):
    """Convert Python values into HBase-compatible bytes."""
    if isinstance(value, (dict, list)):
        value = json.dumps(value, ensure_ascii=False)
    return str(value).encode("utf-8")


def make_row_key(session):
    """Create a user-prefixed, reverse-timestamp row key."""
    dt = datetime.fromisoformat(
        session["start_time"].replace("Z", "+00:00")
    )

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    timestamp_ms = int(dt.timestamp() * 1000)
    reverse_timestamp = MAX_TIMESTAMP_MS - timestamp_ms

    return (
        f"{session['user_id']}#"
        f"{reverse_timestamp:013d}#"
        f"{session['session_id']}"
    ).encode("utf-8")


def make_columns(session):
    """Map the nested JSON session into HBase column families."""
    device = session.get("device_profile") or {}
    geo = session.get("geo_data") or {}

    fields = {
        "session:start_time": session.get("start_time", ""),
        "session:end_time": session.get("end_time", ""),
        "session:duration_seconds": session.get("duration_seconds", 0),
        "session:conversion_status": session.get("conversion_status", ""),
        "session:referrer": session.get("referrer", ""),
        "session:session_id": session["session_id"],
        "session:user_id": session["user_id"],

        "device:type": device.get("type", ""),
        "device:os": device.get("os", ""),
        "device:browser": device.get("browser", ""),

        "geo:city": geo.get("city", ""),
        "geo:state": geo.get("state", ""),
        "geo:country": geo.get("country", ""),

        "activity:viewed_products": session.get("viewed_products", []),
        "activity:page_views": session.get("page_views", []),
        "activity:cart_contents": session.get("cart_contents", {}),
    }

    return {
        column.encode("utf-8"): encode_value(value)
        for column, value in fields.items()
    }


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--files",
        type=int,
        default=1,
        help="Number of session JSON files to import"
    )

    parser.add_argument(
        "--start",
        type=int,
        default=0,
        help="Session file number to start importing from"
    )

    args = parser.parse_args()

    session_files = sorted(
        DATA_DIR.glob("sessions_*.json"),
        key=lambda p: int(p.stem.split("_")[-1])
    )
    if not session_files:
        raise FileNotFoundError("No session JSON batches found.")

    if args.files < 1 or args.files > len(session_files):
        raise ValueError("Invalid number of session files requested.")

    connection = happybase.Connection(
        HBASE_HOST,
        port=HBASE_PORT,
        timeout=30000
    )

    total = 0

    try:
        connection.open()
        table = connection.table(TABLE_NAME)

        for file_path in session_files[args.start:args.start + args.files]:
            with file_path.open("r", encoding="utf-8") as file:
                sessions = json.load(file)

            with table.batch(batch_size=500) as batch:
                for session in sessions:
                    batch.put(
                        make_row_key(session),
                        make_columns(session)
                    )
                    total += 1

            print(
                f"Loaded {file_path.name}: "
                f"{len(sessions):,} sessions"
            )

        print(f"\nTotal sessions processed: {total:,}")

    finally:
        connection.close()


if __name__ == "__main__":
    main()
