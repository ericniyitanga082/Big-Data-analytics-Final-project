import json
import uuid
from pathlib import Path

data_dir = Path(r"D:\AUCA_Ecommerce_BigData\data")
file_path = data_dir / "sessions_82.json"

old_id = "sess_3a15f1cc10"
target_user = "user_004170"

with open(file_path, "r", encoding="utf-8") as f:
    sessions = json.load(f)

matches = [
    s for s in sessions
    if s.get("session_id") == old_id
    and s.get("user_id") == target_user
]

if len(matches) != 1:
    raise ValueError(f"Expected exactly one matching session, found {len(matches)}")

# Use a full UUID to minimize the risk of another collision
new_id = "sess_" + uuid.uuid4().hex

matches[0]["session_id"] = new_id

with open(file_path, "w", encoding="utf-8") as f:
    json.dump(sessions, f)

print("Duplicate session ID corrected.")
print("Old ID:", old_id)
print("New ID:", new_id)
print("Updated file:", file_path.name)
print("Other session remains unchanged.")