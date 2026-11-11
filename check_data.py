# check_data.py - counts what is inside your generated dataset
# Usage:  python check_data.py            (reads JSON files in the current folder)
#         python check_data.py data_small (reads JSON files in that folder)
import json, glob, os, sys

folder = sys.argv[1] if len(sys.argv) > 1 else "."

users = json.load(open(os.path.join(folder, "users.json")))
products = json.load(open(os.path.join(folder, "products.json")))
txns = json.load(open(os.path.join(folder, "transactions.json")))
print("users", len(users), "| products", len(products), "| transactions", len(txns))

no_session = sum(1 for t in txns if t["session_id"] is None)
print("transactions with no session:", no_session)

sess_files = sorted(glob.glob(os.path.join(folder, "sessions_*.json")))
total = conv = aband = brow = 0
for f in sess_files:
    for s in json.load(open(f)):
        total += 1
        c = s["conversion_status"]
        conv += c == "converted"
        aband += c == "abandoned"
        brow += c == "browsed"
print("session files:", sess_files)
print("sessions", total, "| converted", conv, "| abandoned", aband, "| browsed", brow)
