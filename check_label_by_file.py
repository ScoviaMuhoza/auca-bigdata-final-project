"""
check_label_by_file.py - does the 'converted' label match real transactions, file by file?
Run from the project root:  python check_label_by_file.py
"""
import json, glob, os

txn_sessions = {t["session_id"] for t in json.load(open("transactions.json")) if t["session_id"]}
print("transactions linked to a session:", len(txn_sessions))
print(f"{'file':20s} {'sessions':>9s} {'converted':>10s} {'converted+txn':>14s} {'converted, NO txn':>18s}")
for f in sorted(glob.glob("sessions_*.json")):
    sess = json.load(open(f))
    conv = [s for s in sess if s["conversion_status"] == "converted"]
    ok = sum(1 for s in conv if s["session_id"] in txn_sessions)
    print(f"{os.path.basename(f):20s} {len(sess):>9,} {len(conv):>10,} {ok:>14,} {len(conv) - ok:>18,}")
    # position of the first 'converted without transaction' inside the file
    first = next((i for i, s in enumerate(sess) if s["conversion_status"] == "converted" and s["session_id"] not in txn_sessions), None)
    print(f"   first converted-without-txn at position: {first}")
