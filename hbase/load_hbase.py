"""
load_hbase.py - loads session data into HBase (through the Thrift server on port 9090).

Run from the project root:
    python hbase\\load_hbase.py                    (current folder, first 100,000 sessions)
    python hbase\\load_hbase.py data_small         (small test data)
    python hbase\\load_hbase.py . 200000           (all sessions)

TWO TABLES (be ready to explain this):

1) user_sessions   - one row per SESSION, grouped by user
   row key  : <user_id>#<reverse_timestamp>      e.g. user_000042#9223370...
              reverse_timestamp = Long.MAX - start_time_in_ms (zero-padded to 19 digits)
              -> all sessions of one user are stored next to each other (prefix scan),
              -> and the NEWEST session comes FIRST, because HBase sorts keys ascending.
   families : info = session metadata (device, referrer, geo, duration, conversion status)
              pv   = page views, ONE COLUMN PER EVENT (pv:000, pv:001, ...). Sessions have
                     different numbers of events, so rows are sparse and HBase only stores
                     the cells that exist (wide-column strength).
              cart = cart contents, one column per product (qualifier = product_id)
   Why 3 families? Data in the same family is stored together on disk, so reading
   only 'info' never touches page views or cart data.

2) product_daily_metrics - one row per PRODUCT PER DAY
   row key  : <product_id>#<yyyymmdd>            e.g. prod_00123#20261015
   family m : views, cart_adds, purchases  (counts)
   -> a time-range scan for one product is one contiguous read.
"""
import json, os, sys, glob
from datetime import datetime, timezone
from collections import defaultdict
import happybase

HOST = os.getenv("HBASE_HOST", "localhost")
PORT = int(os.getenv("HBASE_PORT", 9090))
DATA_DIR = sys.argv[1] if len(sys.argv) > 1 else "."
LIMIT = int(sys.argv[2]) if len(sys.argv) > 2 else 100000
EVERY = int(sys.argv[3]) if len(sys.argv) > 3 else 1   # 2 = take every 2nd session (systematic sample)
LONG_MAX = 9223372036854775807


def epoch_ms(iso):
    """ISO timestamp (naive) -> milliseconds since epoch, treated as UTC."""
    return int(datetime.fromisoformat(iso).replace(tzinfo=timezone.utc).timestamp() * 1000)


def user_session_key(user_id, start_iso):
    return f"{user_id}#{LONG_MAX - epoch_ms(start_iso):019d}".encode()


def b(value):
    return ("" if value is None else str(value)).encode()


def session_to_row(s):
    row = {
        b"info:session_id": b(s["session_id"]),
        b"info:start_time": b(s["start_time"]),
        b"info:end_time": b(s["end_time"]),
        b"info:duration": b(s["duration_seconds"]),
        b"info:device_type": b(s["device_profile"]["type"]),
        b"info:os": b(s["device_profile"]["os"]),
        b"info:browser": b(s["device_profile"]["browser"]),
        b"info:city": b(s["geo_data"]["city"]),
        b"info:country": b(s["geo_data"]["country"]),
        b"info:referrer": b(s["referrer"]),
        b"info:status": b(s["conversion_status"]),
    }
    for i, pv in enumerate(s["page_views"]):
        value = "|".join(str(x) if x is not None else "" for x in
                         (pv["page_type"], pv["product_id"], pv["category_id"],
                          pv["view_duration"], pv["timestamp"]))
        row[f"pv:{i:03d}".encode()] = value.encode()
    for pid, item in s["cart_contents"].items():
        row[f"cart:{pid}".encode()] = f"{item['quantity']}|{item['price']}".encode()
    return row


def main():
    conn = happybase.Connection(HOST, PORT, timeout=300000)   # 5 min socket timeout
    existing = [t.decode() for t in conn.tables()]
    for name, families in (
        ("user_sessions", {"info": dict(max_versions=1), "pv": dict(max_versions=1), "cart": dict(max_versions=1)}),
        ("product_daily_metrics", {"m": dict(max_versions=1)}),
    ):
        if name in existing:
            conn.delete_table(name, disable=True)   # re-runnable: start clean
        conn.create_table(name, families)
    print("tables created: user_sessions, product_daily_metrics")

    sessions_table = conn.table("user_sessions")
    views = defaultdict(int); adds = defaultdict(int); buys = defaultdict(int)

    loaded = 0
    seen = 0                      # position across ALL files, used for systematic sampling
    files = sorted(glob.glob(os.path.join(DATA_DIR, "sessions_*.json")))
    for path in files:
        if loaded >= LIMIT:
            break
        with open(path, encoding="utf-8") as f:
            sessions = json.load(f)
        with sessions_table.batch(batch_size=500) as batch:
            for s in sessions:
                if loaded >= LIMIT:
                    break
                seen += 1
                if EVERY > 1 and (seen % EVERY) != 0:
                    continue          # skip: keeps a sample spread over the WHOLE dataset
                batch.put(user_session_key(s["user_id"], s["start_time"]), session_to_row(s))
                day = s["start_time"][:10].replace("-", "")
                for pv in s["page_views"]:
                    if pv["page_type"] == "product_detail" and pv["product_id"]:
                        views[(pv["product_id"], day)] += 1
                for pid in s["cart_contents"]:
                    adds[(pid, day)] += 1
                    if s["conversion_status"] == "converted":
                        buys[(pid, day)] += 1
                loaded += 1
        print(f"{path}: total sessions loaded so far = {loaded}")

    metrics = conn.table("product_daily_metrics")
    keys = set(views) | set(adds)
    with metrics.batch(batch_size=500) as batch:
        for (pid, day) in keys:
            batch.put(f"{pid}#{day}".encode(), {
                b"m:views": b(views.get((pid, day), 0)),
                b"m:cart_adds": b(adds.get((pid, day), 0)),
                b"m:purchases": b(buys.get((pid, day), 0))})
    print(f"user_sessions rows: {loaded} | product_daily_metrics rows: {len(keys)}  DONE.")


if __name__ == "__main__":
    main()
