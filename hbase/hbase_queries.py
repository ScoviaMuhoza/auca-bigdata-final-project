"""
hbase_queries.py - sample HBase queries (through happybase).
Run from the project root:   python hbase\\hbase_queries.py [user_id]
"""
import os, sys
from datetime import datetime, timezone
import happybase

LONG_MAX = 9223372036854775807
conn = happybase.Connection(os.getenv("HBASE_HOST", "localhost"), int(os.getenv("HBASE_PORT", 9090)))
sessions = conn.table("user_sessions")
metrics = conn.table("product_daily_metrics")


def rev(dt):
    ms = int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
    return LONG_MAX - ms


def decode(row):
    return {k.decode(): v.decode() for k, v in row.items()}


# Q1: the 5 MOST RECENT sessions of a user (newest first, thanks to the reverse timestamp)
def latest_sessions(user_id, n=5):
    out = []
    for key, data in sessions.scan(row_prefix=f"{user_id}#".encode(), limit=n,
                                   columns=[b"info"]):   # only the 'info' family is read
        d = decode(data)
        out.append((d["info:start_time"], d["info:device_type"], d["info:referrer"], d["info:status"]))
    return out


# Q2: all sessions of a user inside a TIME RANGE (range scan on the row key)
def sessions_in_range(user_id, start, end):
    row_start = f"{user_id}#{rev(end):019d}".encode()          # later time = smaller number
    row_stop = f"{user_id}#{rev(start) + 1:019d}".encode()
    return [decode(d)["info:session_id"] for _, d in
            sessions.scan(row_start=row_start, row_stop=row_stop, columns=[b"info:session_id"])]


# Q3: the complete journey (page views + cart) of the user's latest session
def latest_journey(user_id):
    for key, data in sessions.scan(row_prefix=f"{user_id}#".encode(), limit=1):
        d = decode(data)
        pages = [d[k] for k in sorted(d) if k.startswith("pv:")]
        cart = {k[5:]: d[k] for k in d if k.startswith("cart:")}
        return d["info:session_id"], pages, cart
    return None


# Q4: product performance over a date range -> view-to-cart and cart-to-purchase rates
def product_performance(product_id, day_from, day_to):
    v = a = p = 0
    for _, data in metrics.scan(row_start=f"{product_id}#{day_from}".encode(),
                                row_stop=f"{product_id}#{day_to}~".encode()):
        d = decode(data)
        v += int(d["m:views"]); a += int(d["m:cart_adds"]); p += int(d["m:purchases"])
    return {"views": v, "cart_adds": a, "purchases": p,
            "view_to_cart": round(a / v, 3) if v else None,
            "cart_to_purchase": round(p / a, 3) if a else None}


if __name__ == "__main__":
    user = sys.argv[1] if len(sys.argv) > 1 else "user_000042"
    print(f"\n--- Q1 latest 5 sessions of {user} ---")
    for r in latest_sessions(user): print(r)
    print(f"\n--- Q2 sessions of {user} between 2026-09-01 and 2026-10-01 ---")
    print(sessions_in_range(user, datetime(2026, 9, 1), datetime(2026, 10, 1)))
    print(f"\n--- Q3 journey of the latest session of {user} ---")
    j = latest_journey(user)
    if j:
        print("session:", j[0]); [print("  ", p) for p in j[1]]; print("cart:", j[2])
    first = next(metrics.scan(limit=1), None)
    if first:
        pid = first[0].decode().split("#")[0]
        print(f"\n--- Q4 performance of {pid}, 2026-08-01..2026-11-30 ---")
        print(product_performance(pid, "20260801", "20261130"))
