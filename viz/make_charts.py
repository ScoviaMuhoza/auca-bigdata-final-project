"""
make_charts.py - PART 4: builds the report visualizations from the CSV files that the
Spark and integration jobs saved in the output folder.

Run from the project root (after steps 01-04 and the integration script):
    python viz\\make_charts.py
Charts are saved as PNG files in output\\charts\\
"""
import os
import pandas as pd
import matplotlib
matplotlib.use("Agg")            # draw to files, no window needed
import matplotlib.pyplot as plt

OUT = "output"
CH = os.path.join(OUT, "charts")
os.makedirs(CH, exist_ok=True)
plt.rcParams.update({"figure.dpi": 130, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.titlesize": 13, "axes.titleweight": "bold", "font.size": 10})
BLUE, ORANGE, GREY, GREEN = "#2F6DB5", "#E07B39", "#B8BEC6", "#3C9D6E"


def read(*parts):
    return pd.read_csv(os.path.join(OUT, *parts))


def save(fig, name):
    path = os.path.join(CH, name)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    print("saved", path)


# ---------------------------------------------------------------- 1. conversion funnel
def chart_funnel():
    """Preferred source: the Spark SQL funnel on ALL 200,000 sessions (verified purchases).
    Fallback: the HBase+MongoDB integration funnel (a sample of the sessions)."""
    if os.path.exists(os.path.join(OUT, "sql_funnel.csv")):
        f = read("sql_funnel.csv").rename(columns={"pct_of_all_sessions": "pct_of_all"})
        title = "Conversion funnel, all 200,000 sessions (verified purchases only)"
    else:
        f = read("integration_funnel.csv")
        f["stage"] = f["stage"].str.replace(r"^\d\s", "", regex=True).str.replace(" (MongoDB)", "", regex=False)
        title = "Conversion funnel: HBase sessions joined with MongoDB transactions (sample)"
    f["pct_of_previous_stage"] = (100 * f["sessions"] / f["sessions"].shift(1)).round(0)
    fig, ax = plt.subplots(figsize=(8, 4))
    y = list(range(len(f)))[::-1]
    ax.barh(y, f["sessions"], color=BLUE)
    for yi, (n, p, prev) in zip(y, zip(f["sessions"], f["pct_of_all"], f["pct_of_previous_stage"])):
        extra = "" if pd.isna(prev) else f"  ({prev:.0f}% of previous stage)"
        ax.text(n, yi, f"  {n:,}  |  {p:.1f}%{extra}", va="center", fontsize=9)
    ax.set_yticks(y); ax.set_yticklabels(f["stage"])
    ax.set_xlim(0, f["sessions"].max() * 1.65); ax.set_xlabel("Sessions")
    ax.set_title(title, fontsize=11)
    save(fig, "1_conversion_funnel.png")


# ---------------------------------------------------------------- 2. sales over time
def chart_sales_over_time():
    t = read("clean", "transactions.csv", ).copy()
    t["week"] = pd.to_datetime(t["timestamp"]).dt.to_period("W").dt.start_time
    w = t.groupby("week")["total"].sum().reset_index()
    colors = [BLUE] * len(w); colors[0] = GREY; colors[-1] = GREY      # partial weeks
    fig, ax = plt.subplots(figsize=(9, 4))
    ax.bar(w["week"].dt.strftime("%d %b"), w["total"] / 1000, color=colors)
    ax.set_ylabel("Revenue (thousands)"); ax.set_xlabel("Week starting")
    ax.set_title("Weekly revenue (grey bars = partial weeks)")
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    save(fig, "2_weekly_revenue.png")


# ---------------------------------------------------------------- 3. segmentation
def chart_segmentation():
    t = read("clean", "transactions.csv")
    u = read("clean", "users.csv")[["user_id"]]
    per = t.groupby("user_id").agg(orders=("transaction_id", "count"), spent=("total", "sum")).reset_index()
    per = u.merge(per, on="user_id", how="left").fillna({"orders": 0, "spent": 0})
    bins = [-1, 0, 2, 4, 7, 11, 10_000]
    labels = ["0", "1-2", "3-4", "5-7", "8-11", "12+"]
    per["segment"] = pd.cut(per["orders"], bins=bins, labels=labels)
    g = per.groupby("segment", observed=False).agg(users=("user_id", "count"), avg_spent=("spent", "mean")).reset_index()
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(g["segment"], g["users"], color=BLUE)
    ax.set_xlabel("Orders per user"); ax.set_ylabel("Number of users", color=BLUE)
    ax2 = ax.twinx(); ax2.spines["right"].set_visible(True)
    ax2.plot(g["segment"], g["avg_spent"], color=ORANGE, marker="o", linewidth=2)
    ax2.set_ylabel("Average spend per user", color=ORANGE)
    ax.set_title("Customer segments by purchase frequency")
    save(fig, "3_customer_segments.png")


# ---------------------------------------------------------------- 4. product performance
def chart_products():
    it = read("clean", "transaction_items.csv")
    names = read("clean", "products.csv")[["product_id", "name"]]
    g = it.groupby("product_id").agg(units=("quantity", "sum"), revenue=("subtotal", "sum")).reset_index().merge(names, on="product_id")
    g["label"] = g["product_id"].str.replace("prod_", "#") + " " + g["name"].str.slice(0, 22)
    top_u = g.nlargest(10, "units").iloc[::-1]
    top_r = g.nlargest(10, "revenue").iloc[::-1]
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.6))
    axes[0].barh(top_u["label"], top_u["units"], color=BLUE); axes[0].set_title("Top 10 by units sold")
    axes[1].barh(top_r["label"], top_r["revenue"] / 1000, color=GREEN); axes[1].set_title("Top 10 by revenue (thousands)")
    for ax in axes: ax.tick_params(axis="y", labelsize=8)
    save(fig, "4_product_performance.png")


# ---------------------------------------------------------------- 5. cohort heatmap
def chart_cohort():
    c = read("cohort_analysis.csv")
    p = c.pivot(index="cohort", columns="month_offset", values="spend_per_cohort_user").sort_index()
    fig, ax = plt.subplots(figsize=(8, 4))
    im = ax.imshow(p.values, cmap="Blues", aspect="auto")
    ax.set_xticks(range(len(p.columns))); ax.set_xticklabels(p.columns)
    ax.set_yticks(range(len(p.index))); ax.set_yticklabels(p.index)
    for i in range(p.shape[0]):
        for j in range(p.shape[1]):
            v = p.values[i, j]
            if pd.notna(v):
                ax.text(j, i, f"{v:,.0f}", ha="center", va="center", fontsize=8,
                        color="white" if v > p.values[~pd.isna(p.values)].max() * 0.6 else "black")
    ax.set_xlabel("Months since registration"); ax.set_ylabel("Registration cohort")
    ax.set_title("Spend per cohort user (last observed month is partial)")
    fig.colorbar(im, ax=ax, label="Spend per user")
    save(fig, "5_cohort_heatmap.png")


if __name__ == "__main__":
    for fn in (chart_funnel, chart_sales_over_time, chart_segmentation, chart_products, chart_cohort):
        try:
            fn()
        except FileNotFoundError as e:
            print(f"SKIPPED {fn.__name__}: missing file {e.filename} (run the earlier step first)")
