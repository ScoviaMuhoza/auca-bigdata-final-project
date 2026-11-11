"""
build_report.py - builds the Technical Report PDF (A4) with reportlab.

Run from the project root, AFTER make_charts.py:
    pip install reportlab
    python report\\build_report.py
Output: output\\Technical_Report.pdf
The five PNG charts are read from output\\charts\\ (a grey placeholder box is drawn for any missing chart).
Edit the STUDENT_* and GITHUB_URL lines below, and read every paragraph so you can explain it.
"""
import os
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.utils import ImageReader
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
                                Image, KeepTogether)
from reportlab.graphics.shapes import Drawing, Rect, String, Line, Polygon

# ------------------------------------------------------------------ EDIT THESE
STUDENT_NAME = "Your Full Name"
STUDENT_ID = "Your Student ID"
COURSE = "Big Data Analytics - AUCA"
DATE = "11 November 2026"
GITHUB_URL = "https://github.com/YOUR-USERNAME/YOUR-REPO"
# ------------------------------------------------------------------------------
CHART_DIR = os.getenv("CHART_DIR", os.path.join("output", "charts"))
OUT_FILE = os.getenv("REPORT_OUT", os.path.join("output", "Technical_Report.pdf"))
os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)

NAVY, BLUE, LIGHT, GREY = colors.HexColor("#1F3A5F"), colors.HexColor("#2F6DB5"), colors.HexColor("#EAF1FA"), colors.HexColor("#6B7280")
BASE = ParagraphStyle("base", fontName="Helvetica", fontSize=9.2, leading=12.4, alignment=TA_JUSTIFY, spaceAfter=4)
H1 = ParagraphStyle("h1", fontName="Helvetica-Bold", fontSize=13, leading=16, textColor=NAVY, spaceBefore=9, spaceAfter=4, keepWithNext=1)
H2 = ParagraphStyle("h2", fontName="Helvetica-Bold", fontSize=10.2, leading=13, textColor=BLUE, spaceBefore=5, spaceAfter=2, keepWithNext=1)
BUL = ParagraphStyle("bul", parent=BASE, leftIndent=12, bulletIndent=2, spaceAfter=1.5)
CAP = ParagraphStyle("cap", fontName="Helvetica-Oblique", fontSize=8, leading=10, textColor=GREY, alignment=TA_CENTER, spaceAfter=6)
CELL = ParagraphStyle("cell", fontName="Helvetica", fontSize=8, leading=10)
CELLB = ParagraphStyle("cellb", parent=CELL, fontName="Helvetica-Bold", textColor=colors.white)
TITLE = ParagraphStyle("title", fontName="Helvetica-Bold", fontSize=19, leading=23, textColor=NAVY, alignment=TA_CENTER, spaceAfter=3)
SUB = ParagraphStyle("sub", fontName="Helvetica", fontSize=10.5, leading=14, textColor=GREY, alignment=TA_CENTER, spaceAfter=2)


def P(t): return Paragraph(t, BASE)
def B(t): return Paragraph(t, BUL, bulletText="\u2022")
def hdr(t): return Paragraph(t, H1)
def sub(t): return Paragraph(t, H2)


def table(rows, widths, zebra=True):
    data = [[Paragraph(str(c), CELLB if i == 0 else CELL) for c in r] for i, r in enumerate(rows)]
    t = Table(data, colWidths=[w * cm for w in widths], repeatRows=1)
    st = [("BACKGROUND", (0, 0), (-1, 0), NAVY), ("VALIGN", (0, 0), (-1, -1), "TOP"),
          ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#C9D3E0")),
          ("TOPPADDING", (0, 0), (-1, -1), 2.5), ("BOTTOMPADDING", (0, 0), (-1, -1), 2.5),
          ("LEFTPADDING", (0, 0), (-1, -1), 4), ("RIGHTPADDING", (0, 0), (-1, -1), 4)]
    if zebra:
        for i in range(2, len(rows), 2):
            st.append(("BACKGROUND", (0, i), (-1, i), LIGHT))
    t.setStyle(TableStyle(st))
    t.hAlign = "CENTER"
    return t


def figure(fname, caption, width_cm=13.5):
    path = os.path.join(CHART_DIR, fname)
    if os.path.exists(path):
        iw, ih = ImageReader(path).getSize()
        img = Image(path, width=width_cm * cm, height=width_cm * cm * ih / iw)
    else:
        img = Table([[Paragraph(f"[ chart missing: {fname} - run viz/make_charts.py ]", CELL)]],
                    colWidths=[width_cm * cm], rowHeights=[4.2 * cm])
        img.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#F1F3F6")),
                                 ("BOX", (0, 0), (-1, -1), 0.5, GREY), ("VALIGN", (0, 0), (-1, -1), "MIDDLE")]))
    img.hAlign = "CENTER"
    return KeepTogether([img, Paragraph(caption, CAP)])


def architecture_diagram():
    w, h = 17 * cm, 5.0 * cm
    d = Drawing(w, h)

    def box(x, y, bw, bh, title, lines, fill):
        d.add(Rect(x, y, bw, bh, rx=5, ry=5, fillColor=fill, strokeColor=NAVY, strokeWidth=0.8))
        d.add(String(x + bw / 2, y + bh - 12, title, fontName="Helvetica-Bold", fontSize=8.6, textAnchor="middle", fillColor=NAVY))
        for i, ln in enumerate(lines):
            d.add(String(x + bw / 2, y + bh - 24 - i * 9.5, ln, fontName="Helvetica", fontSize=7.2, textAnchor="middle"))

    def arrow(x1, y1, x2, y2):
        d.add(Line(x1, y1, x2 - 5, y2, strokeColor=GREY, strokeWidth=1.2))
        d.add(Polygon([x2, y2, x2 - 6, y2 + 3, x2 - 6, y2 - 3], fillColor=GREY, strokeColor=GREY))

    box(0, 55, 88, 70, "Raw JSON files", ["users, products,", "categories,", "transactions,", "sessions (200k)"], colors.HexColor("#FFF4D6"))
    box(135, 92, 130, 58, "MongoDB (document)", ["products, users,", "transactions (items embedded)"], LIGHT)
    box(135, 8, 130, 58, "HBase (wide-column)", ["user_sessions,", "product_daily_metrics"], colors.HexColor("#E3F4EA"))
    box(310, 50, 85, 80, "Apache Spark", ["clean, normalise", "recommend, cohort", "Spark SQL, joins"], colors.HexColor("#FDE8E0"))
    box(425, 50, 52, 80, "Outputs", ["CSV tables", "PNG charts", "PDF report"], colors.HexColor("#F1F3F6"))
    arrow(88, 105, 135, 120); arrow(88, 75, 135, 40)
    arrow(265, 121, 310, 105); arrow(265, 37, 310, 75); arrow(395, 90, 425, 90)
    return d


def footer(canvas, doc):
    canvas.saveState()
    canvas.setFont("Helvetica", 7.5); canvas.setFillColor(GREY)
    canvas.drawString(2 * cm, 1.1 * cm, "Distributed Multi-Model Analytics for E-commerce Data")
    canvas.drawRightString(A4[0] - 2 * cm, 1.1 * cm, f"Page {doc.page}")
    canvas.restoreState()


story = []

# ============================================================ TITLE + 1. INTRODUCTION
story += [Spacer(1, 4), Paragraph("Distributed Multi-Model Analytics for E-commerce Data", TITLE),
          Paragraph("Technical Report - MongoDB, HBase and Apache Spark", SUB),
          Paragraph(f"{STUDENT_NAME} | {STUDENT_ID} | {COURSE} | {DATE}", SUB),
          Paragraph(f"Code repository: {GITHUB_URL}", SUB), Spacer(1, 6)]

story += [hdr("1. Introduction and objectives"),
          P("This project builds a multi-model analytics system for a synthetic e-commerce platform. Each technology is used for the "
            "workload it handles best: <b>MongoDB</b> stores transactional and catalogue data as documents, <b>HBase</b> stores the "
            "high-volume browsing time series, and <b>Apache Spark</b> cleans the data, computes analytics and joins the two stores. "
            "The questions answered are: which products and categories earn the revenue, which customers matter most, which products "
            "are bought or viewed together, and where shoppers leave the purchase funnel."),
          table([["Entity", "Records", "Notes"],
                 ["Users", "10,000", "profile, geography, registration and last-active dates (no age field in the generator)"],
                 ["Products / categories", "5,000 / 25", "price history embedded; products carry a category but no subcategory"],
                 ["Sessions", "200,000", "page views, cart contents, device, referrer; about 90 days of activity"],
                 ["Transactions", "50,000", "embedded line items; 24,906 (49.8%) have no linked session"]],
                [3.6, 2.4, 11.0])]

# ============================================================ 2. ARCHITECTURE
story += [hdr("2. System architecture"), architecture_diagram(), Spacer(1, 3),
          P("The JSON files are loaded into two stores according to their access pattern. Transactions, users and products go to "
            "MongoDB because they are nested documents queried with flexible aggregations. Sessions go to HBase because they are "
            "an append-only time series that is read by user and by time range. Spark reads the raw files for large batch jobs and "
            "reads from both databases (through Python extraction) for the integrated query. Spark writes small result tables as CSV; "
            "Python then draws the charts for this report. MongoDB and HBase run in Docker containers started by one "
            "<font face='Courier'>docker compose up</font> command, so the whole project is reproducible.")]

# ============================================================ 3. DATA MODELLING
story += [hdr("3. Data modelling (Part 1)"), sub("3.1 MongoDB document model")]
story += [table([["Collection", "Design decision", "Reason"],
                 ["transactions", "Line items <b>embedded</b> as an array; <font face='Courier'>category_id</font> copied into every item; timestamps stored as real dates",
                  "A transaction is always read with its items (one read, no join). Copying the category makes revenue-by-category a single pipeline with no lookup."],
                 ["products", "Price history embedded; category name copied into the product", "Always needed together with the product; avoids a join in every product query."],
                 ["users", "Profile plus geography embedded, and a <b>pre-computed purchase_summary</b> (orders, total spent, first and last purchase)",
                  "Segmentation reads one small document per user instead of scanning 50,000 transactions."],
                 ["categories", "Separate reference collection with subcategories embedded", "Small and rarely changing; referenced by <font face='Courier'>$lookup</font> only for display names."]],
                [2.4, 7.0, 7.6]),
          P("<b>Trade-off.</b> Embedding and copying make reads fast but duplicate data: if a category were renamed it would have to be updated in several "
            "places. This is acceptable for an analytics dataset that is loaded in bulk. Indexes were created on <i>transactions.timestamp</i>, "
            "<i>transactions.user_id</i>, <i>transactions.items.product_id</i>, <i>products.category_id</i> and the user order count, matching the query patterns. "
            "Sessions were deliberately <b>not</b> stored in MongoDB: at 200,000 documents with long page-view arrays they are a write-heavy time series."),
          P("<b>Aggregation pipelines.</b> (1) <i>Revenue by month and category</i>: <font face='Courier'>$unwind</font> on items, <font face='Courier'>$group</font> by category and "
            "month (<font face='Courier'>$dateToString</font>), <font face='Courier'>$lookup</font> for the category name. (2) <i>User segmentation by purchase frequency</i>: "
            "<font face='Courier'>$bucket</font> on the order count with average and total spend per band. (3) <i>Top-selling products</i>: unwind, group by product, sort, lookup. "
            "Age-group segmentation was not possible because the generator creates no age, so purchase frequency was used instead.")]

story += [sub("3.2 HBase wide-column model")]
story += [table([["Table", "Row key", "Column families", "Why"],
                 ["user_sessions", "<font face='Courier'>user_id#reverse_timestamp</font> (Long.MAX minus start time in ms, zero-padded to 19 digits)",
                  "<b>info</b>: session metadata; <b>pv</b>: one column per page view (pv:000, pv:001, ...); <b>cart</b>: one column per product",
                  "All sessions of a user are adjacent, so one prefix scan returns them; HBase sorts keys ascending, so the <b>newest session comes first</b>. Sessions have different numbers of events, so rows are sparse and only existing cells are stored."],
                 ["product_daily_metrics", "<font face='Courier'>product_id#yyyymmdd</font>", "<b>m</b>: views, cart_adds, purchases",
                  "A product's history over a date range is one contiguous scan; counts are pre-aggregated per day."]],
                [2.6, 4.2, 4.8, 5.4]),
          P("Separate column families keep data that is read together physically together: a query that only needs device and referrer reads the <i>info</i> family and never touches the "
            "page-view cells. Implemented queries: latest N sessions of a user (prefix scan with a limit), sessions of a user inside a time range (start/stop rows computed from the reversed "
            "timestamps), the full journey of the latest session, and view-to-cart and cart-to-purchase rates of a product over a date range. The HBase shell commands for the schema and queries "
            "are in the repository. The loaded subset holds 100,000 sessions (the first generated file) and 236,715 product-day rows.")]
story += [sub("3.3 Why some data is in MongoDB and some in HBase"),
          table([["Criterion", "MongoDB", "HBase"],
                 ["Data shape", "Nested business documents (transactions with items, users, catalogue)", "Event streams: sessions and page views, sparse and variable length"],
                 ["Main access pattern", "Filters, grouping and joins across many fields (aggregation framework)", "Lookup by key and range scans over time (user and day)"],
                 ["Growth", "Moderate (50,000 transactions)", "Very high and append-only (200,000 sessions, millions of events)"],
                 ["Weak point", "Large write-heavy time series", "No flexible ad-hoc queries; the row key must match the query"]],
                [3.0, 7.0, 7.0])]

# ============================================================ 4. SPARK
story += [hdr("4. Data processing with Apache Spark (Part 2)"),
          P("All Spark jobs share one cleaning module (<i>common.py</i>), so every analysis starts from the same cleaned DataFrames. Explicit schemas are used instead of schema inference; "
            "the files are single JSON arrays, so they are read with the multi-line option, and the cart contents are modelled as a <i>map</i> type because every cart has different product keys."),
          sub("4.1 Cleaning and normalisation (batch job 1)"),
          B("Removed duplicates and null keys, cast text dates to timestamps, trimmed and lower-cased status, payment and referrer fields, filled missing geography and device values with explicit \"unknown\" labels."),
          B("Added quality flags and a data-quality report. Findings: <b>24,906 transactions without a session</b> (flag <i>is_orphan</i>); <b>15,515 sessions labelled \"converted\" but with no transaction</b> "
            "(38% of all converted labels); <b>163 products active with zero stock</b>; no duplicates, bad durations, amount mismatches or inconsistent dates."),
          B("Cause of the false labels (confirmed by a per-file check): the first 100,000 sessions are fully consistent (20,239 converted, all with a transaction); in the second file only 4,855 of 20,370 converted sessions "
            "have a transaction, with the first mismatch around session 124,000. The generator stops creating transactions at its 50,000 limit but keeps labelling sessions as converted. "
            "The cleaning job therefore derives <b>verified_conversion</b> by joining sessions with transactions and uses it instead of the label."),
          sub("4.2 Recommendation indicators (batch job 2)"),
          P("For two items A and B in a basket (a transaction or a session) the job computes <b>support</b> (share of baskets with both), <b>confidence</b> A to B (baskets with both divided by baskets with A, i.e. \"users who bought A also bought B\") "
            "and <b>lift</b> (support divided by the product of the individual supports; above 1 means they appear together more than chance). It is implemented as a self-join inside each basket with the condition "
            "A less than B, followed by a join to the item counts. It was run for products bought together (50,000 baskets), products viewed together (197,422 sessions) and category affinity."),
          sub("4.3 Cohort analysis (batch job 3) and Spark SQL"),
          P("Users are grouped into cohorts by registration month. For each cohort and each month offset (whole months between registration and purchase) the job computes revenue, active buyers, "
            "spend per cohort user and buyer rate. Spark SQL queries on views named after the system they represent (<i>mongo_users</i>, <i>mongo_transactions</i>, <i>hbase_sessions</i>) cover: top three categories per month "
            "(window function <i>ROW_NUMBER</i>); conversion by device and referrer; customer value quartiles joining users, transactions and sessions (<i>NTILE</i>, CTEs); and a page-level funnel using the "
            "higher-order function <i>exists</i> on the page-view array."),
          sub("4.4 Optimisation choices"),
          B("Small dimension tables (categories, products, transaction session ids) are <b>broadcast</b>; the physical plan of a test query shows <i>BroadcastHashJoin</i>."),
          B("Sessions are cached once (memory and disk) because they are reused many times; shuffle partitions are reduced from the default 200 to 16 for a single machine."),
          B("Rare pairs are filtered before the second join to reduce shuffled data; analysis scripts skip the data-quality counting jobs; small results are saved with pandas to avoid Hadoop write problems on Windows.")]

# ============================================================ 5. INTEGRATION
story += [hdr("5. Integrated analytics workflow (Part 3)"),
          table([["Item", "Description"],
                 ["Business question", "Where do shoppers leave between browsing and paying, and do device, week or product category change the conversion, using only <b>verified</b> purchases?"],
                 ["Sources", "<b>HBase</b> user_sessions (page views, cart, device, referrer); <b>MongoDB</b> transactions (what was really bought) and products (category); <b>Spark</b> for the join and aggregation."],
                 ["Steps", "1) scan HBase for the three column families needed; 2) query MongoDB for only the needed fields; 3) load both into Spark DataFrames with explicit schemas; "
                           "4) left join sessions with transactions on session_id; 5) aggregate the funnel overall and by device, week and category (view, cart, purchase per category)."],
                 ["Why these technologies", "HBase serves the large session time series efficiently; MongoDB is the source of truth for purchases; Spark performs the cross-system join that neither database can do alone."],
                 ["Performance notes", "The HBase scan reads only the needed families in batches of 500 rows; the MongoDB query projects only the needed fields; the small transaction table is broadcast. "
                                       "Extraction through one Python process is the bottleneck; at scale the HBase-Spark and MongoDB Spark connectors would load the data in parallel."]],
                [3.4, 13.6]),
          P("On the 100,000 HBase sessions the integrated funnel reached 20.2% verified purchases, with 20.2-20.3% on every device and 19.7-21.2% in every full week; cart-to-purchase by category ranged from 29.4% to 32.3%. "
            "The sample is the first generated file, which contains no false labels, so its rate is higher than the full-dataset 12.5% computed by Spark; this gap is explained in Section 4.1 and in the limitations.")]

# ============================================================ 6. RESULTS
story += [hdr("6. Results, visualisations and business insights (Part 4)"),
          figure("1_conversion_funnel.png",
                 "Figure 1. Conversion funnel for all 200,000 sessions, counting only purchases verified by a MongoDB transaction.", 13.0),
          P("<b>Funnel.</b> 98.7% of sessions view a product, 65.1% fill a cart (66% of viewers), 43.5% reach checkout (67% of carts) and only <b>12.5%</b> buy (28.8% of checkout sessions). "
            "The raw label would suggest 20.3%, an overstatement of 38%. The weakest step is checkout to purchase: about 62,000 sessions reached checkout and did not buy."),
          figure("2_weekly_revenue.png",
                 "Figure 2. Weekly revenue from the MongoDB transactions (grey bars are partial first and last weeks: the data starts in early August and ends on 3 November).", 13.0),
          P("<b>Sales over time.</b> Weekly revenue is steady with no strong trend, as expected for randomly generated data; the partial weeks must not be compared with full ones. "
            "By category, <i>Kelly-Santiago</i> is the top earner in August, September and October (about 756k, 883k and 937k), but the next categories are close behind, so the ranking is not decisive."),
          figure("3_customer_segments.png",
                 "Figure 3. Customers by number of orders (bars) and average spend per user (line), computed from the cleaned transactions.", 12.0),
          KeepTogether([table([["Orders per user", "Users", "Average spend", "Share of users", "Share of revenue"],
                 ["0", "58", "0", "0.6%", "0%"], ["1-2", "1,176", "1,804", "11.8%", "3.9%"], ["3-4", "3,155", "3,840", "31.6%", "22.5%"],
                 ["5-7", "4,326", "6,298", "43.3%", "50.5%"], ["8-11", "1,226", "9,481", "12.3%", "21.6%"], ["12+", "59", "13,916", "0.6%", "1.5%"]],
                [3.2, 2.4, 3.2, 3.2, 3.6])]),
          Spacer(1, 3),
          P("<b>Segmentation.</b> Spend per user rises from about 1.8k to 13.9k as orders increase, while the average order value stays near 1,080 (total revenue of about 53.9 million over 50,000 orders). "
            "Revenue is therefore driven by <b>how often</b> customers buy, not by basket size. The 5-7 order segment is 43% of users and 50% of revenue. The value-quartile query agrees: the top quartile "
            "orders 7.5 times and spends 9,253 per user against 2.7 orders and 2,034 in the bottom quartile, while sessions per user (19-21) and session length (about 1,810 seconds) barely differ."),
          figure("4_product_performance.png",
                 "Figure 4. Top 10 products by units sold (left) and by revenue (right).", 16.0),
          P("<b>Products.</b> Ranking by units and by revenue gives different lists: the best-seller by units (prod_01751, 102 units) earned 33,779, whereas prod_04285 sold 87 units but earned only 1,786 because it is cheap. "
            "Category affinity lift is 0.80-0.85 and product-pair lifts are inflated by counts of 2-5 pairs, so the data contains <b>no real affinity</b>; the method is valid but cannot recommend from random data."),
          figure("5_cohort_heatmap.png",
                 "Figure 5. Spend per cohort user by registration month (rows) and months since registration (columns); the last observed month of each row is partial.", 12.0),
          P("<b>Cohorts.</b> Cohorts are Feb-Aug 2026 (1,241 to 1,745 users each; August only 280 because registrations end in August). In their first full months 76-85% of each cohort buys and spend per user is about 1,490-1,910; "
            "the drop to 13-18% in the last column is an artefact of the incomplete final month, not churn."),
          sub("Actionable insights"),
          table([["Finding", "Recommended action"],
                 ["Only 28.8% of checkout sessions end in a purchase (about 62,000 lost)", "Simplify checkout, add payment options, and send recovery emails to abandoned checkouts; measure this step first."],
                 ["Frequency drives revenue; 5-7 order customers give half of revenue", "Loyalty or reminder campaigns that move 3-4 order customers to 5-7 orders; reactivate the 1,176 customers with only 1-2 orders."],
                 ["Units and revenue rankings differ", "Rank and promote products by revenue or margin, not units; review cheap high-volume items."],
                 ["The converted label overstates conversion by 38%; half of the orders have no session", "Fix event tracking and order-to-session attribution before using the label for decisions."],
                 ["Device, referrer, week and category conversions differ by only 1-3 points", "Do not shift budget by device or channel on this evidence; test with controlled experiments."],
                 ["163 active products have no stock", "Deactivate or restock them so shoppers do not reach unavailable items."]],
                [7.2, 9.8])]

# ============================================================ 7. METHODOLOGY
story += [hdr("7. Methodology of the key analyses"),
          B("<b>Verified conversion:</b> a session is converted only if a transaction with its session_id exists. Funnel stages are counted per session from the page-view and cart data."),
          B("<b>Revenue:</b> gross item revenue (before the transaction-level discount) for category and product results; transaction totals (after discount) for user spend and segments."),
          B("<b>Segments:</b> users are bucketed by order count into 0, 1-2, 3-4, 5-7, 8-11 and 12+; the bands were chosen to give groups large enough to compare (total users 10,000)."),
          B("<b>Lift:</b> support(A and B) divided by support(A) times support(B); values are only meaningful when pair counts are large, so category-level lift is the reliable view."),
          B("<b>Cohort offset:</b> whole months between the registration month and the purchase month; partial final months are flagged, not removed.")]

# ============================================================ 8-9. SCALABILITY + TECH CHOICE
story += [hdr("8. Scalability considerations"),
          B("<b>Spark:</b> the job code is cluster-ready (DataFrames, no driver-side loops). The generator's JSON files are single arrays, which Spark cannot split, so each file is read by one task; at scale the data should first be converted to JSON Lines or Parquet "
            "and partitioned by date. The full 2,000,000-session size would need a cluster or Parquet input."),
          B("<b>MongoDB:</b> use a replica set for availability and shard transactions by a hashed user_id (or by time) to spread writes; keep the embedded design so a transaction stays on one shard."),
          B("<b>HBase:</b> regions split automatically as data grows. Sequential user ids can create hotspots, so a hash or salt prefix on the row key would be added at very large scale while keeping the reverse timestamp for recency."),
          B("<b>Integration:</b> replace the Python extraction with the HBase-Spark and MongoDB Spark connectors so that reading is parallel and filters are pushed down to the databases.")]

story += [hdr("9. Technology selection justification"),
          table([["Analytical task", "Technology", "Justification"],
                 ["Revenue, product popularity, user segments", "MongoDB aggregation", "Nested transactions, grouping, bucketing and lookups in one pipeline over documents"],
                 ["Latest sessions of a user, time-range scans, product-day metrics", "HBase", "Key-ordered storage: reverse-timestamp row keys give recent-first reads without sorting"],
                 ["Cleaning, recommendations, cohorts, SQL over all sessions", "Spark", "Distributed joins and aggregations over data larger than one process, with SQL and DataFrame APIs"],
                 ["Funnel across behaviour and purchases", "HBase + MongoDB + Spark", "Behaviour and transactions live in different systems; only Spark can join them"]],
                [5.6, 3.4, 8.0])]

# ============================================================ 10. LIMITATIONS
story += [hdr("10. Limitations and future work"),
          B("<b>Synthetic, random data:</b> differences between categories, devices and weeks are noise, and lift values show no real affinity. The analyses demonstrate the methods, not real market findings."),
          B("<b>Generator inconsistencies:</b> half the transactions are not linked to a session, 15,515 converted labels are false, and there is no user age or product subcategory; these were flagged rather than repaired."),
          B("<b>Short history:</b> only about 90 days, so cohorts are short and the last month is partial. No statistical tests were run; with about 4,000 cart sessions per category a 2-3 point difference can be chance."),
          B("<b>HBase subset:</b> a single-laptop Docker HBase crashed under the full load, so 100,000 sessions (the first generated file) were loaded. Its funnel (20.2%) is not representative of the full data (12.5%). "
            "A systematic sample over both files was added to the loader as a fix; the full-dataset funnel was computed in Spark."),
          B("<b>Future work:</b> connector-based integration, Parquet storage, a collaborative-filtering recommender (ALS) on real data, customer lifetime value modelling, a streaming simulation with Spark Structured Streaming, and significance testing of conversion differences.")]

doc = SimpleDocTemplate(OUT_FILE, pagesize=A4, leftMargin=2 * cm, rightMargin=2 * cm, topMargin=1.8 * cm, bottomMargin=1.8 * cm,
                        title="Distributed Multi-Model Analytics for E-commerce Data", author=STUDENT_NAME)
doc.build(story, onFirstPage=footer, onLaterPages=footer)
print("saved", OUT_FILE)
