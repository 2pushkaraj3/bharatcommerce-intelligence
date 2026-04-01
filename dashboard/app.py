"""
Bharatcommerce Intelligence Platform
Local Dashboard — reads Parquet from data_lake/ directly.
No BigQuery, no GCP needed. Works the moment consumer starts writing.

Run:  streamlit run dashboard/app.py
URL:  http://localhost:8501

Auto-refreshes every 5 seconds to show live data.
"""

import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

# ── Config ────────────────────────────────────────────────────

DATA_LAKE   = Path(os.getenv("DATA_LAKE_PATH", "./data_lake"))
REFRESH_SEC = 5

st.set_page_config(
    page_title = "Bharatcommerce Intelligence",
    page_icon  = "🛒",
    layout     = "wide",
)

st.markdown("""
<style>
[data-testid="stMetric"]          { background:#f8fafc; border-radius:10px; padding:14px; }
[data-testid="stMetricLabel"]     { font-size:12px !important; }
.stTabs [data-baseweb="tab"]      { font-size:14px; }
div.stAlert                        { border-radius:8px; }
</style>
""", unsafe_allow_html=True)


# ── Data loading ──────────────────────────────────────────────

@st.cache_data(ttl=REFRESH_SEC)
def load_orders() -> pd.DataFrame:
    """Load all Parquet files from data_lake/orders/ into one DataFrame."""
    path = DATA_LAKE / "orders"
    if not path.exists():
        return pd.DataFrame()
    files = list(path.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True, errors="coerce")
    df["order_date"]      = df["event_timestamp"].dt.date
    return df


@st.cache_data(ttl=REFRESH_SEC)
def load_returns() -> pd.DataFrame:
    path = DATA_LAKE / "returns"
    if not path.exists():
        return pd.DataFrame()
    files = list(path.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True, errors="coerce")
    return df


def get_demo_orders() -> pd.DataFrame:
    """Shown while the pipeline hasn't produced data yet."""
    import random
    cities = [
        ("Mumbai","Maharashtra",1), ("Bengaluru","Karnataka",1),
        ("Jaipur","Rajasthan",3), ("Lucknow","Uttar Pradesh",3),
        ("Patna","Bihar",3),      ("Surat","Gujarat",2),
        ("Coimbatore","Tamil Nadu",2), ("Ranchi","Jharkhand",3),
        ("Pune","Maharashtra",2), ("Agra","Uttar Pradesh",3),
    ]
    cats = ["Fashion","Electronics","Home","Beauty","Grocery"]
    pmts = ["COD","UPI","Card"]
    rows = []
    for i in range(80):
        city, state, tier = random.choice(cities)
        cat = random.choice(cats)
        amt = round(random.uniform(150, 8000), 2)
        pmnt = random.choices(pmts, weights=[60,30,10] if tier==3 else [40,45,15])[0]
        rows.append({
            "order_id":         f"ORD-DEMO{i:04d}",
            "event_timestamp":  pd.Timestamp.now(tz="UTC") - pd.Timedelta(seconds=i*12),
            "order_date":       (datetime.now() - timedelta(days=i//20)).date(),
            "customer_id":      f"CUST-{i:06d}",
            "customer_name":    f"Demo User {i}",
            "customer_tier":    tier,
            "city":             city,
            "state":            state,
            "category":         cat,
            "subcategory":      cat,
            "quantity":         random.randint(1,3),
            "unit_price_inr":   round(amt/random.randint(1,3), 2),
            "total_amount_inr": amt,
            "payment_method":   pmnt,
            "warehouse_id":     "WH-DEMO",
            "is_anomalous":     (i % 15 == 0),
            "anomaly_reason":   f"demo_anomaly_{i}" if i % 15 == 0 else "",
            "ingestion_id":     f"demo-{i}",
            "consumed_at":      str(datetime.now()),
        })
    return pd.DataFrame(rows)


# ── Header ────────────────────────────────────────────────────

col_h, col_r = st.columns([5, 1])
with col_h:
    st.markdown("## 🛒 Bharatcommerce Intelligence Platform")
    st.caption(f"Live pipeline dashboard · {datetime.now().strftime('%d %b %Y %H:%M:%S')}")
with col_r:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("↻  Refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ── Load data ─────────────────────────────────────────────────

df_orders  = load_orders()
df_returns = load_returns()
is_demo    = df_orders.empty

if is_demo:
    st.info("⚡ No pipeline data yet — showing demo data. "
            "Start the producer and consumer to see live data.", icon="ℹ️")
    df_orders = get_demo_orders()

st.markdown("---")

# ── Tabs ──────────────────────────────────────────────────────

tab1, tab2, tab3, tab4 = st.tabs([
    "📡 Live event feed",
    "📊 Business KPIs",
    "⚠️ Anomaly log",
    "🔧 Pipeline health",
])


# ─────────────────────────────────────────────────────────────
# Tab 1 — Live event feed
# ─────────────────────────────────────────────────────────────
with tab1:
    st.markdown("#### Last 50 orders — streaming in real time")

    recent = (
        df_orders
        .sort_values("event_timestamp", ascending=False)
        .head(50)
        .reset_index(drop=True)
    )

    pm_icon = {"COD": "🟡", "UPI": "🟢", "Card": "🔵", "Wallet": "🟣"}

    for _, row in recent.iterrows():
        ts  = row["event_timestamp"]
        ago = int((pd.Timestamp.now(tz="UTC") - ts).total_seconds()) if pd.notnull(ts) else 0
        ago_str = f"{ago}s ago" if ago < 60 else f"{ago//60}m {ago%60}s ago"
        flag    = " 🚨 ANOMALY" if row.get("is_anomalous") else ""
        icon    = pm_icon.get(row.get("payment_method",""), "⚪")
        st.markdown(
            f"`{ago_str}` &nbsp; **{row['order_id']}** &nbsp;|&nbsp; "
            f"{row['city']}, {row['state']} &nbsp;|&nbsp; "
            f"{row['category']} — {row['subcategory']} &nbsp;|&nbsp; "
            f"**₹{row['total_amount_inr']:,.0f}** &nbsp; "
            f"{icon} {row['payment_method']}{flag}"
        )
        st.divider()


# ─────────────────────────────────────────────────────────────
# Tab 2 — Business KPIs
# ─────────────────────────────────────────────────────────────
with tab2:
    st.markdown("#### Business KPIs — all time")

    total_orders = len(df_orders)
    gmv          = df_orders["total_amount_inr"].sum()
    aov          = df_orders["total_amount_inr"].mean()
    cod_pct      = (df_orders["payment_method"] == "COD").mean() * 100
    anomaly_cnt  = df_orders["is_anomalous"].sum() if "is_anomalous" in df_orders.columns else 0

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Total orders",     f"{total_orders:,}")
    k2.metric("GMV",              f"₹{gmv/100_000:.1f}L")
    k3.metric("Avg order value",  f"₹{aov:,.0f}")
    k4.metric("COD ratio",        f"{cod_pct:.0f}%",
              delta="⚠ High" if cod_pct > 60 else "Normal",
              delta_color="inverse" if cod_pct > 60 else "normal")
    k5.metric("Anomalies flagged",f"{int(anomaly_cnt)}")

    st.markdown("---")
    c1, c2 = st.columns(2)

    with c1:
        st.markdown("#### GMV by state")
        gmv_state = (
            df_orders.groupby("state")["total_amount_inr"]
            .sum().reset_index()
            .sort_values("total_amount_inr", ascending=True)
            .tail(10)
        )
        fig = px.bar(gmv_state, x="total_amount_inr", y="state",
                     orientation="h", labels={"total_amount_inr":"GMV (₹)","state":""},
                     color_discrete_sequence=["#1D9E75"])
        fig.update_layout(margin=dict(l=0,r=0,t=0,b=0), height=320)
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.markdown("#### Payment method mix")
        pm_counts = df_orders["payment_method"].value_counts().reset_index()
        pm_counts.columns = ["method","count"]
        fig2 = px.pie(pm_counts, values="count", names="method",
                      color_discrete_sequence=["#FAC775","#1D9E75","#378ADD","#7F77DD"])
        fig2.update_layout(margin=dict(l=0,r=0,t=0,b=0), height=320)
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("#### Orders by category")
    cat_data = (
        df_orders.groupby("category")
        .agg(orders=("order_id","count"), gmv=("total_amount_inr","sum"))
        .reset_index().sort_values("gmv", ascending=False)
    )
    cat_data["aov"]      = (cat_data["gmv"] / cat_data["orders"]).round(0)
    cat_data["gmv_disp"] = cat_data["gmv"].apply(lambda x: f"₹{x:,.0f}")
    cat_data["aov_disp"] = cat_data["aov"].apply(lambda x: f"₹{x:,.0f}")
    st.dataframe(
        cat_data[["category","orders","gmv_disp","aov_disp"]].rename(columns={
            "category":"Category","orders":"Orders",
            "gmv_disp":"Total GMV","aov_disp":"Avg Order Value"
        }),
        use_container_width=True, hide_index=True,
    )


# ─────────────────────────────────────────────────────────────
# Tab 3 — Anomaly log
# ─────────────────────────────────────────────────────────────
with tab3:
    st.markdown("#### Flagged orders")

    if "is_anomalous" in df_orders.columns:
        anom = df_orders[df_orders["is_anomalous"]].sort_values(
            "event_timestamp", ascending=False
        ).head(50)
    else:
        anom = pd.DataFrame()

    if anom.empty:
        st.success("No anomalies detected. Pipeline is clean.")
    else:
        a1, a2 = st.columns(2)
        a1.metric("Total flagged", len(anom))
        a2.metric("Flagged GMV",   f"₹{anom['total_amount_inr'].sum():,.0f}")

        for _, row in anom.iterrows():
            amt  = row["total_amount_inr"]
            sev  = "🔴 Critical" if amt > 50000 else ("🟠 High" if amt > 15000 else "🟡 Medium")
            reason = str(row.get("anomaly_reason","")).replace("_"," ")
            ts   = row["event_timestamp"]
            st.markdown(
                f"{sev} &nbsp; **{row['order_id']}** — {row['city']}, {row['state']} "
                f"| {row['category']} | **₹{amt:,.0f}** | {row['payment_method']}"
            )
            st.caption(f"{reason} · {ts}")
            st.divider()


# ─────────────────────────────────────────────────────────────
# Tab 4 — Pipeline health
# ─────────────────────────────────────────────────────────────
with tab4:
    st.markdown("#### Pipeline health")

    # Count Parquet files in data_lake
    order_files  = list((DATA_LAKE / "orders").rglob("*.parquet"))  if (DATA_LAKE / "orders").exists()  else []
    return_files = list((DATA_LAKE / "returns").rglob("*.parquet")) if (DATA_LAKE / "returns").exists() else []

    h1, h2, h3, h4 = st.columns(4)
    h1.metric("Order Parquet files",  len(order_files))
    h2.metric("Return Parquet files", len(return_files))
    h3.metric("Total order rows",     f"{len(df_orders):,}")

    if not df_orders.empty and "event_timestamp" in df_orders.columns:
        latest    = df_orders["event_timestamp"].max()
        fresh_sec = int((pd.Timestamp.now(tz="UTC") - latest).total_seconds())
        fresh_str = f"{fresh_sec}s" if fresh_sec < 60 else f"{fresh_sec//60}m {fresh_sec%60}s"
        status    = "🟢 Fresh" if fresh_sec < 30 else ("🟡 Stale" if fresh_sec < 120 else "🔴 Old")
        h4.metric("Data freshness", fresh_str, delta=status)
    else:
        h4.metric("Data freshness", "No data yet")

    st.markdown("---")
    st.markdown("#### Service checklist")

    services = [
        ("Zookeeper",        "docker compose up -d",                   "http://—",            "port 2181"),
        ("Kafka broker",     "docker compose up -d",                   "http://—",            "port 9092"),
        ("Kafka UI",         "docker compose up -d",                   "http://localhost:8080","Redpanda Console"),
        ("Order generator",  "python producer/order_generator.py",     "terminal",            "orders.raw topic"),
        ("Parquet consumer", "python consumer/kafka_to_parquet.py",    "terminal",            "data_lake/ folder"),
        ("This dashboard",   "streamlit run dashboard/app.py",         "http://localhost:8501","you're here"),
    ]

    for svc, cmd, url, note in services:
        c1, c2, c3, c4 = st.columns([2, 3, 2, 2])
        c1.markdown(f"**{svc}**")
        c2.code(cmd, language=None)
        c3.markdown(f"[{url}]({url})" if url.startswith("http://localhost") else url)
        c4.markdown(f"*{note}*")
        st.divider()

    st.markdown("#### Recent Parquet files written")
    if order_files:
        file_info = sorted(
            [{"file": f.name, "path": str(f.relative_to(DATA_LAKE)),
              "size_kb": round(f.stat().st_size / 1024, 1)}
             for f in order_files],
            key=lambda x: x["file"], reverse=True
        )[:20]
        st.dataframe(pd.DataFrame(file_info), use_container_width=True, hide_index=True)
    else:
        st.info("No Parquet files yet — start producer and consumer first.")


# ── Auto-refresh ──────────────────────────────────────────────
time.sleep(REFRESH_SEC)
st.rerun()