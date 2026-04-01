"""
Bharatcommerce Intelligence Platform
Production dashboard — reads from BigQuery.
Deployed on Streamlit Cloud (free, always-on).

Local run:   streamlit run dashboard/app.py
Cloud:       Auto-deploys from GitHub via streamlit.io/cloud
"""

import os
import json
import time
import warnings
warnings.filterwarnings("ignore")
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

PROJECT = "sigma-composite-492018-p3"
DATASET = "bharatcommerce_gold"

st.set_page_config(
    page_title="Bharatcommerce Intelligence",
    page_icon="🛒",
    layout="wide",
)

st.markdown("""
<style>
[data-testid="stMetric"] {
    background: var(--background-color);
    border: 1px solid rgba(128,128,128,0.2);
    border-radius:10px;
    padding:14px;
}
[data-testid="stMetricLabel"] { font-size:12px !important; }
[data-testid="stMetricValue"] { color: var(--text-color) !important; }
[data-testid="stMetricDelta"] { color: var(--text-color) !important; }
</style>
""", unsafe_allow_html=True)


# ── BigQuery client ───────────────────────────────────────────

@st.cache_resource
def get_bq_client():
    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account

        # Streamlit Cloud: secrets set in dashboard
        if "GCP_SA_KEY" in st.secrets:
            key = st.secrets["GCP_SA_KEY"]
            info = json.loads(key) if isinstance(key, str) else dict(key)
            creds = service_account.Credentials.from_service_account_info(info)
            return bigquery.Client(credentials=creds, project=PROJECT)

        # Local: use gcp_key.json
        key_file = Path("gcp_key.json")
        if key_file.exists():
            return bigquery.Client.from_service_account_json(str(key_file), project=PROJECT)

    except Exception as e:
        st.warning(f"BigQuery not connected: {e}")
    return None


@st.cache_data(ttl=30)
def query(sql: str) -> pd.DataFrame:
    client = get_bq_client()
    if client is None:
        return pd.DataFrame()
    try:
        return client.query(sql).to_dataframe()
    except Exception as e:
        st.warning(f"Query error: {e}")
        return pd.DataFrame()


def load_kpis():
    return query(f"""
        SELECT
            COUNT(*)                                        AS total_orders,
            ROUND(SUM(total_amount_inr), 0)                AS gmv_inr,
            ROUND(AVG(total_amount_inr), 0)                AS aov_inr,
            ROUND(COUNTIF(is_cod) / COUNT(*) * 100, 1)     AS cod_pct,
            COUNTIF(is_anomalous)                          AS anomaly_count,
            COUNTIF(is_high_return_risk)                   AS high_return_risk
        FROM `{PROJECT}.{DATASET}.fct_orders`
    """)


def load_recent_orders():
    return query(f"""
        SELECT order_id, event_timestamp, city, state, category,
               total_amount_inr, payment_method, is_anomalous, anomaly_reason
        FROM `{PROJECT}.{DATASET}.fct_orders`
        ORDER BY event_timestamp DESC
        LIMIT 50
    """)


def load_state_performance():
    return query(f"""
        SELECT state, total_orders, total_gmv_inr, aov_inr,
               cod_pct, cod_risk_score, state_profile
        FROM `{PROJECT}.{DATASET}.agg_state_performance`
        ORDER BY total_gmv_inr DESC
    """)


def load_daily_gmv():
    return query(f"""
        SELECT order_date, SUM(gmv_inr) AS gmv_inr, SUM(order_count) AS orders
        FROM `{PROJECT}.{DATASET}.agg_daily_gmv`
        GROUP BY order_date ORDER BY order_date
    """)


def load_category_breakdown():
    return query(f"""
        SELECT category,
               COUNT(*)                            AS orders,
               ROUND(SUM(total_amount_inr), 0)    AS gmv_inr,
               ROUND(AVG(total_amount_inr), 0)    AS aov_inr,
               COUNTIF(is_cod)                    AS cod_orders,
               COUNTIF(is_anomalous)              AS anomalies
        FROM `{PROJECT}.{DATASET}.fct_orders`
        GROUP BY 1 ORDER BY gmv_inr DESC
    """)


def load_anomalies():
    return query(f"""
        SELECT order_id, order_date, city, state, category,
               total_amount_inr, payment_method, anomaly_reason, severity
        FROM `{PROJECT}.{DATASET}.agg_anomaly_log`
        ORDER BY total_amount_inr DESC
        LIMIT 100
    """)


def load_payment_mix():
    return query(f"""
        SELECT payment_method, COUNT(*) AS orders
        FROM `{PROJECT}.{DATASET}.fct_orders`
        GROUP BY 1 ORDER BY 2 DESC
    """)


# ── Demo data fallback ────────────────────────────────────────

def demo_kpis():
    return pd.DataFrame([{
        "total_orders": 361, "gmv_inr": 2352910,
        "aov_inr": 6517, "cod_pct": 53.5,
        "anomaly_count": 12, "high_return_risk": 28,
    }])


def demo_orders():
    import random; random.seed(42)
    cities = [("Mumbai","Maharashtra"),("Bengaluru","Karnataka"),
              ("Jaipur","Rajasthan"),("Patna","Bihar"),("Lucknow","UP")]
    cats   = ["Fashion","Electronics","Home","Beauty","Grocery"]
    pmts   = ["COD","UPI","Card"]
    rows   = []
    for i in range(50):
        c, s = random.choice(cities)
        rows.append({
            "order_id": f"ORD-DEMO{i:04d}",
            "event_timestamp": pd.Timestamp.now(tz="UTC") - pd.Timedelta(seconds=i*15),
            "city": c, "state": s,
            "category": random.choice(cats),
            "total_amount_inr": round(random.uniform(200,8000),0),
            "payment_method": random.choices(pmts,[60,30,10])[0],
            "is_anomalous": i%12==0,
            "anomaly_reason": f"demo_anomaly" if i%12==0 else "",
        })
    return pd.DataFrame(rows)


# ── Header ────────────────────────────────────────────────────

col_h, col_r = st.columns([5, 1])
with col_h:
    st.markdown("## 🛒 Bharatcommerce Intelligence Platform")
    st.caption(
        f"Real-time Indian e-commerce pipeline · "
        f"Kafka → Parquet → BigQuery → dbt · "
        f"{datetime.now().strftime('%d %b %Y %H:%M:%S')}"
    )
with col_r:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("↻ Refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# Check connection
client = get_bq_client()
is_connected = client is not None

if is_connected:
    st.success("Connected to BigQuery — showing live pipeline data")
else:
    st.info("BigQuery not connected — showing demo data. "
            "Add GCP_SA_KEY to Streamlit secrets to see live data.")

st.markdown("---")

tab1, tab2, tab3, tab4 = st.tabs([
    "📡 Live event feed",
    "📊 Business KPIs",
    "⚠️  Anomaly log",
    "🗺️  State intelligence",
])


# ── Tab 1: Live event feed ────────────────────────────────────

with tab1:
    st.markdown("#### Last 50 orders from the pipeline")
    df = load_recent_orders() if is_connected else demo_orders()

    pm_icon = {"COD":"🟡","UPI":"🟢","Card":"🔵","Wallet":"🟣"}
    for _, row in df.iterrows():
        ts = row["event_timestamp"]
        try:
            ago = int((pd.Timestamp.now(tz="UTC") - pd.Timestamp(ts, tz="UTC")).total_seconds())
            ago_str = f"{ago}s ago" if ago < 60 else f"{ago//60}m ago"
        except Exception:
            ago_str = "recently"
        flag = " 🚨" if row.get("is_anomalous") else ""
        icon = pm_icon.get(str(row.get("payment_method","")), "⚪")
        st.markdown(
            f"`{ago_str}` &nbsp; **{row['order_id']}** &nbsp;|&nbsp; "
            f"{row['city']}, {row['state']} &nbsp;|&nbsp; "
            f"{row['category']} &nbsp;|&nbsp; "
            f"**₹{float(row['total_amount_inr']):,.0f}** &nbsp;"
            f"{icon} {row['payment_method']}{flag}"
        )
        st.divider()


# ── Tab 2: Business KPIs ──────────────────────────────────────

with tab2:
    st.markdown("#### Business KPIs")
    df_kpi = load_kpis() if is_connected else demo_kpis()

    if not df_kpi.empty:
        row = df_kpi.iloc[0]
        k1,k2,k3,k4,k5 = st.columns(5)
        k1.metric("Total orders",     f"{int(row['total_orders']):,}")
        k2.metric("GMV",              f"₹{float(row['gmv_inr'])/100_000:.1f}L")
        k3.metric("Avg order value",  f"₹{float(row['aov_inr']):,.0f}")
        k4.metric("COD ratio",        f"{float(row['cod_pct']):.0f}%",
                  delta="High risk" if float(row['cod_pct'])>60 else "Normal",
                  delta_color="inverse" if float(row['cod_pct'])>60 else "normal")
        k5.metric("Anomalies flagged",str(int(row['anomaly_count'])))

    st.markdown("---")

    df_trend = load_daily_gmv() if is_connected else pd.DataFrame()
    if not df_trend.empty:
        st.markdown("#### GMV trend")
        st.line_chart(df_trend.set_index("order_date")["gmv_inr"])

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### Category breakdown")
        df_cat = load_category_breakdown() if is_connected else pd.DataFrame()
        if not df_cat.empty:
            fig = px.bar(df_cat, x="category", y="gmv_inr",
                         color_discrete_sequence=["#1D9E75"],
                         labels={"gmv_inr":"GMV (₹)","category":""})
            fig.update_layout(margin=dict(l=0,r=0,t=0,b=0), height=280)
            st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.markdown("#### Payment method mix")
        df_pm = load_payment_mix() if is_connected else pd.DataFrame()
        if not df_pm.empty:
            fig2 = px.pie(df_pm, values="orders", names="payment_method",
                          color_discrete_sequence=["#FAC775","#1D9E75","#378ADD","#7F77DD"])
            fig2.update_layout(margin=dict(l=0,r=0,t=0,b=0), height=280)
            st.plotly_chart(fig2, use_container_width=True)


# ── Tab 3: Anomaly log ────────────────────────────────────────

with tab3:
    st.markdown("#### Flagged orders — anomaly detection")
    df_anom = load_anomalies() if is_connected else pd.DataFrame()

    if df_anom.empty:
        st.success("No anomalies in dataset.")
    else:
        a1,a2,a3 = st.columns(3)
        a1.metric("Total flagged", len(df_anom))
        a2.metric("Critical",      len(df_anom[df_anom["severity"]=="critical"]) if "severity" in df_anom.columns else 0)
        a3.metric("High",          len(df_anom[df_anom["severity"]=="high"]) if "severity" in df_anom.columns else 0)
        st.markdown("---")

        sev_icon = {"critical":"🔴","high":"🟠","medium":"🟡"}
        for _, row in df_anom.iterrows():
            sev  = str(row.get("severity","medium"))
            icon = sev_icon.get(sev,"🟡")
            reason = str(row.get("anomaly_reason","")).replace("_"," ")
            st.markdown(
                f"{icon} **{row['order_id']}** — {row['city']}, {row['state']} "
                f"| {row['category']} | **₹{float(row['total_amount_inr']):,.0f}** "
                f"| {row['payment_method']}"
            )
            st.caption(reason)
            st.divider()

    st.markdown("#### Detection rules")
    rules = [
        ("High-value order",  "Amount > μ + 3σ for category",    "Fraud or test order"),
        ("COD threshold",     "COD order > ₹5,000",               "Non-delivery risk"),
        ("Bulk residential",  "Quantity > 15 from home pincode",  "Reseller signal"),
    ]
    for name, condition, why in rules:
        with st.expander(f"**{name}** — {condition}"):
            st.write(f"Why it matters: {why}")


# ── Tab 4: State intelligence ─────────────────────────────────

with tab4:
    st.markdown("#### COD risk score by state")
    st.caption("Formula: return_risk × 0.4 + cod_ratio × 0.3 + tier3_ratio × 0.3")

    df_state = load_state_performance() if is_connected else pd.DataFrame()

    if not df_state.empty:
        k1,k2,k3 = st.columns(3)
        top_risk   = df_state.loc[df_state["cod_risk_score"].idxmax()]
        top_gmv    = df_state.loc[df_state["total_gmv_inr"].idxmax()]
        top_orders = df_state.loc[df_state["total_orders"].idxmax()]
        k1.metric("Highest risk state",  top_risk["state"],
                  delta=f"Score {top_risk['cod_risk_score']:.3f}")
        k2.metric("Highest GMV state",   top_gmv["state"],
                  delta=f"₹{float(top_gmv['total_gmv_inr'])/100_000:.1f}L")
        k3.metric("Most orders",         top_orders["state"],
                  delta=f"{int(top_orders['total_orders'])} orders")

        st.markdown("---")

        fig = px.bar(
            df_state.sort_values("cod_risk_score", ascending=True),
            x="cod_risk_score", y="state", orientation="h",
            color="cod_risk_score",
            color_continuous_scale=["#1D9E75","#FAC775","#D85A30"],
            labels={"cod_risk_score":"COD risk score","state":""},
        )
        fig.update_layout(margin=dict(l=0,r=0,t=0,b=0), height=350,
                          coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("#### Full state table")
        st.dataframe(
            df_state.rename(columns={
                "state":"State","total_orders":"Orders",
                "total_gmv_inr":"GMV (₹)","aov_inr":"AOV (₹)",
                "cod_pct":"COD %","cod_risk_score":"Risk Score",
                "state_profile":"Profile",
            }),
            use_container_width=True, hide_index=True,
        )
    else:
        st.info("Connect BigQuery to see state intelligence data.")


# Auto-refresh every 30s
time.sleep(30)
st.rerun()
