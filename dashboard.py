import streamlit as st
import pandas as pd
import json
import time
from kafka import KafkaConsumer
from datetime import datetime
from fraud_scoring import load_models, score_transaction

st.set_page_config(page_title="Fraud Detection Dashboard", layout="wide")

st.title("🚨 Real-Time Fraud Detection Dashboard")
st.markdown("**PaySim Fraud Detection Pipeline — CSIS 4260**")
st.caption("3-layer detection: Rule Engine + Random Forest + Isolation Forest")

# ── Metrics row ──────────────────────────────────────────────────────────────
col1, col2, col3, col4, col5 = st.columns(5)
total_placeholder  = col1.empty()
fraud_placeholder  = col2.empty()
real_placeholder   = col3.empty()
rate_placeholder   = col4.empty()
amount_placeholder = col5.empty()

total_placeholder.metric("Total Transactions", "0")
fraud_placeholder.metric("Fraud Alerts", "0")
real_placeholder.metric("Real Frauds Caught", "0")
rate_placeholder.metric("Fraud Rate", "0%")
amount_placeholder.metric("Total Flagged Amount", "$0")

st.divider()

st.subheader("Live Fraud Alerts")
alert_table = st.empty()

st.divider()

st.subheader("All Transactions (Live)")
st.caption("Every transaction passing through the pipeline, flagged or not")
all_tx_table = st.empty()

st.divider()

st.subheader("Fraud by Detection Method")
chart_area = st.empty()

# ── Load models once (not per-transaction) ──────────────────────────────────
@st.cache_resource
def get_models():
    return load_models()

models = get_models()

# ── State ────────────────────────────────────────────────────────────────────
alerts       = []
all_records  = []   # every transaction, scrutinized or not
total     = 0
flagged   = 0
real      = 0
total_amt = 0.0

# ── Kafka consumer ────────────────────────────────────────────────────────────
@st.cache_resource
def get_consumer():
    return KafkaConsumer(
        'fraud-transactions',
        bootstrap_servers='127.0.0.1:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='dashboard-group',
        api_version=(0, 10, 2),
        consumer_timeout_ms=1000
    )

consumer = get_consumer()

st.info("Waiting for transactions... Make sure producer.py is running in another window.")

# ── Main loop ─────────────────────────────────────────────────────────────────
while True:
    try:
        batch = consumer.poll(timeout_ms=2000, max_records=50)
        for tp, messages in batch.items():
            for message in messages:
                tx     = message.value
                total += 1

                result = score_transaction(tx, models)  # runs all 3 layers
                hit         = result['is_flagged']
                is_real     = tx.get('isFraud') == 1
                method      = '+'.join(result['layers_triggered']) if hit else '-'

                # Log every transaction that gets scrutinized by the pipeline,
                # regardless of outcome — this powers the "All Transactions" table.
                all_records.append({
                    'Time'           : datetime.now().strftime('%H:%M:%S'),
                    'From'           : tx['nameOrig'],
                    'Type'           : tx['type'],
                    'Amount'         : f"${tx['amount']:,.2f}",
                    'Real Flagged'   : '🔴 Yes' if is_real else '⚪ No',
                    'System Flagged' : '🔴 Yes' if hit else '⚪ No',
                    'Method'         : method,
                    'RF Prob'        : f"{result['layer_detail']['random_forest']['probability']*100:.0f}%",
                })

                # Show a row if the system flagged it OR it's a real fraud the
                # system missed — otherwise "Real Flagged" would never have a
                # chance to disagree with "System Flagged" in this table.
                if hit or is_real:
                    if hit:
                        flagged   += 1
                        total_amt += tx['amount']
                        if is_real:
                            real += 1
                    alerts.append({
                        'Time'           : datetime.now().strftime('%H:%M:%S'),
                        'From'           : tx['nameOrig'],
                        'Type'           : tx['type'],
                        'Amount'         : f"${tx['amount']:,.2f}",
                        'Real Flagged'   : '🔴 Yes' if is_real else '⚪ No',
                        'System Flagged' : '🔴 Yes' if hit else '⚪ No',
                        'Method'         : method,
                        'RF Prob'        : f"{result['layer_detail']['random_forest']['probability']*100:.0f}%",
                    })

        # Update all 5 metrics
        total_placeholder.metric("Total Transactions", f"{total:,}")
        fraud_placeholder.metric("Fraud Alerts", f"{flagged:,}")
        real_placeholder.metric("Real Frauds Caught", f"{real:,}")
        rate_placeholder.metric("Fraud Rate", f"{flagged/total*100:.2f}%" if total > 0 else "0%")
        amount_placeholder.metric("Total Flagged Amount", f"${total_amt:,.0f}")

        # Update alert table (last 20)
        if alerts:
            df = pd.DataFrame(alerts[-20:])
            df.index = range(1, len(df) + 1)
            alert_table.dataframe(df, use_container_width=True)

        # Update all-transactions table (last 20)
        if all_records:
            df_all_tx = pd.DataFrame(all_records[-20:])
            df_all_tx.index = range(1, len(df_all_tx) + 1)
            all_tx_table.dataframe(df_all_tx, use_container_width=True)

        # Update chart — only counts rows the system actually flagged,
        # so missed-fraud rows (Method: '-') don't show up as a fake category
        if alerts:
            df_all = pd.DataFrame(alerts)
            flagged_only = df_all[df_all['System Flagged'] == '🔴 Yes']
            if not flagged_only.empty:
                method_counts = flagged_only['Method'].value_counts()
                chart_area.bar_chart(method_counts)

        time.sleep(1)

    except Exception as e:
        time.sleep(2)
        continue