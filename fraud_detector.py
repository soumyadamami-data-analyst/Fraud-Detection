"""
Real-Time Fraud Detector
Kafka → Rule Engine + Random Forest + Isolation Forest → Alerts
"""

import json
import time
import os
import joblib
import numpy as np
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from datetime import datetime

KAFKA_BROKER         = 'localhost:9092'
TOPIC_NAME           = 'fraud-transactions'
RF_MODEL_PATH        = 'fraud_rf.joblib'
ISO_MODEL_PATH       = 'isolation_forest.joblib'
FRAUD_PROB_THRESHOLD = 0.5

# ── Load models ───────────────────────────────────────────────────────────────
if os.path.exists(RF_MODEL_PATH):
    rf_model = joblib.load(RF_MODEL_PATH)
    print(f'✓ Random Forest loaded')
    RF_AVAILABLE = True
else:
    print('✗ Random Forest not found')
    RF_AVAILABLE = False

if os.path.exists(ISO_MODEL_PATH):
    iso_model = joblib.load(ISO_MODEL_PATH)
    print(f'✓ Isolation Forest loaded')
    ISO_AVAILABLE = True
else:
    print('✗ Isolation Forest not found - copy isolation_forest.joblib here')
    ISO_AVAILABLE = False

# ── Feature engineering ───────────────────────────────────────────────────────
def rf_features(tx):
    type_map = {'CASH_IN': 0, 'CASH_OUT': 1, 'DEBIT': 2, 'PAYMENT': 3, 'TRANSFER': 4}
    a, oo, no, od, nd, t = (tx['amount'], tx['oldbalanceOrg'], tx['newbalanceOrig'],
                             tx['oldbalanceDest'], tx['newbalanceDest'], tx['type'])
    return [[a, oo, no, od, nd,
             type_map.get(t, -1),
             1 if t in ('TRANSFER', 'CASH_OUT') else 0,
             1 if (no == 0 and oo > 0) else 0]]

def iso_features(tx):
    return [[tx['amount'], tx['oldbalanceOrg'], tx['newbalanceOrig'],
             tx['oldbalanceDest'], tx['newbalanceDest']]]

# ── Detection layers ──────────────────────────────────────────────────────────
def rule_flag(tx):
    return (tx['type'] in ('TRANSFER', 'CASH_OUT') and
            tx['newbalanceOrig'] == 0 and
            tx['oldbalanceOrg'] > 0 and
            tx['amount'] > 100_000)

def rf_flag(tx):
    if not RF_AVAILABLE:
        return False, 0.0
    prob = float(rf_model.predict_proba(rf_features(tx))[0][1])
    return prob >= FRAUD_PROB_THRESHOLD, prob

def iso_flag(tx):
    if not ISO_AVAILABLE:
        return False
    result = iso_model.predict(iso_features(tx))[0]
    return result == -1  # -1 means anomaly in Isolation Forest

# ── Kafka connection ──────────────────────────────────────────────────────────
def connect_kafka(retries=10, delay=5):
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='fraud-detector-group',
                api_version=(0, 10, 2)
            )
            print(f'✓ Connected to Kafka')
            return consumer
        except NoBrokersAvailable:
            print(f'  Kafka not ready, retrying ({attempt+1}/{retries})...')
            time.sleep(delay)
    raise Exception('Could not connect to Kafka. Is docker-compose running?')

# ── Main ──────────────────────────────────────────────────────────────────────
consumer   = connect_kafka()
total      = 0
flagged    = 0
start_time = time.time()

print()
print('=' * 65)
print('  3-LAYER FRAUD DETECTOR RUNNING')
print('  Layer 1: Rule Engine')
print('  Layer 2: Random Forest (supervised ML)')
print('  Layer 3: Isolation Forest (unsupervised anomaly detection)')
print()
print('  Open a second Anaconda Prompt and run: python producer.py')
print('  Press Ctrl+C to stop')
print('=' * 65)
print()

try:
    for message in consumer:
        tx     = message.value
        total += 1

        rule_hit          = rule_flag(tx)
        rf_hit, rf_prob   = rf_flag(tx)
        iso_hit           = iso_flag(tx)

        if rule_hit or rf_hit or iso_hit:
            flagged += 1
            layers  = []
            if rule_hit: layers.append('RULE')
            if rf_hit:   layers.append('RF')
            if iso_hit:  layers.append('ISO')
            method = '+'.join(layers)
            now    = datetime.now().strftime('%H:%M:%S')

            print(f'🚨 FRAUD #{flagged} [{now}] | {tx["type"]} | ${tx["amount"]:>12,.2f} | Detected by: {method} | RF prob: {rf_prob:.2f} | Actual: {tx.get("isFraud","?")}')

        if total % 5000 == 0:
            elapsed = time.time() - start_time
            print(f'--- {total:,} processed | {flagged} alerts | {elapsed:.0f}s ---')

except KeyboardInterrupt:
    elapsed = time.time() - start_time
    print(f'\nStopped. {total:,} processed, {flagged} flagged in {elapsed:.1f}s')
finally:
    consumer.close()