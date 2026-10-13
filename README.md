# Real-Time Fraud Detection Pipeline

A real-time fraud detection system built on Apache Kafka, PySpark 
Structured Streaming, and machine learning, using the PaySim synthetic 
financial dataset (6.36 million transactions).

## What This Project Does
Simulates a live bank transaction stream and detects fraud in real time 
using three layers of detection — rules, supervised ML, and unsupervised 
anomaly detection.

## Architecture
PaySim CSV → Kafka Producer → PySpark Streaming → Rule Engine + Random Forest + Isolation Forest → Streamlit Dashboard

## Detection Layers
- **Layer 1 — Rule Engine:** Filters by transaction type, flags velocity bursts, large amounts, and balance drain patterns
- **Layer 2 — Random Forest:** Supervised ML model trained on 6.36M PaySim transactions (F1 > 0.85, class_weight=balanced)
- **Layer 3 — Isolation Forest:** Unsupervised anomaly detector for catching novel fraud patterns not seen in training

## Tech Stack
Python, Apache Kafka, PySpark, scikit-learn, Docker, Streamlit, pandas, Jupyter

## How to Run
1. Download PaySim dataset from https://www.kaggle.com/datasets/ealaxi/paysim1 and save as `fraudetection.csv`
2. Install Docker Desktop and run: `docker-compose up -d`
3. Wait 30 seconds for Kafka to fully start
4. Open Terminal 1 and run: `python producer.py`
5. Open Terminal 2 and run: `python fraud_detector.py`
6. Open Terminal 3 and run: `streamlit run dashboard.py`

## Dataset
- **Name:** PaySim Synthetic Financial Dataset
- **Source:** https://www.kaggle.com/datasets/ealaxi/paysim1
- **Size:** 6.36 million transactions, 11 columns
- **Fraud rate:** 0.13% (773:1 class imbalance)

## Key Findings from EDA
- Fraud occurs exclusively in TRANSFER and CASH_OUT transaction types
- 49.8% of fraud cases drain the origin account to zero
- Fraud is uniformly distributed across all hours — justifying 24/7 streaming detection
- Class imbalance of 773:1 requires F1-score evaluation, not raw accuracy
