import json
import time
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from datetime import datetime

KAFKA_BROKER  = '127.0.0.1:9092'
TOPIC_NAME    = 'fraud-transactions'
CSV_FILE      = 'fraudetection.csv'
DELAY_SECONDS = 0.01
PRINT_EVERY   = 1000

def connect_kafka(retries=10, delay=5):
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                api_version=(0,10,2),
                request_timeout_ms=30000,
                metadata_max_age_ms=10000,
                connections_max_idle_ms=30000
            )
            print(f'Connected to Kafka at {KAFKA_BROKER}')
            return producer
        except NoBrokersAvailable:
            print(f'Kafka not ready. Retrying in {delay}s... (attempt {attempt+1}/{retries})')
            time.sleep(delay)
    raise Exception('Could not connect to Kafka. Is docker-compose running?')

print('Loading fraudetection.csv...')
df = pd.read_csv(CSV_FILE)
print(f'Loaded {len(df):,} transactions')
print(f'Fraud cases: {df["isFraud"].sum():,} ({df["isFraud"].mean():.3%})')
print()

producer = connect_kafka()

print(f'Streaming to Kafka topic: {TOPIC_NAME}')
print('Press Ctrl+C to stop\n')

sent       = 0
fraud_sent = 0
start_time = time.time()

try:
    for _, row in df.iterrows():
        message = {
            'step'           : int(row['step']),
            'type'           : str(row['type']),
            'amount'         : float(row['amount']),
            'nameOrig'       : str(row['nameOrig']),
            'oldbalanceOrg'  : float(row['oldbalanceOrg']),
            'newbalanceOrig' : float(row['newbalanceOrig']),
            'nameDest'       : str(row['nameDest']),
            'oldbalanceDest' : float(row['oldbalanceDest']),
            'newbalanceDest' : float(row['newbalanceDest']),
            'isFraud'        : int(row['isFraud']),
            'isFlaggedFraud' : int(row['isFlaggedFraud']),
            'timestamp'      : datetime.now().isoformat()
        }

        producer.send(TOPIC_NAME, value=message)
        sent += 1

        if row['isFraud'] == 1:
            fraud_sent += 1

        if sent % PRINT_EVERY == 0:
            elapsed = time.time() - start_time
            rate = sent / elapsed
            print(f'Sent: {sent:>8,} | Fraud sent: {fraud_sent:>5,} | Rate: {rate:>6.0f} tx/s')

        time.sleep(DELAY_SECONDS)

except KeyboardInterrupt:
    print(f'\nStopped after {sent:,} transactions')

finally:
    producer.flush()
    producer.close()
    elapsed = time.time() - start_time
    print(f'\nTotal sent  : {sent:,}')
    print(f'Fraud sent  : {fraud_sent:,}')
    print(f'Time elapsed: {elapsed:.1f}s')