@echo off
echo ============================================================
echo   PaySim Fraud Detection Pipeline - Startup Script
echo ============================================================
echo.
echo [Step 1] Starting Kafka and Spark with Docker...
docker-compose up -d
echo.
echo [Step 2] Waiting 30 seconds for Kafka to be ready...
timeout /t 30 /nobreak
echo.
echo [Step 3] Installing Python Kafka library...
pip install kafka-python --quiet
echo.
echo ============================================================
echo  DONE! Now open TWO more Command Prompt windows:
echo.
echo  Window 1 - run this:
echo  docker exec spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /app/fraud_detector.py
echo.
echo  Window 2 - run this:
echo  python producer.py
echo ============================================================
pause