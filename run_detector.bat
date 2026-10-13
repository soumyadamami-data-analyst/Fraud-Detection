@echo off
set HADOOP_HOME=C:\hadoop
set JAVA_HOME=C:\Program Files\Java\jdk-17.0.19
set PYSPARK_SUBMIT_ARGS=--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell

cd /d "%~dp0"
python fraud_detector.py
pause