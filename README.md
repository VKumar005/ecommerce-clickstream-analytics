# E-Commerce Clickstream Analytics Pipeline

Real-time data pipeline for e-commerce user behaviour analytics.

## Architecture
Python Producer → Apache Kafka → Apache Spark Streaming → PostgreSQL → Power BI
## Tech Stack
- Apache Kafka 4.x (KRaft mode - No Zookeeper)
- Apache Spark 3.5.8
- PostgreSQL 17
- Power BI Desktop
- Python 3.12

## Project Files
- `producer.py` — Simulates clickstream events, sends to Kafka
- `spark_consumer.py` — Reads from Kafka, processes with Spark, writes to PostgreSQL
- `requirements.txt` — Python dependencies

## How to Run
1. Start Kafka: `bin\windows\kafka-server-start.bat config\server.properties`
2. Set password: `set PG_PASSWORD=your_postgres_password`
3. Run producer: `python producer.py`
4. Run Spark: `run_spark.bat`
5. Connect Power BI to PostgreSQL

## Dashboard Insights
- User events by type
- Revenue by product category
- Traffic by country
- Device distribution
- Total events and revenue KPIs
