# RUNBOOK — E-Commerce Clickstream Analytics Pipeline

This document describes how to **operate, monitor, and recover** the real-time clickstream analytics pipeline.

---

## Platform Components

| Component | Role |
|-----------|------|
| Apache Kafka 4.x | Event backbone |
| Python Producer | Clickstream simulator |
| Apache Spark 3.5.8 | Stream processor |
| PostgreSQL 17 | Analytics database |
| Power BI | Dashboard |

---

## Service Startup Order

Services must be started in the following order:

1. **Kafka Broker**
2. **Python Producer**
3. **Spark Streaming Job**
4. **Power BI** (connect after data flows)

Starting Spark before Kafka will cause connection errors.

---

## Kafka Operations

### Start Kafka (KRaft Mode — No Zookeeper)
```cmd
cd C:\kafka
bin\windows\kafka-server-start.bat config\server.properties
```

### Verify Kafka is Running
```cmd
bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
```
Expected output: `clickstream`

### Create Topic (First Time Only)
```cmd
bin\windows\kafka-topics.bat --create --topic clickstream --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### Watch Live Messages (Debugging)
```cmd
bin\windows\kafka-console-consumer.bat --topic clickstream --from-beginning --bootstrap-server localhost:9092
```

### Restart Kafka (If Crashed)
```cmd
rmdir /s /q C:\kafka\kafka-logs
bin\windows\kafka-storage.bat random-uuid
bin\windows\kafka-storage.bat format --standalone -t YOUR-UUID -c config\server.properties
bin\windows\kafka-server-start.bat config\server.properties
```

---

## Producer Operations

### Start Producer
```cmd
venv\Scripts\activate
python producer.py
```

Expected output:
```
==================================================
  Clickstream Producer Started
  Topic : clickstream
  Rate  : 5 events/second
==================================================
[    10 events] page_view | User: U0042 | Device: mobile | Country: USA
```

### Stop Producer
Press `Ctrl + C`

---

## Spark Streaming Operations

### Set Password (Required Every Session)
```cmd
set PG_PASSWORD=your_postgres_password
```

### Start Spark Consumer
```cmd
run_spark.bat
```

Expected output after ~10 seconds:
```
Spark streaming running. Waiting for Kafka data...
--- Batch 0 | 50 rows ---
Written 50 raw events + aggregates to PostgreSQL
```

### Stop Spark
Press `Ctrl + C`

---

## PostgreSQL Operations

### Connect to Database
```cmd
"C:\Program Files\PostgreSQL\17\bin\psql.exe" -U postgres -h localhost -d ecommerce_analytics
```

### Verify Data is Flowing
```sql
SELECT COUNT(*) FROM clickstream_events;
SELECT event_type, COUNT(*) FROM clickstream_events GROUP BY event_type;
SELECT category, SUM(total_revenue) FROM clickstream_aggregates GROUP BY category;
```

### Clear All Data (Reset)
```sql
TRUNCATE TABLE clickstream_events;
TRUNCATE TABLE clickstream_aggregates;
```

---

## Power BI Operations

### Connect to PostgreSQL
- Server: `localhost`
- Database: `ecommerce_analytics`
- Username: `analytics_viewer`
- Password: `viewer_pass_123`

### Refresh Data
Click **Home → Refresh** in Power BI Desktop

---

## Failure Scenarios & Recovery

### Kafka Broker Crash
**Symptom:** Producer shows `NoBrokersAvailable`

**Recovery:**
```cmd
# Delete corrupted logs
rmdir /s /q C:\kafka\kafka-logs

# Generate new UUID
bin\windows\kafka-storage.bat random-uuid

# Format and restart
bin\windows\kafka-storage.bat format --standalone -t NEW-UUID -c config\server.properties
bin\windows\kafka-server-start.bat config\server.properties
```

---

### Spark NativeIO Error (hadoop.dll)
**Symptom:** `UnsatisfiedLinkError: NativeIO$Windows.access0`

**Recovery:**
```cmd
# Verify hadoop.dll exists in Spark bin
dir C:\spark358\spark-3.5.8-bin-hadoop3\bin\hadoop.dll

# If missing, copy from hadoop folder
copy C:\hadoop\bin\hadoop.dll C:\spark358\spark-3.5.8-bin-hadoop3\bin\
```

---

### PostgreSQL Authentication Failed
**Symptom:** `FATAL: password authentication failed`

**Recovery:**
```cmd
# Make sure password is set
set PG_PASSWORD=your_actual_password

# Then restart Spark
run_spark.bat
```

---

### Spark Empty Batches (0 rows)
**Symptom:** `--- Batch 0 | 0 rows ---`

**Recovery:**
- Make sure producer is running
- Make sure Kafka topic `clickstream` exists
- Check Kafka is running in CMD window

---

### Power BI Shows Old Data
**Recovery:**
- Click **Home → Refresh** in Power BI
- Check Spark consumer is still running
- Check PostgreSQL has new data:
```sql
SELECT MAX(created_at) FROM clickstream_events;
```

---

## Environment Variables Reference

| Variable | Description | Where Used |
|----------|-------------|-----------|
| `PG_PASSWORD` | PostgreSQL password | `spark_consumer.py` |
| `HADOOP_HOME` | Hadoop path | `run_spark.bat` |
| `SPARK_HOME` | Spark path | `run_spark.bat` |
| `JAVA_HOME` | Java 17 path | `run_spark.bat` |

---

## Quick Health Check

Run these to verify everything is working:

```cmd
# 1. Kafka running?
bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092

# 2. Data in PostgreSQL?
"C:\Program Files\PostgreSQL\17\bin\psql.exe" -U postgres -h localhost -d ecommerce_analytics -c "SELECT COUNT(*) FROM clickstream_events;"

# 3. Java version correct?
java -version

# 4. Python venv active?
python --version
```

---

## Operational Ownership Notes

- Kafka topic is durable — events survive broker restarts
- PostgreSQL is the system of record for processed events
- Spark checkpoints enable recovery without data loss
- Producer can be restarted safely at any time
- Power BI connects read-only via `analytics_viewer` user
