# Real-Time Fraud Detection Pipeline

A production-ready, end-to-end fraud detection system built with Apache Kafka, Spark Streaming, dbt, and Apache Airflow. This project demonstrates modern data engineering practices for real-time transaction monitoring and fraud analytics.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Docker](https://img.shields.io/badge/docker-required-blue.svg)](https://www.docker.com/)

## 🎯 Project Overview

This fraud detection pipeline processes **100 transactions per second** in real-time, applying ML-based fraud scoring, data transformations, and automated reporting. The system identifies fraudulent patterns including velocity attacks, amount anomalies, geographic impossibilities, and suspicious merchant categories.

### Key Features

- **Real-Time Processing**: Kafka + Spark Streaming for sub-second fraud detection
- **Velocity Attack Detection**: 5-minute sliding windows detect suspicious transaction bursts
- **Error Handling**: Dead Letter Queue (DLQ) and batch-level error logging
- **Automated Data Pipeline**: Airflow orchestrates hourly dbt transformations and reporting
- **Graph Analytics**: Neo4j integration for fraud ring detection using PageRank and Louvain algorithms
- **Data Quality**: 6 automated dbt tests ensure data integrity
- **Monitoring**: Prometheus + Grafana dashboards for real-time system metrics
- **Scalable Architecture**: Fully containerized with Docker Compose
- **Security**: Environment variables for credential management

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    FRAUD DETECTION PIPELINE                      │
└─────────────────────────────────────────────────────────────────┘

1. DATA INGESTION (Real-time)
   ├─ Transaction Generator (Python)
   │  └─ Generates 100 TPS → Kafka Topic: txn_raw
   │
   └─ Apache Kafka (Port 9092)
      └─ Distributed message broker

2. FRAUD SCORING (Real-time)
   └─ Spark Structured Streaming
      ├─ Reads from Kafka (txn_raw)
      ├─ Applies ML-based fraud scoring
      │  ├─ Amount anomaly detection
      │  ├─ Velocity checks
      │  ├─ Geographic patterns
      │  └─ Behavioral analysis
      ├─ Windowed Aggregations (5-min sliding windows)
      │  └─ Detects velocity attacks (>10 txns/5min)
      ├─ Error Handling
      │  ├─ Dead Letter Queue (txn_dlq)
      │  └─ Batch error logging (batch_errors table)
      └─ Writes to Postgres
         ├─ scored_transactions
         └─ velocity_alerts

3. DATA WAREHOUSE (Postgres)
   ├─ Raw Layer:
   │  ├─ scored_transactions
   │  ├─ velocity_alerts
   │  └─ batch_errors
   ├─ Staging Layer: stg_transactions (cleaned)
   └─ Mart Layer:
      ├─ fraud_summary (hourly aggregations)
      └─ account_risk_profile (user risk scoring)

4. TRANSFORMATION & VALIDATION (Hourly - Airflow)
   └─ DAG: fraud_detection_pipeline
      ├─ Task 1: Data Freshness Check
      ├─ Task 2: dbt Run (3 models)
      ├─ Task 3: dbt Test (6 validations)
      ├─ Task 4: Fraud Report Generation
      └─ Task 5: Data Cleanup (7+ days)

5. GRAPH ANALYTICS (Optional)
   └─ Neo4j (Port 7474)
      ├─ PageRank: Identify central accounts in fraud networks
      ├─ Louvain: Detect fraud ring communities
      └─ Relationship analysis: Transaction patterns between accounts

6. MONITORING
   ├─ Airflow UI (Port 8081) - Pipeline orchestration
   ├─ Spark UI (Port 8080) - Job monitoring
   ├─ Prometheus (Port 9090) - Metrics collection
   │  └─ Transaction generator metrics (Port 8000)
   └─ Grafana (Port 3000) - Visualization dashboards
      ├─ Transaction throughput (TPS)
      ├─ Fraud rate trends
      ├─ Processing latency
      └─ System resource usage
```

---

## 🚀 Quick Start

### Prerequisites

- **Docker Desktop** (with Docker Compose)
- **Python 3.9+** (for local development)
- **8GB RAM** minimum (16GB recommended)
- **20GB disk space**

### Installation

1. **Clone the repository:**

```bash
git clone https://github.com/yourusername/fraud-detection-pipeline.git
cd fraud-detection-pipeline
```

2. **Set up environment variables:**

```bash
# Copy the example environment file
cp .env.example .env

# Edit .env and update with your credentials
# For production, change all passwords!
```

**Important:** The `.env` file contains sensitive credentials and is excluded from Git via `.gitignore`.

3. **Start the entire pipeline:**

```bash
docker-compose up -d --build
```

This starts all services:
- ✅ Kafka & Zookeeper
- ✅ Postgres database
- ✅ Spark (master + worker)
- ✅ Airflow (webserver + scheduler)
- ✅ Neo4j graph database
- ✅ Prometheus + Grafana
- ✅ **Transaction Generator (auto-running at 100 TPS)**

3. **Start Spark fraud scoring:**

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.0 \
  /opt/spark-apps/fraud_detector.py
```

4. **Access the UIs:**

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8081 | From .env (default: admin / admin) |
| Spark Master | http://localhost:8080 | - |
| Neo4j Browser | http://localhost:7474 | From .env (default: neo4j / password) |
| Grafana | http://localhost:3000 | From .env (default: admin / admin) |
| Prometheus | http://localhost:9090 | - |

---

## 📊 Data Flow

### Transaction Schema

```json
{
  "txn_id": "uuid",
  "user_id": "U0001",
  "amount": 1250.50,
  "currency": "USD",
  "merchant": "Amazon",
  "merchant_category": "electronics",
  "timestamp": "2026-03-06T10:15:30Z",
  "country": "US",
  "device_id": "device_12345",
  "ip_address": "192.168.1.1",
  "is_fraudulent": false,
  "fraud_label": "legitimate"
}
```

### Fraud Scoring Logic

Transactions are scored 0-100 based on:

- **Amount Anomalies** (+40 points): Transactions >$5000
- **Suspicious Categories** (+30 points): Wire transfers, ATM withdrawals, jewelry, luxury goods
- **High-Risk Countries** (+20 points): RU, CN, NG, BR
- **Fraud Patterns** (+50 points): Velocity attacks, geographic impossibilities, round amounts

**Risk Levels:**
- `HIGH`: Score ≥ 70
- `MEDIUM`: Score 40-69
- `LOW`: Score < 40

---

## 🔧 Configuration

### Environment Variables

All sensitive credentials are stored in the `.env` file (not committed to Git). 

**Available environment variables:**

```bash
# Database Credentials
POSTGRES_USER=admin
POSTGRES_PASSWORD=your_secure_password
POSTGRES_DB=appdb

# Neo4j Credentials
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_neo4j_password

# Airflow Credentials
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=your_airflow_password
AIRFLOW_FERNET_KEY=your_fernet_key

# Kafka Configuration
KAFKA_BROKER_INTERNAL=kafka:29092
KAFKA_BROKER_EXTERNAL=localhost:9092
```

**To customize:**
1. Copy `.env.example` to `.env`
2. Update values in `.env` with your credentials
3. Restart services: `docker-compose up -d`

### dbt Configuration

Located in `fraud_detection/profiles.yml` (uses environment variables):

```yaml
fraud_detection:
  target: dev
  outputs:
    dev:
      type: postgres
      host: postgres
      port: 5432
      user: "{{ env_var('POSTGRES_USER') }}"
      password: "{{ env_var('POSTGRES_PASSWORD') }}"
      dbname: "{{ env_var('POSTGRES_DB') }}"
      schema: public
      threads: 4
```

dbt automatically reads from environment variables using Jinja2 syntax.

---

## 📈 Monitoring & Observability

### Airflow DAG Monitoring

The `fraud_detection_pipeline` DAG runs **hourly** with the following tasks:

1. **check_data_freshness**: Validates new transactions exist
2. **dbt_run**: Transforms raw data into analytics tables
3. **dbt_test**: Runs 6 data quality tests
4. **generate_fraud_report**: Logs fraud metrics and alerts
5. **cleanup_old_data**: Removes data older than 7 days

### Sample Fraud Report

```
============================================================
HOURLY FRAUD REPORT
============================================================
Hour: 2026-03-06 16:00
Total Transactions: 33,151
Fraud Transactions: 1,391
Fraud Rate: 4.20%
High Risk Count: 1,391
Avg Fraud Score: 3.79
============================================================

TOP 5 RISKY ACCOUNTS:
  F0036 | CRITICAL | Fraud Rate: 100.0% | Score: 100.00
  F0045 | CRITICAL | Fraud Rate: 100.0% | Score: 100.00
  F0043 | CRITICAL | Fraud Rate: 100.0% | Score: 100.00
```

### Data Quality Tests

All dbt tests must pass before reporting:

- ✅ `scored_transactions.txn_id` is unique
- ✅ `scored_transactions.txn_id` is not null
- ✅ `fraud_summary.summary_hour` is unique
- ✅ `fraud_summary.summary_hour` is not null
- ✅ `account_risk_profile.user_id` is unique
- ✅ `account_risk_profile.user_id` is not null

---

## 🗂️ Project Structure

```
fraud-detection-pipeline/
├── airflow/
│   ├── dags/
│   │   └── fraud_detection_pipeline.py    # Airflow DAG
│   ├── logs/
│   ├── plugins/
│   └── Dockerfile                          # Custom Airflow image
├── fraud_detection/                        # dbt project
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_transactions.sql
│   │   └── marts/
│   │       ├── fraud_summary.sql
│   │       ├── account_risk_profile.sql
│   │       └── schema.yml
│   ├── dbt_project.yml
│   └── profiles.yml
├── spark-apps/
│   └── fraud_detector.py                   # Spark streaming job
├── prometheus/
│   └── prometheus.yml                      # Prometheus config
├── grafana/
│   └── provisioning/
│       └── dashboards/
│           └── fraud-detection.json        # Grafana dashboard
├── transaction_generator.py                # Kafka producer with Prometheus metrics
├── neo4j_consumer.py                       # Neo4j graph ingest
├── neo4j_graph_analytics.py                # PageRank & Louvain algorithms
├── docker-compose.yml                      # Infrastructure
├── Dockerfile.generator                    # Transaction generator image
├── requirements.txt                        # Python dependencies
├── .env.example                            # Environment variables template
├── .gitignore                              # Git exclusions
└── README.md
```

---

## 🧪 Testing

### Verify Transaction Generation

```bash
docker logs transaction-generator --tail 20
```

Expected output:
```
✓ Connected to Kafka
🚀 Starting transaction stream at 100 TPS
[1,000 txns] Fraud: 52 (5.2%) | TPS: 110.6 | Runtime: 9s
[2,000 txns] Fraud: 101 (5.1%) | TPS: 104.9 | Runtime: 19s
```

### Check Spark Processing

```bash
docker logs spark-master --tail 30 | grep "Batch"
```

Expected output:
```
✓ Batch 0: Wrote 1000 transactions to Postgres
✓ Batch 0: Wrote 15 velocity alerts
✓ Batch 1: Wrote 1000 transactions to Postgres
✓ Batch 1: Wrote 12 velocity alerts
```

### Check Velocity Alerts

```bash
docker exec -it postgres psql -U admin -d appdb -c \
  "SELECT user_id, txn_count, window_start FROM velocity_alerts ORDER BY txn_count DESC LIMIT 10;"
```

Expected output:
```
 user_id | txn_count |    window_start     
---------+-----------+---------------------
 U0792   |        46 | 2026-03-07 15:28:00
 F0045   |        43 | 2026-03-07 15:28:00
```

### Check Prometheus Metrics

```bash
curl http://localhost:8000/metrics
```

Or visit: http://localhost:8000/metrics

### Query Transformed Data

```bash
docker exec -it postgres psql -U admin -d appdb -c \
  "SELECT COUNT(*) FROM public_marts.fraud_summary;"
```

### Trigger Airflow DAG Manually

```bash
docker exec -it airflow-webserver airflow dags trigger fraud_detection_pipeline
```

---

## 🛠️ Development

### Local Development Setup

```bash
# Install Python dependencies
pip install -r requirements.txt

# Run transaction generator locally
python transaction_generator.py

# Run dbt models locally
cd fraud_detection
dbt run --profiles-dir .
dbt test --profiles-dir .
```

### Adding New Fraud Rules

Edit `spark-apps/fraud_detector.py` and modify the `calculate_fraud_score` UDF:

```python
@udf(returnType=IntegerType())
def calculate_fraud_score(amount, merchant_category, country, fraud_label):
    score = 0
    
    # Add your custom rules here
    if amount > 10000:
        score += 50
    
    return min(score, 100)
```

### Adding New dbt Models

1. Create SQL file in `fraud_detection/models/marts/`
2. Add tests in `schema.yml`
3. Run: `dbt run --select your_model_name`

---

## 📊 Performance Metrics

### System Throughput

- **Transaction Ingestion**: 100 TPS (configurable)
- **Spark Processing**: ~1000 transactions per 10-second batch
- **Velocity Detection**: 5-minute sliding windows with 1-minute slide interval
- **End-to-End Latency**: < 15 seconds (ingestion → scored in Postgres)
- **dbt Transformation**: ~6 seconds for 40K+ transactions
- **Alert Generation**: Real-time velocity alerts for >10 txns/5min

### Resource Usage

- **CPU**: 4-6 cores recommended
- **Memory**: 8GB minimum, 16GB recommended
- **Disk I/O**: ~500MB/hour for logs and data
- **Network**: Minimal (all services on same Docker network)

---

## 🔒 Security Considerations

✅ **Security Features Implemented:**

- **Environment Variables**: All credentials stored in `.env` file (excluded from Git)
- **No Hardcoded Secrets**: All sensitive data uses environment variables
- **Gitignore Protection**: `.env` file automatically excluded from version control

⚠️ **For Production Deployment:**

1. **Change all passwords** in `.env` file (use strong, unique passwords)
2. **Generate new Fernet key** for Airflow:
   ```python
   from cryptography.fernet import Fernet
   print(Fernet.generate_key().decode())
   ```
3. **Enable SSL/TLS** for Kafka, Postgres, and Airflow
4. **Implement authentication** for all web UIs
5. **Use secrets management** (e.g., HashiCorp Vault, AWS Secrets Manager)
6. **Enable network policies** and firewall rules
7. **Implement audit logging** for all data access
8. **Encrypt data at rest** in Postgres and Kafka
9. **Never commit `.env`** to version control

---

## 🐛 Troubleshooting

### Transaction Generator Not Running

```bash
# Check logs
docker logs transaction-generator --tail 50

# Restart container
docker restart transaction-generator
```

### Spark Job Fails to Connect to Kafka

Ensure Kafka is using the correct internal port (`kafka:29092`):

```python
.option("kafka.bootstrap.servers", "kafka:29092")
```

### Airflow DAG Fails with Password Error

Update the `postgres_default` connection:

```bash
docker exec -it airflow-webserver airflow connections delete postgres_default
docker exec -it airflow-webserver airflow connections add 'postgres_default' \
  --conn-type 'postgres' --conn-host 'postgres' --conn-schema 'appdb' \
  --conn-login 'admin' --conn-password 'admin' --conn-port 5432
```

### dbt Connection Issues

Verify `fraud_detection/profiles.yml` uses `host: postgres` (not `localhost` or `127.0.0.1`).

---

## 📚 Technologies Used

| Technology | Version | Purpose |
|------------|---------|---------|
| Apache Kafka | 7.5.0 | Message streaming |
| Apache Spark | 3.5.0 | Real-time processing |
| PostgreSQL | 15 | Data warehouse |
| Apache Airflow | 2.8.0 | Workflow orchestration |
| dbt | 1.8.2 | Data transformation |
| Neo4j | 5 | Graph analytics |
| Prometheus | latest | Metrics collection |
| Grafana | latest | Visualization |
| Python | 3.9 | Application logic |
| Docker | latest | Containerization |

---

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 👤 Author

**Krish Naresh Patil**
- GitHub: [@krish1700](https://github.com/krish1700)
- LinkedIn: [Your LinkedIn](https://www.linkedin.com/in/krish-naresh-patil/)

---

## 🙏 Acknowledgments

- Inspired by real-world fraud detection systems at fintech companies
- Built as a demonstration of modern data engineering best practices
- Special thanks to the Apache Software Foundation for Kafka, Spark, and Airflow

---
