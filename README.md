# FraudWatch 🛡️
> Real-Time Fraud Detection using Big Data Analytics and Machine Learning

FraudWatch is an end-to-end distributed stream processing and fraud detection system built with Apache Flink, Apache Spark, Kafka, FastAPI, React, and Three.js.

---

## 👥 Academic Context & Contributors

- **Project:** CA1 Mini-Project & CA2 Content Beyond Syllabus
- **Topic:** Real-Time Fraud Detection using Big Data Analytics & Apache Flink True Stream Processing
- **Team Members:**
  - **Hardik Hinduja** ([@Hardik182005](https://github.com/Hardik182005))
  - **Avinash Gehi** ([@Avi36005](https://github.com/Avi36005))
  - **Siddharth Dey**
  - **Sahil Deshmukh**

---

## 🏛️ System Architecture

```
[ Transactions Generator ] ──> [ Apache Kafka ]
                                     │
                    ┌────────────────┴────────────────┐
                    ▼                                 ▼
           [ Apache Flink ]                  [ Apache Spark ]
     (Sub-millisecond Streaming)           (Micro-batch Baseline)
                    │                                 │
                    └────────────────┬────────────────┘
                                     ▼
                           [ Scoring Service ]
                     (XGBoost / LightGBM Inference)
                                     │
                    ┌────────────────┴────────────────┐
                    ▼                                 ▼
           [ PostgreSQL & Redis ]              [ SSE Stream ]
                    │                                 │
                    └────────────────┬────────────────┘
                                     ▼
                       [ FraudWatch Web Apps ]
           ┌─────────────────────────┴─────────────────────────┐
           ▼                                                   ▼
  FraudWatch Live                                     FraudWatch Lab
  (Real-Time Monitoring)                             (Academic Benchmarks)
```

---

## 🚀 Key Modules

- **`apps/live`**: Real-time fraud detection operations dashboard with 3D pipeline visualization, alerts inspector, and scenario generator.
- **`apps/lab`**: Academic companion application for CA1 / CA2 benchmarks, viva demonstration, and interactive report.
- **`packages/fwcore`**: Core feature engineering, rule engine, and transaction schemas.
- **`packages/ui`**: Shared design system, charts, tables, and 3D pipeline components.
- **`packages/contracts`**: Type-safe shared contracts, API clients, and SSE stream hooks.
- **`streaming/flink`**: Production Flink job performing stateful tumbling/sliding feature aggregation and fraud routing.
- **`streaming/spark`**: Spark Structured Streaming micro-batch baseline for latency & throughput comparisons.
- **`services/api`**: FastAPI gateway providing live stats, run management, and audit logs.
- **`services/scoring`**: High-performance ML scoring microservice.
- **`services/generator`**: Realistic transaction stream synthesizer based on IEEE-CIS patterns.

---

## 🛠️ Local Development

### Prerequisites
- Node.js 20+ & npm
- Python 3.11+ / uv
- Docker & Docker Compose
- Java 17+ & Maven (for Flink streaming engine)

### Setup
```bash
# Install workspace dependencies
npm install

# Build shared packages
npm run build:packages

# Run development servers
npm run dev:live   # FraudWatch Live dashboard
npm run dev:lab    # FraudWatch Lab companion
```
