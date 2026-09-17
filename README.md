# FraudWatch 🛡️
> **Real-Time Fraud Detection using Big Data Analytics and Machine Learning**  
> *Academic CA1 Mini-Project & CA2 Content Beyond Syllabus (Group 4)*

---

## 👥 Academic Context & Contributors

- **Course Project:** Big Data Analytics & Distributed Systems
- **CA1 Topic:** Real-Time Fraud Detection using Big Data Analytics & Machine Learning
- **CA2 Topic (Beyond Syllabus):** Apache Flink: True Stream Processing vs. Apache Spark Structured Streaming Micro-Batch
- **Submission Date:** September 2026
- **Team Members:**
  - **Hardik Hinduja** ([@Hardik182005](https://github.com/Hardik182005)) — *Architecture, Web Applications & GCP Deployment*
  - **Avinash Gehi** ([@Avi36005](https://github.com/Avi36005)) — *Streaming Pipelines (Flink/Spark), ML Engine & Integration*
  - **Siddharth Dey** — *Data Analysis, Benchmarking & Evaluation*
  - **Sahil Deshmukh** — *Infrastructure, Cloud Setup & Security Operations*

---

## 🏛️ System Architecture

```
                                [ IEEE-CIS Benchmark Dataset ]
                                              │
                                              ▼
                                   [ Replay / Generator ]
                                              │
                                              ▼
                                      [ Apache Kafka ]
                                       (Events Bus)
                                              │
                     ┌────────────────────────┴────────────────────────┐
                     ▼                                                 ▼
            [ Apache Flink ]                                  [ Apache Spark ]
     • Stateful Event-Time Windows                     • Micro-Batch Baseline (1s - 5s)
     • Sliding Feature Aggregations                    • Batch Window Aggregations
     • Sub-Millisecond Processing                      • Comparative Latency / Throughput
                     │                                                 │
                     └────────────────────────┬────────────────────────┘
                                              ▼
                                    [ Scoring Service ]
                             • LightGBM Stream Classifier (v1.1.0)
                             • Rule Engine Policy Enforcement
                                              │
                     ┌────────────────────────┴────────────────────────┐
                     ▼                                                 ▼
            [ PostgreSQL & Redis ]                              [ SSE Stream ]
       (Audit Trail, Runs & States)                        (Live Scored Events)
                     │                                                 │
                     └────────────────────────┬────────────────────────┘
                                              ▼
                                 [ FastAPI Gateway Service ]
                                              │
                     ┌────────────────────────┴────────────────────────┐
                     ▼                                                 ▼
           [ FraudWatch Live ]                                [ FraudWatch Lab ]
       • Real-Time Monitoring                             • Academic Benchmarks (CA1/CA2)
       • Interactive 3D Pipeline                          • Flink vs. Spark Evaluation
       • Alert Inspector & Scenarios                      • Viva Demonstration Mode
```

---

## 🔬 Dataset & Machine Learning

- **Benchmark Dataset:** **IEEE-CIS Fraud Detection**, provided by Vesta Corporation on Kaggle (`ieee-fraud-detection`).
- **Input Stream:** Replayed historical IEEE-CIS transactions preserving event-time chronology, transaction amounts, device characteristics, and anonymized categorical features.
- **Model:** **LightGBM Classifier** (`fw-ieee-stream-1.1.0`) with exact cross-language feature parity:
  - Streaming features: Rolling transaction frequency, rolling amount sum/mean, velocity, and delta from mean.
  - Zero target leakage: Labels are kept outside inference requests and joined only in the evaluation pipeline.
- **Evaluation Metrics:** ROC-AUC, PR-AUC, F1-score, Precision/Recall trade-offs, and processing latency percentiles ($P_{50}$, $P_{95}$, $P_{99}$).

---

## 🌐 Dual Web Applications

The system exposes two synchronized web interfaces sharing server-owned state via `demo_session_id` and `run_id`:

1. **FraudWatch Live (`apps/live`)**
   - **Live Overview:** High-throughput streaming dashboard displaying real-time metrics, flagged transactions, and observed pipeline latency.
   - **Interactive 3D Pipeline:** Three.js / React Three Fiber isometric pipeline visualization tracing events through Kafka, Flink/Spark, and Scoring.
   - **Inspector & Alerts:** Detailed breakdown of model probability, rule contributions, and operator review decisions.
   - **Scenario Injection:** Test late-arriving events, duplicate delivery, velocity bursts, and card-testing patterns.

2. **FraudWatch Lab (`apps/lab`)**
   - **CA1 Explorer:** Problem definition, dataset distribution, feature engineering, and model validation curves.
   - **CA2 Benchmark Suite:** True stream processing (Flink) vs. micro-batching (Spark) measured under variable load and out-of-order event scenarios.
   - **Viva Demonstration Mode:** Interactive concept cards, architecture questions, and technical talking points for project presentation.
   - **Evidence & Provenance:** Reproducible benchmark runs, downloadable JSON/CSV traces, and configuration manifests.

---

## 📁 Repository Structure

```
FraudWatch/
├── apps/
│   ├── live/                    # FraudWatch Live monitoring dashboard (React + Vite)
│   └── lab/                     # FraudWatch Lab academic companion (React + Vite)
├── packages/
│   ├── contracts/               # Shared TypeScript schemas, API client & SSE hooks
│   ├── fwcore/                  # Python core: feature specs, schema validation, replay engine
│   └── ui/                      # Shared UI design system, 3D scenes & visualization charts
├── services/
│   ├── api/                     # FastAPI gateway & SSE broadcaster
│   ├── controller/              # Orchestrator for benchmark runs and scenario injection
│   ├── generator/               # Realistic IEEE-CIS event replay & transaction streamer
│   ├── scoring/                 # High-performance ML scoring service (LightGBM)
│   ├── result-consumer/         # Sink consumer storing scored events to PostgreSQL
│   └── common/                  # Shared database models, migrations & logging
├── streaming/
│   ├── flink/                   # Apache Flink stateful streaming job (Java 17 / Maven)
│   └── spark/                   # Apache Spark Structured Streaming job (PySpark)
├── infra/
│   ├── compose/                 # Local Docker Compose configurations
│   └── gcp/                     # Google Cloud deployment automation scripts
├── ml/
│   ├── artifacts/               # Packaged model artifacts (v1.1.0) & metadata
│   ├── training/                # Training scripts for baseline and stream models
│   └── evaluation/              # Metrics calculation and report exports
└── docs/                        # API contracts, system architecture & deployment guides
```

---

## ☁️ Google Cloud Platform Deployment

The production demo is deployed on **Google Cloud Platform** (Project ID: `fraudwatch-508912`, Region: `asia-south1`):

- **Compute Engine (e2-standard-4):** Hosts the streaming backbone (Kafka, Zookeeper, Flink JobManager/TaskManager, Redis, and PostgreSQL).
- **Cloud Run / Nginx:** Serves the frontend applications (`fraudwatch-live` and `fraudwatch-lab`) with unified TLS and CORS headers.
- **Automation Scripts:** Located in [`infra/gcp/`](file:///e:/All%20projects/FraudWatch/FraudWatch/infra/gcp/) for zero-touch bootstrapping and teardown.

---

## 🚀 Quickstart & Local Setup

### Prerequisites
- **Node.js:** v20+ and npm
- **Python:** 3.11+ / `uv`
- **Docker:** Docker Desktop with Compose V2
- **Java:** JDK 17+ and Maven (for Flink streaming engine)

### 1. Install Dependencies
```bash
# Install root & workspace packages
npm install

# Build shared TypeScript libraries
npm run build:packages
```

### 2. Run with Docker Compose
```bash
# Start full distributed pipeline (Kafka, Flink, Postgres, Redis, Services)
docker compose -f infra/compose/compose.yaml up -d
```

### 3. Run Frontend Applications
```bash
# Start FraudWatch Live (http://localhost:5173)
npm run dev:live

# Start FraudWatch Lab (http://localhost:5174)
npm run dev:lab
```

### 4. Run Integration & Parity Tests
```bash
# Verify feature parity between Java Flink and Python inference
pytest tests/integration/smoke_scoring.py
pytest tests/streaming/parity_integration.py
```

---

## 📄 License & Academic Integrity

This project is developed solely for academic evaluation under the Big Data Analytics and Machine Learning coursework. IEEE-CIS dataset terms provided by Vesta Corporation apply to all Kaggle competition artifacts.
