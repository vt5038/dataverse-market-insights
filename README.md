# 🧠 Dataverse Market Insights

**Dataverse Market Insights** is a production-grade **data engineering pipeline** that collects and unifies real-time data from multiple global market sources — **Crypto**, **Stocks**, and **Forex** — into a centralized **Data Lake architecture** (Bronze → Silver → Gold).  
It demonstrates the complete lifecycle of a data engineering project: **ingestion, validation, storage, orchestration, and analytics**.

---

## 🎯 Project Objectives
- Design a **scalable, modular data pipeline** using modern data engineering practices.  
- Extract and validate data from multiple APIs:
  - 🪙 **CoinGecko API** → Cryptocurrency market data  
  - 📈 **Alpha Vantage API** → Global stock market data  
  - 💱 **ExchangeRate.host API** → Currency exchange rates  
- Store and organize datasets into **structured AWS S3 layers**.  
- Enable transformation and querying via **Glue** and **Athena**.  
- Automate and monitor the entire process with **Apache Airflow**.  
- Monitor pipeline performance and failures with **AWS CloudWatch**.
- Enable visualization and trend analysis through **AWS QuickSight** or **Power BI**, showcasing unified insights across crypto, stock, and forex data.

Apply best practices of data engineering — modular design, fault tolerance, schema validation, retry logic, and logging to simulate a real-world enterprise data ecosystem.

---

## ⚙️ Tech Stack

| Category | Tools & Technologies |
|-----------|---------------------|
| **Programming Language** | Python 3.10+ |
| **Core Libraries** | `requests`, `pandas`, `tenacity`, `python-dotenv`, `boto3` |
| **Cloud Platform** | AWS (S3, Glue, Athena, CloudWatch) |
| **Orchestration** | Apache Airflow |
| **Version Control** | Git / GitHub |
| **Visualization (optional)** | Plotly / Power BI |
| **Environment** | Virtual Environment (`venv`) |

---

## 🧩 Data Architecture

                  ┌──────────────────────────────────────────────┐
                │                Data Sources                   │
                │──────────────────────────────────────────────│
                │  🪙 CoinGecko API  → Crypto market data       │
                │  📈 Alpha Vantage API → Stock data            │
                │  💱 ExchangeRate.host → Forex data            │
                └──────────────────────────────────────────────┘
                                      │
                                      ▼
                        ┌──────────────────────────┐
                        │  Ingestion & Orchestration│
                        │──────────────────────────│
                        │  🧠 Apache Airflow (MWAA) │→ Schedules API extraction jobs
                        │  ⚡ AWS Lambda             │→ Triggers Glue jobs on S3 upload
                        └──────────────────────────┘
                                      │
                                      ▼
                     ┌────────────────────────────────┐
                     │        AWS S3 Data Lake         │
                     │────────────────────────────────│
                     │  🟤 Bronze  → Raw API responses │
                     │  ⚪ Silver  → Cleaned datasets  │
                     │  🟡 Gold    → Aggregated metrics│
                     └────────────────────────────────┘
                                      │
                                      ▼
                     ┌────────────────────────────────┐
                     │        AWS Glue Services        │
                     │────────────────────────────────│
                     │ 🧩 Glue Crawlers → Detect new data & update catalog   │
                     │ 🔄 Glue Jobs → Transform Bronze → Silver → Gold       │
                     │ 🗂️ Glue Data Catalog → Stores schemas & partitions    │
                     └────────────────────────────────┘
                                      │
                                      ▼
                     ┌────────────────────────────────┐
                     │         AWS Athena              │
                     │────────────────────────────────│
                     │ Serverless SQL queries on S3 data│
                     │ Joins crypto, stock, forex data  │
                     │ Exposes Gold-layer tables to BI  │
                     └────────────────────────────────┘
                                      │
                                      ▼
                     ┌────────────────────────────────┐
                     │     Monitoring & Observability  │
                     │────────────────────────────────│
                     │ 🪶 AWS CloudWatch Logs → Track events & errors        │
                     │ 📊 CloudWatch Metrics → Monitor job success/duration  │
                     │ 🔔 CloudWatch Alarms → Alert on failures or delays    │
                     └────────────────────────────────┘
                                      │
                                      ▼
                     ┌────────────────────────────────┐
                     │   Visualization & Analytics     │
                     │────────────────────────────────│
                     │ 📈 AWS QuickSight / Power BI     │
                     │ Dashboards for market insights   │
                     │ Trend analysis (crypto vs forex) │
                     └────────────────────────────────┘


---

## 🧠 Key Features

- ✅ **Retry Logic & Fault Tolerance** — automatic exponential retries on failed API calls.  
- ✅ **Structured Logging** — every extraction run is timestamped and logged.  
- ✅ **Schema Validation** — prevents malformed or incomplete data from being saved.  
- ✅ **Data Layering** — raw → cleaned → analytical structure (Bronze/Silver/Gold).  
- ✅ **Scalable AWS Integration** — data pushed to S3, queried via Athena.  
- ✅ **Extensible Design** — easily add more APIs or orchestrate via Airflow.  

---

## 🚀 Quick Start

### 1️⃣ Setup Environment
```bash
git clone https://github.com/<your-username>/dataverse-market-insights.git
cd dataverse-market-insights
python -m venv venv
venv\Scripts\activate       # or source venv/bin/activate (Mac/Linux)
pip install -r requirements.txt

2️⃣ Run First Extractor (Crypto)
python -m src.extractors.extract_coingecko

3️⃣ Output Example
2025-10-08 12:15:02 | INFO | coingecko_extractor | ✅ Saved snapshot: data/bronze/crypto/coingecko_20251008T121502Z.csv (rows=25)

📂 Project Structure

dataverse-market-insights/
 ├── src/
 │   ├── extractors/      → API extractors (crypto, stocks, forex)
 │   └── utils/           → helpers (logging, validation)
 ├── data/
 │   ├── bronze/          → raw API data
 │   ├── silver/          → cleaned & validated data
 │   └── gold/            → aggregated data for analytics
 ├── notebooks/           → analysis & visualization
 ├── tests/               → unit tests
 ├── tmp/                 → temporary/intermediate files
 ├── .env.example         → environment variables template
 ├── requirements.txt     → dependencies
 ├── README.md            → project documentation
 └── venv/                → virtual environment

📊 Example Insights (Future Scope)

Track crypto-to-stock correlation over time

Compare Bitcoin vs USD strength using forex data

Build dashboards on AWS QuickSight or Power BI

👨‍💻 Author

 Jayanth — Data Engineer passionate about building scalable data platforms and cloud-native data pipelines.