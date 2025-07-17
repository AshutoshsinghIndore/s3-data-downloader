# ☁️ Cloud S3 ETL Pipeline: YAML-Driven Sync & Download Framework

![Build Status](https://img.shields.io/badge/build-passing-brightgreen)
![Python](https://img.shields.io/badge/python-3.9+-blue)
![AWS S3](https://img.shields.io/badge/AWS-S3-orange)
![Power BI Ready](https://img.shields.io/badge/PowerBI-ready-yellowgreen)
![License](https://img.shields.io/badge/license-MIT-lightgrey)

> 🔄 A production-grade, YAML-configurable pipeline to automate secure downloads from AWS S3 using Python, with sync tracking for incremental updates and integration-ready outputs for tools like Power BI, Pandas, or SQL engines.

---

## 🚀 Why This Project?

Companies often need to automate and monitor recurring data pulls from cloud storage (S3). Manual effort is time-consuming and error-prone. This ETL pipeline:

- ✅ Automates end-to-end download from S3
- ✅ Tracks sync status via Parquet snapshot
- ✅ Supports **incremental** or **full refresh** modes
- ✅ Enables downstream use in Power BI, SQL, Pandas
- ✅ Is cloud-ready, secure, and highly extensible

---

## 🧠 Real-World Use Case

> **Domain:** Healthcare, Operations, Government M&E  
> **Scenario:** Download daily patient reports or IoT logs from S3, auto-clean them, and feed into Power BI dashboards without duplicating downloads or manual triggers.

---

## 🔧 Technologies Used

| Stack Area     | Tools / Libraries                         |
|----------------|-------------------------------------------|
| 🐍 Language     | Python 3.9+                               |
| ☁️ Cloud        | AWS S3 (Boto3)                            |
| 🔐 Security     | `.env`-based IAM key handling             |
| ⚙️ Config       | YAML-driven, modular structure            |
| 📊 Output Ready | Power BI • Pandas • SQL • Excel           |
| 🧪 Tracking     | Parquet sync-state (incremental updates)  |
| 📦 Packaging    | Virtualenv compatible                     |
| ⏱️ Performance  | Multi-threaded downloads (via `tqdm`)     |

---

## 📁 Project Structure

.
├── src/
│ ├── config_loader.py # Loads YAML config
│ ├── file_utils.py # File system helpers, sync state
│ ├── s3_manager.py # S3 logic (connect, list, download)
├── config/
│ ├── default_config.yaml # Bucket/prefix/filter settings
│ ├── config.env # AWS credentials
├── s3_downloader_pipeline.py # Main runner script
└── README.md

---

## ⚙️ How It Works

1. ✅ **Load credentials** from `.env`
2. 📑 **Read config** from `default_config.yaml`
3. 🔐 **Connect securely** to AWS S3 via Boto3
4. 📦 **List & filter objects** (by extension, name, timestamp)
5. ⬇️ **Download files** with `ThreadPoolExecutor`
6. 📊 **Save sync state** in `Parquet` for incremental runs

---

## 📄 Configuration Examples

### `config.env`
AWS_ACCESS_KEY_ID=AKIxxxxxxxxxxxx
AWS_SECRET_ACCESS_KEY=xxxxxxx
AWS_REGION=ap-south-1

### `default_config.yaml`
```yaml
s3:
  your-bucket-name:
    - logs/
    - uploads/

filters:
  include_extensions: ['.csv', '.json']
  exclude_files: ['ignore_this.csv']

sync:
  loc_download: ./downloads
  mode: incremental  # or full_refresh
  
  # Install requirements
pip install -r requirements.txt

# Run the ETL pipeline
python s3_downloader_pipeline.py

📈 Example Output
Files downloaded to: /downloads

Metadata tracked in: /downloads/sync_state.parquet

Dashboard-ready format: .csv, .json, .parquet

🧪 Future Enhancements
 Airflow / Prefect integration

 Scheduling via GitHub Actions or cron

 File validation module (schema + data profiling)

 Support for other cloud providers (GCS, Azure)

🧑‍💼 Ideal For
Data Analysts automating data ingestion

Data Engineers building ETL pipelines

BI Developers feeding cloud data into dashboards

Healthcare & GovTech projects requiring audit-ready downloads

📬 Contact
Ashutosh Singh
📧 ashutoshsinghindore@gmail.com
🔗 LinkedIn
🐙 GitHub