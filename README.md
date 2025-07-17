# ☁️ Cloud S3 ETL Pipeline: YAML-Driven Sync & Download Framework

![Python](https://img.shields.io/badge/python-3.9+-blue)
![AWS S3](https://img.shields.io/badge/AWS-S3-orange)
![Power BI Ready](https://img.shields.io/badge/PowerBI-ready-yellowgreen)
![ETL](https://img.shields.io/badge/ETL-Cloud%20Pipeline-brightgreen)
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
```
Cloud-ETL-S3-Pipeline/
├── config/
│ ├── config.env # 🔐 AWS credentials (use with dotenv)
│ └── default_config.yaml # ⚙️ YAML-based S3 bucket and sync settings
│ └── config.env.txt # Dummy config.env file
│
├── downloads/ # 📁 Local download directory (auto-created)
│ └── sync_state.parquet # 🧠 Parquet file to track incremental syncs
│
├── src/ # 🧠 Core logic modules
│ ├── config_loader.py # 🗂️ Reads and parses YAML config
│ ├── file_utils.py # 📄 File discovery, sync-state writing
│ └── s3_manager.py # ☁️ S3 connection, filtering, multi-threaded download
│
├── s3_downloader_pipeline.py # 🚀 Main entry script to execute pipeline
├── requirements.txt # 📦 Project dependencies
├── README.md # 📘 This file
└── .gitignore # 🚫 Ignore credentials, downloads, cache
```
---

## ⚙️ How It Works

1. ✅ **Load credentials** from `src/config.env`
2. 📑 **Read config** from `default_config.yaml`
3. 🔐 **Connect securely** to AWS S3 via Boto3
4. 📦 **List & filter objects** (by extension, name, timestamp)
5. ⬇️ **Download files** with `ThreadPoolExecutor`
6. 📊 **Save sync state** in `Parquet` for incremental runs

---

## 📄 Configuration Examples

### `config.env`
```yaml
AWS_ACCESS_KEY_ID=AKIxxxxxxxxxxxx
AWS_SECRET_ACCESS_KEY=xxxxxxx
AWS_REGION=ap-south-1
```

### `default_config.yaml`
```yaml
# 📦 S3 Configuration
s3:
  prod-completed:                # (REQUIRED) S3 bucket name (e.g., healthcare-data-bucket)
    - 2025-05-30/                # Prefix/folder inside bucket. Must end with '/'.
    - 2025-03-01/                # Add more prefixes as needed.

# 🔄 Sync Mode Configuration
sync:
  loc_download: ../downloads     # (REQUIRED) Local directory to save downloaded files.
  mode: incremental              # Options:
                                 #   - full_refresh: Download all files
                                 #   - incremental: Only new/updated files
                                 #   - mirror: Match S3 exactly (delete local extras)
  threads: 8                     # Number of parallel download threads (default: 12)

# 🎯 File Filters
filters:
  include_extensions:            # Only download files with these extensions
    - .mp4
    - .xlsx
  exclude_files:                 # Skip these exact filenames (optional)
    - skip_this_file.csv
```

```bash
# Install requirements
pip install -r requirements.txt

# Run the ETL pipeline
python s3_downloader_pipeline.py
```

## 🧪 Future Enhancements
- Integration with Airflow or Prefect for orchestration  
- Scheduling support via GitHub Actions or cron  
- File validation module with schema checks and data profiling  
- Compatibility with additional cloud providers (GCS, Azure)

## 🧑💼 Ideal For
- **Data Analysts** automating ingestion from S3  
- **Data Engineers** building scalable ETL pipelines  
- **BI Developers** integrating cloud data into dashboards  
- **Healthcare & GovTech teams** needing audit-ready data downloads

## 📬 Contact

**Ashutosh Singh**  
📧 [ashutoshsinghindore@gmail.com](mailto:ashutoshsinghindore@gmail.com)  
🔗 [LinkedIn](https://linkedin.com/in/ashutoshsinghindore)  
🐙 [GitHub](https://github.com/AshutoshsinghIndore)
