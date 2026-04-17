<<<<<<< HEAD

# Snowflake CDC Pipeline (Stream + Task + MERGE)

## 🚀 Overview

This project demonstrates an end-to-end **Change Data Capture (CDC)** pipeline built using Snowflake.

It simulates a real-world data engineering workflow where incremental data changes are captured and applied automatically to a final clean dataset.

The pipeline uses:

- Internal Stage
- COPY INTO
- Streams (CDC tracking)
- Tasks (automation)
- MERGE (upsert logic)

---

## 🏗️ Architecture

---

## ⚙️ How It Works

1. CSV files are uploaded to a Snowflake internal stage
2. `COPY INTO` loads data into the RAW table
3. A STREAM tracks all changes (INSERT / UPDATE / DELETE)
4. A TASK runs automatically every 5 minutes
5. A MERGE operation updates the CLEAN table with the latest data

---

## 📊 Dataset

The dataset simulates employee records.

### Batch 1 (Initial Load)
Contains 10 employee records.

### Batch 2 (Incremental Update)
Includes:
- Updates to existing employees (salary, city changes)
- New employee records (IDs 11 and 12)

---

## 🔄 Key Features

- Incremental data processing (CDC)
- Automated pipeline execution
- Real-time change tracking
- Upsert logic using MERGE
- Scalable data architecture

---

## 🧠 Learning Outcomes

This project demonstrates:

- How Snowflake Streams track data changes
- How Tasks automate data pipelines
- How MERGE enables upsert operations
- How modern ELT pipelines work in cloud data platforms

---

## 🛠️ Tech Stack

- Snowflake
- SQL
- Data Engineering concepts (CDC, ETL/ELT)

---

## 👨‍💻 Author

Metin — Data Engineering Practice Project

---

## 📌 Note

This project is designed for learning and portfolio purposes to demonstrate real-world data pipeline architecture using Snowflake.
=======
# snowflake-cdc-stream-task-pipeline
>>>>>>> 9ef6077f98c41b7ea03b6e738a2fca2e8c2791d6
