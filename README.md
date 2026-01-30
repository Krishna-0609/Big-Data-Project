# 🏪 Big Mart Sales Analysis using PySpark

## 📌 Project Overview
This project demonstrates an **end-to-end Big Data ETL pipeline** built using **PySpark**, **HDFS**, and **Hive** to analyze Big Mart sales data.  
The pipeline ingests raw CSV data, performs large-scale data cleaning and transformations, applies aggregations and window functions, optimizes Spark jobs, and stores the processed data in **Hive tables using Parquet format**.

This project simulates **real-world data engineering workflows** and processes **1M+ records**.

---

## 🛠️ Technologies Used
- PySpark
- Apache Spark
- HDFS
- Apache Hive
- Spark SQL
- Parquet
- Linux
- VS Code
- Git & GitHub
- Databricks (Optional)

---

## 📂 Project Structure
bigmart-pyspark-project/
│
├── data/
│ └── raw/
│ └── Train.csv
│
├── spark/
│ ├── ingest_data.py
│ ├── data_cleaning.py
│ ├── transformations.py
│ └── aggregations.py
│
├── hive/
│ └── create_tables.sql
│
├── output/
│ └── parquet/
│
├── README.md
└── requirements.txt'


---

## 📊 Dataset
- **Dataset Name:** Big Mart Sales Dataset  
- **Source:** Kaggle  
- **Link:** https://www.kaggle.com/datasets/brijbhushannanda1979/bigmart-sales-data  

To simulate **large-scale processing**, the dataset was duplicated using PySpark to generate **1M+ records**.

---

## ⚙️ Setup Instructions

### 1️⃣ Start Hadoop & Hive Services
```bash
start-dfs.sh
hive --service metastore &
hive --service hiveserver2 &
