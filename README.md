# Databricks End-to-End Data Engineering Project
## Flight Booking Analytics - Medallion Architecture

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=Databricks&logoColor=white)](https://databricks.com/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io/)

A complete production-grade data engineering pipeline built on Databricks, implementing the Medallion Architecture (Bronze → Silver → Gold) for flight booking analytics.

## 🎯 Project Overview

This project demonstrates modern data engineering best practices using Databricks and Unity Catalog:

* **Incremental Data Ingestion**: Auto Loader for streaming CSV ingestion
* **Medallion Architecture**: Three-layer data transformation (Bronze/Silver/Gold)
* **Dimensional Modeling**: Star schema with fact and dimension tables
* **Change Data Capture (CDC)**: Incremental updates using modifiedDate timestamps
* **Automated Orchestration**: Multi-task Databricks Job with dependency management
* **Data Quality**: Validation rules and schema evolution

## 🚀 Quick Start

1. Clone this repository into your Databricks workspace
2. Upload CSV files to `/Volumes/workspace/raw/rawvolume/rawdata/`
3. Run the "End to End Data Pipeline - Medallion Architecture" job
4. Query the star schema in `workspace.gold.*`

## 📊 Architecture

```
RAW DATA (CSV Files in UC Volumes)
          ↓
    BRONZE LAYER
    • Auto Loader streaming ingestion
    • Schema evolution & rescue
    • Delta Lake storage
          ↓
    SILVER LAYER
    • Data type casting
    • Quality rules (NOT NULL)
    • CDC timestamp tracking
          ↓
     GOLD LAYER
    • Star schema (Dimensions + Fact)
    • Surrogate keys (SCD Type 1)
    • Incremental MERGE (upsert)
          ↓
    ANALYTICS & BI
```

## 🛠️ Technologies

* **Databricks** - Unified analytics platform
* **Unity Catalog** - Data governance
* **Delta Lake** - ACID transactions, MERGE operations
* **Auto Loader** - Incremental file ingestion
* **PySpark** - Distributed processing
* **Serverless Compute** - Auto-scaling

## 📁 Project Structure

```
├── BronzeLayer.ipynb          # Auto Loader ingestion
├── SilverNotebook.ipynb       # Data transformations
├── GOLD_DIMS.ipynb            # Dimension tables (parameterized)
├── GOLD_FACT                  # Fact table
├── SrcParameters.ipynb        # Source parameters
├── Setup.ipynb                # Initial setup
├── DLT/
│   └── dltPipeline.py        # Alternative DLT pipeline
├── PROJECT_DOCUMENTATION.md   # Detailed docs
└── README.md                  # This file
```

## 📈 Sample Query

```sql
SELECT 
    f.airline,
    SUM(fb.amount) as total_revenue,
    COUNT(*) as booking_count
FROM workspace.gold.factbookings fb
JOIN workspace.gold.dimflights f ON fb.DimFlightsKey = f.DimFlightsKey
GROUP BY f.airline
ORDER BY total_revenue DESC;
```

## 📚 Documentation

See [PROJECT_DOCUMENTATION.md](./PROJECT_DOCUMENTATION.md) for:
* Complete architecture details
* Layer specifications
* Job orchestration setup
* Troubleshooting guide

## 🎓 Learning Outcomes

✅ Medallion Architecture  
✅ Auto Loader & streaming ingestion  
✅ Delta Lake MERGE operations  
✅ Dimensional modeling (star schema)  
✅ SCD Type 1 dimensions  
✅ CDC patterns  
✅ Databricks Jobs orchestration  
✅ Unity Catalog governance  

---

⭐ **Star this repo** if you found it helpful!

**Created**: August 2026 | **Platform**: Databricks with Unity Catalog
