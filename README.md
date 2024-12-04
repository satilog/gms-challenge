# **GMS Data Engineering Challenge**

## **Overview**
This repository contains the implementation of an ETL pipeline to process messy insurance claim data and prepare it for advanced analytics. The pipeline extracts data from multiple sources, cleans and transforms the data, and loads it into a PostgreSQL database. The processed data is ready for use in analytics tasks, such as fraud detection, customer retention, operational efficiency, policy optimization, and generating region-wise insights.

---

## **Folder Structure**
```plaintext
GMS-CHALLENGE/
├── data/
│   ├── csv/                # Contains input CSV files
│   ├── json/               # Contains input JSON files
├── db/
│   ├── objectives/         # Contains SQL queries for analytics objectives
│   │   ├── customer_retention.sql
│   │   ├── fraud_detection.sql
│   │   ├── operational_efficiency.sql
│   │   ├── policy_optimization.sql
│   │   ├── region_wise_insights.sql
│   ├── init.sql            # Database schema and initialization script
│   ├── other_queries.sql   # Additional SQL queries
├── etl_service/
│   ├── config/             # Configuration files
│   │   ├── etl_config.json
│   ├── src/                # ETL source code
│       ├── clean_utils.py  # Helper functions for data cleaning
│       ├── db_utils.py     # Database utilities for execution
│       ├── etl.py          # Main ETL pipeline script
│       ├── main.py         # Entry point for executing the ETL process
├── .env                    # Environment file for sensitive variables
├── .gitignore              # Files and folders to ignore in version control
├── docker-compose.yml      # Docker Compose configuration for the pipeline
├── Dockerfile              # Dockerfile for building the ETL service
├── requirements.txt        # Python dependencies
└── README.md               # Project documentation

---

## **Getting Started**

### **Requirements**
- Docker
- Docker Compose
- Python 3.8+ (if running locally)

---

### **Setup**
1. **Clone the Repository**
   ```bash
   git clone <repository-url>
   cd gms-challenge
   ```

2. **Build the Docker Containers**
   ```bash
   docker-compose up --build
   ```

3. **Run the ETL Process**
The ETL pipeline will automatically run as part of the etl_service container when started with Docker Compose.

---

### **Configuration**
The configuration for the ETL pipeline is stored in `etl_service/config/etl_config.json`:
- **Database Configuration**:
  - `host`, `port`, `name`, `user`, `password`
- **Data Files**:
  - Paths to input CSV and JSON files.
- **ETL Settings**:
  - Batch size and logging level.

---

## **ETL Pipeline**
The ETL pipeline:
1. **Extracts** data from:
   - Multiple CSV files (`claims_master`, `claims_payment`, `policy_premium`)
   - JSON files (`claim_details`)
2. **Cleans and Transforms**:
   - Dates are parsed into a consistent format.
   - Invalid regions, genders, and payment statuses are cleaned.
   - Foreign key constraints are validated, with missing references handled appropriately.
3. **Loads** the cleaned data into PostgreSQL tables:
   - `claims_master`, `claims_payment`, `policy_premium`, `claim_details`, and `procedure_codes`.

---

## **Database Schema**
The database schema is defined in `db/init.sql` and includes the following tables:
- `claims_master`
- `claims_payment`
- `policy_premium`
- `claim_details`
- `procedure_codes`

---

## **Analytics Objectives**
The following SQL scripts in the `db/objectives` directory provide insights for the key business objectives:
1. **Fraud Detection** (`fraud_detection.sql`)
2. **Customer Retention** (`customer_retention.sql`)
3. **Operational Efficiency** (`operational_efficiency.sql`)
4. **Policy Optimization** (`policy_optimization.sql`)
5. **Region-Wise Insights** (`region_wise_insights.sql`)

---