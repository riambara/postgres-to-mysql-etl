# PostgreSQL to MySQL ETL Pipeline

ETL pipeline using Apache Airflow to move data from PostgreSQL to MySQL every 6 hours.

## Setup
1. Run: `docker-compose up -d`
2. Access Airflow UI: http://localhost:8080
3. Login: admin/admin
4. Enable DAG: `postgres_to_mysql_etl`