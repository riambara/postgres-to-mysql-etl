# PostgreSQL to MySQL ETL Pipeline

ETL pipeline menggunakan Apache Airflow untuk memindahkan data dari PostgreSQL ke MySQL setiap 6 jam.

## 🚀 Fitur
- Jadwal otomatis setiap 6 jam
- Load incremental (hanya data terupdate)
- Transformasi data sesuai business rules
- Error handling dengan retry 2x
- UPSERT operations ke MySQL

## 🏗️ Teknologi
- Apache Airflow 2.5.1
- PostgreSQL 13
- MySQL 5.7
- Docker & Docker Compose

## 📁 Struktur
postgres-to-mysql-etl/
├── dags/ # DAG Airflow
├── sql/ # Schema database
├── scripts/ # Scripts
├── config/ # Konfigurasi
├── docker-compose.yml # Docker setup
└── requirements.txt # Dependencies


## 🚀 Cara Jalankan
```bash
# 1. Clone repo
git clone https://github.com/riambara/postgres-to-mysql-etl.git
cd postgres-to-mysql-etl

# 2. Start containers
docker-compose up -d

# 3. Akses Airflow: http://localhost:8080
#    Username: admin, Password: admin

# 4. Enable DAG: postgres_to_mysql_etl
