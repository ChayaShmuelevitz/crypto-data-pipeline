\# Real-Time Crypto Data Pipeline



End-to-end real-time data pipeline processing cryptocurrency trades from Binance WebSocket API.



\## Architecture

```

Binance API → Kafka → Spark Streaming → S3 Bronze (Parquet)

&nbsp;                                           ↓

&nbsp;                                      Athena queries

&nbsp;                                           ↓

&nbsp;                                   dbt transformations

&nbsp;                                           ↓

&nbsp;                                 S3 Gold (Parquet tables)

&nbsp;                                           ↓

&nbsp;                                   Airflow orchestration

```



\## Tech Stack



\- \*\*Streaming\*\*: Apache Kafka, Spark Structured Streaming

\- \*\*Storage\*\*: AWS S3 (Medallion Architecture: Bronze/Silver/Gold)

\- \*\*Query Engine\*\*: AWS Athena, AWS Glue Catalog

\- \*\*Transformation\*\*: dbt (Data Build Tool)

\- \*\*Orchestration\*\*: Apache Airflow

\- \*\*Infrastructure\*\*: Docker Compose, Terraform

\- \*\*Language\*\*: Python



\## Data Flow



1\. \*\*Producer\*\*: Python WebSocket consumer → Kafka topic `crypto-trades`

2\. \*\*Spark Streaming\*\*: Reads from Kafka every 30s → Writes Parquet to S3 Bronze

3\. \*\*dbt\*\*: 

&nbsp;  - Silver: `stg\_trades` view (cleaned data)

&nbsp;  - Gold: `agg\_trades\_hourly` table (hourly aggregations)

4\. \*\*Data Quality\*\*: 9 dbt tests (not\_null, unique, accepted\_values)

5\. \*\*Airflow DAG\*\*: Monitors Kafka → Bronze → dbt run → dbt test → Gold



\## Project Structure

```

data-pipeline/

├── producer/          # Binance WebSocket → Kafka

├── spark/            # Spark Streaming → S3 Bronze

├── dbt/              # dbt transformations (Silver/Gold)

├── airflow/          # Orchestration DAGs

├── terraform/        # AWS infrastructure (S3 buckets)

└── docker-compose.yml

```



\## Prerequisites



\- Docker Desktop

\- AWS CLI configured

\- AWS account (S3, Athena, Glue)



\## Setup



1\. \*\*Clone repository\*\*

```bash

git clone <your-repo>

cd data-pipeline

```



2\. \*\*Configure AWS credentials\*\*

```bash

aws configure

```



3\. \*\*Create S3 buckets with Terraform\*\*

```bash

cd terraform

terraform init

terraform apply

```



4\. \*\*Start services\*\*

```bash

docker-compose up -d

```



5\. \*\*Access Airflow\*\*

\- URL: http://localhost:8081

\- Username: `admin`

\- Password: `admin`



6\. \*\*Trigger DAG\*\*

\- Navigate to `crypto\_pipeline` DAG

\- Click "Trigger DAG"



\## Data Quality Tests



9 automated tests ensure data integrity:

\- ✅ No nulls in critical columns

\- ✅ Unique hour/symbol combinations

\- ✅ Valid symbol values (BTCUSDT, ETHUSDT, SOLUSDT)



\## AWS Resources



\- \*\*Bronze\*\*: `s3://data-pipeline-bronze-<id>/spark-streaming/`

\- \*\*Gold\*\*: `s3://data-pipeline-gold-<id>/crypto\_db/agg\_trades\_hourly/`

\- \*\*Database\*\*: `crypto\_db` (AWS Glue Catalog)



\## Monitoring



\- \*\*Kafka UI\*\*: http://localhost:8080

\- \*\*Airflow\*\*: http://localhost:8081



\## Key Features



✅ Real-time streaming ingestion  

✅ Medallion architecture (Bronze/Gold)  

✅ Data quality testing  

✅ Automated orchestration  

✅ Infrastructure as Code  

✅ Containerized deployment  



\## Future Enhancements



\- Grafana dashboards

\- Great Expectations for advanced data quality

\- CI/CD pipeline (GitHub Actions)

\- Kubernetes deployment



\## Author



Chaya Sara Shmuelevitz - Data Engineer

