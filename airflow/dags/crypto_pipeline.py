from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import boto3

# הגדרות ברירת מחדל לכל ה-tasks
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# הגדרת ה-DAG
with DAG(
    dag_id='crypto_pipeline',
    default_args=default_args,
    description='Real-time crypto pipeline',
    schedule_interval='@hourly',  # רץ כל שעה
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    # Task 1 — בדיקה ש-Kafka רץ
    check_kafka = BashOperator(
        task_id='check_kafka',
        bash_command='echo "Checking Kafka..." && nc -z kafka 9092 && echo "Kafka is up!"',
    )


    # Task 2 — בדיקה ש-S3 זמין
    def check_s3():
        s3 = boto3.client('s3', region_name='eu-west-1')
        buckets = s3.list_buckets()
        bucket_names = [b['Name'] for b in buckets['Buckets']]
        print(f"S3 Buckets: {bucket_names}")
        assert 'data-pipeline-bronze-e601c2c3' in bucket_names
        print("✅ Bronze bucket exists!")

    check_s3_task = PythonOperator(
        task_id='check_s3',
        python_callable=check_s3,
    )

    # Task 3 — בדיקה שיש קבצים ב-S3
    def verify_data():
        s3 = boto3.client('s3', region_name='eu-west-1')
        response = s3.list_objects_v2(
            Bucket='data-pipeline-bronze-e601c2c3',
            Prefix='crypto-trades/'
        )
        count = response.get('KeyCount', 0)
        print(f"✅ נמצאו {count} קבצים ב-S3")
        assert count > 0, "אין קבצים ב-S3!"

    verify_data_task = PythonOperator(
        task_id='verify_data',
        python_callable=verify_data,
    )

    # סדר הביצוע
    check_kafka >> check_s3_task >> verify_data_task