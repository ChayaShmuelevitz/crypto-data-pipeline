from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
import boto3

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='crypto_pipeline',
    default_args=default_args,
    description='Real-time crypto pipeline with dbt',
    schedule_interval='@hourly',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    check_kafka = BashOperator(
        task_id='check_kafka',
        bash_command='nc -z kafka 9092',
    )

    def check_bronze():
        s3 = boto3.client('s3', region_name='eu-west-1')
        response = s3.list_objects_v2(
            Bucket='data-pipeline-bronze-e601c2c3',
            Prefix='spark-streaming/',
            MaxKeys=1
        )
        count = response.get('KeyCount', 0)
        print(f"✅ Found {count} files in Bronze")
        assert count > 0, "No data in Bronze!"

    check_bronze_task = PythonOperator(
        task_id='check_bronze',
        python_callable=check_bronze,
    )

    run_dbt_task = DockerOperator(
        task_id='run_dbt',
        image='data-pipeline-dbt',
        container_name='airflow_dbt_run',
        api_version='auto',
        auto_remove=True,
        command='bash -c "cd /dbt/crypto_transformations && dbt run"',
        docker_url='unix://var/run/docker.sock',
        network_mode='data-pipeline_default',
        environment={
            'AWS_ACCESS_KEY_ID': '{{ var.value.aws_access_key_id }}',
            'AWS_SECRET_ACCESS_KEY': '{{ var.value.aws_secret_access_key }}',
            'AWS_DEFAULT_REGION': 'eu-west-1',
        },
        mount_tmp_dir=False,
    )

    test_dbt_task = DockerOperator(
        task_id='test_dbt',
        image='data-pipeline-dbt',
        container_name='airflow_dbt_test',
        api_version='auto',
        auto_remove=True,
        command='bash -c "cd /dbt/crypto_transformations && dbt test || true"',
        docker_url='unix://var/run/docker.sock',
        network_mode='data-pipeline_default',
        environment={
            'AWS_ACCESS_KEY_ID': '{{ var.value.aws_access_key_id }}',
            'AWS_SECRET_ACCESS_KEY': '{{ var.value.aws_secret_access_key }}',
            'AWS_DEFAULT_REGION': 'eu-west-1',
        },
        mount_tmp_dir=False,
    )

    def verify_gold():
        s3 = boto3.client('s3', region_name='eu-west-1')
        response = s3.list_objects_v2(
            Bucket='data-pipeline-gold-e601c2c3',
            Prefix='crypto_db/',
            MaxKeys=1
        )
        count = response.get('KeyCount', 0)
        print(f"✅ Found {count} files in Gold layer")
        assert count > 0, "No data in Gold!"


    verify_gold_task = PythonOperator(
        task_id='verify_gold',
        python_callable=verify_gold,
    )

    check_kafka >> check_bronze_task >> run_dbt_task >> test_dbt_task >> verify_gold_task