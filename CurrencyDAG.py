import boto3
import csv
import os
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from datetime import datetime, timedelta

# AWS S3 설정
S3_BUCKET = "your-s3-bucket"
S3_KEY = "exchange_rate_data/exchange_rate.csv"
LOCAL_FILE_PATH = "C:\Users\kkj214\Repositories\Currency_ETL\exchange_rate.csv"

# API 설정
API_URL_TEMPLATE = "https://www.koreaexim.go.kr/site/program/financial/exchangeJSON?authkey={api_key}&searchdate={date}&data=AP01"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="exchange_rate_etl",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=True,
    tags=["ETL", "API"],
) as dag:

    @task
    def extract_transform(api_key, start_date, end_date):
        """
        1. API 데이터를 추출하고 변환
        2. 로컬 CSV 파일로 저장
        """
        current_date = start_date
        records = []

        while current_date <= end_date:
            # API 호출
            formatted_date = current_date.strftime("%Y%m%d")
            response = requests.get(API_URL_TEMPLATE.format(api_key=api_key, date=formatted_date))
            if response.status_code != 200:
                raise Exception(f"API 호출 실패: {response.status_code}")
            
            data = response.json()
            for item in data:
                if item['cur_unit'] == "USD":  # USD만 필터링
                    records.append([
                        item['cur_unit'],  # currency_code
                        "KRW",  # base_currency
                        float(item['deal_bas_r'].replace(",", "")),  # exchange_rate
                        current_date.strftime("%Y-%m-%d"),  # reference_date
                    ])
            current_date += timedelta(days=1)

        # 로컬 파일 저장
        with open(LOCAL_FILE_PATH, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["currency_code", "base_currency", "exchange_rate", "reference_date"])
            writer.writerows(records)

        return LOCAL_FILE_PATH

    @task
    def upload_to_s3(file_path):
        """
        3. 변환된 데이터를 S3에 업로드
        """
        s3 = boto3.client('s3')
        s3.upload_file(file_path, S3_BUCKET, S3_KEY)
        return f"s3://{S3_BUCKET}/{S3_KEY}"

    load_to_redshift = S3ToRedshiftOperator(
        task_id="load_to_redshift",
        schema="raw_data",
        table="currency",
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY,
        copy_options=["csv", "IGNOREHEADER 1"],
        aws_conn_id="aws_default",
        redshift_conn_id="redshift_default"
    )

    transform_to_analytics = PostgresOperator(
        task_id="transform_to_analytics",
        postgres_conn_id="redshift_default",
        sql="""
        INSERT INTO analytics.currency_analytics (currency_code, base_currency, avg_exchange_rate, reference_month)
        SELECT
            currency_code,
            base_currency,
            AVG(exchange_rate) AS avg_exchange_rate,
            DATE_TRUNC('month', reference_date) AS reference_month
        FROM raw_data.currency
        GROUP BY currency_code, base_currency, DATE_TRUNC('month', reference_date);
        """
    )

    # Task 간 의존성 설정
    records_file = extract_transform("your_api_key", datetime(2024, 1, 1), datetime.now())
    s3_path = upload_to_s3(records_file)
    records_file >> s3_path >> load_to_redshift >> transform_to_analytics
