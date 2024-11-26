from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
import logging

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def extract_transform(api_key):
    # Korea EximBank API URL
    api_url = f"https://www.koreaexim.go.kr/site/program/financial/exchangeJSON?authkey={api_key}&searchdate=20241125&data=AP01"
    
    # API 호출
    response = requests.get(api_url)
    if response.status_code != 200:
        raise Exception(f"API 호출 실패: {response.status_code}")
    
    data = response.json()
    
    # 필요한 데이터 추출
    records = []
    for item in data:
        currency = item['cur_unit']  # 통화 코드
        base_currency = "KRW"  # 기준 통화
        exchange_rate = item['deal_bas_r']  # 기준 환율
        exchange_rate = float(exchange_rate.replace(",", ""))  # 쉼표 제거 및 float 변환
        date = item['bkpr']  # 기준일
        
        records.append([currency, base_currency, exchange_rate, date])
    
    return records

def _create_table(cur, schema, table, drop_first):
    if drop_first:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
    cur.execute(f"""
        CREATE TABLE {schema}.{table} (
            currency VARCHAR(10),
            base_currency VARCHAR(10),
            exchange_rate FLOAT,
            date DATE
        );
    """)

@task
def load(schema, table, records):
    logging.info("Load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        # 원본 테이블이 없으면 생성
        _create_table(cur, schema, table, False)

        # 임시 테이블로 원본 테이블 복사
        cur.execute(f"CREATE TEMP TABLE t AS SELECT * FROM {schema}.{table};")
        for r in records:
            sql = f"""
                INSERT INTO t VALUES (
                    '{r[0]}', '{r[1]}', {r[2]}, '{r[3]}'
                );
            """
            print(sql)
            cur.execute(sql)

        # 원본 테이블 생성 및 데이터 복사
        _create_table(cur, schema, table, True)
        cur.execute(f"INSERT INTO {schema}.{table} SELECT DISTINCT * FROM t;")
        cur.execute("COMMIT;")
    except Exception as error:
        logging.error(error)
        cur.execute("ROLLBACK;")
        raise
    logging.info("Load done")

with DAG(
    dag_id='ExchangeRate_ETL',
    start_date=datetime(2024, 11, 24),
    catchup=False,
    tags=['ETL', 'API']
) as dag:
    api_key = "API_KEY"
    records = extract_transform(api_key)
    load("kyungjun", "exchange_rate", records)
