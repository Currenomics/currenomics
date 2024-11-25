from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from io import StringIO
import pandas as pd
import yfinance as yf

# Airflow 변수 설정 필요
AWS_ACCESS_KEY = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

# 환율 데이터 티커 설정
CURRENCY_PAIRS = {
    "USD_KRW": "KRW=X",
    "JPY_KRW": "KRWJPY=X",
    "CNY_KRW": "CNYKRW=X",
    "EUR_KRW": "EURKRW=X"
}

# 데이터베이스, 스키마 정보
SNOWFLAKE_DATABASE = "dev"
SNOWFLAKE_SCHEMA = "raw_data"

# S3 버킷 정보
S3_BUCKET_NAME = "pjt-currenomics"

def upload_csv_to_S3(data, table_name, is_tmp, ds_nodash):
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    s3_hook = S3Hook(aws_conn_id="aws_conn_id")
    key = f"tmp/{ds_nodash}_{table_name}_tmp.csv" if is_tmp else f"{table_name}/{table_name}.csv"
    s3_hook.load_string(
        string_data=csv_data,
        key=key,
        bucket_name=S3_BUCKET_NAME,
        replace=True
    )

def extract(currency_pair, table_name, **kwargs):
    ds_nodash = kwargs['ds_nodash']
    data = yf.download(currency_pair, start='2019-01-01', progress=False)

    # yfinance 데이터프레임 변환
    currency_df = pd.DataFrame({
        "date": data.index,
        "rate": data['Close']
    }).dropna()

    # S3로 CSV 저장
    upload_csv_to_S3(currency_df, table_name, True, ds_nodash)

def transform(table_name, **kwargs):
    ds_nodash = kwargs['ds_nodash']
    s3_hook = S3Hook(aws_conn_id="aws_conn_id")
    file_content = s3_hook.read_key(
        key=f"tmp/{ds_nodash}_{table_name}_tmp.csv",
        bucket_name=S3_BUCKET_NAME
    )

    # Pandas로 데이터 로드 및 변환
    df = pd.read_csv(StringIO(file_content))
    modified_df = df  # 추가 변환 로직 필요 시 수정

    # 변환된 데이터 S3 업로드
    upload_csv_to_S3(modified_df, table_name, False, ds_nodash)

def delete_temp_data(table_name, **kwargs):
    ds_nodash = kwargs['ds_nodash']
    s3_hook = S3Hook(aws_conn_id="aws_conn_id")
    s3_hook.delete_objects(
        bucket=S3_BUCKET_NAME,
        keys=[f"tmp/{ds_nodash}_{table_name}_tmp.csv"]
    )

# DAG 정의
dag = DAG(
    dag_id='Currency_rate_etl',
    start_date=datetime(2019, 1, 1),
    schedule='0 9 * * *',
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3)
    }
)

# 태스크 정의 함수
def create_tasks(currency_name, currency_pair):
    table_name = currency_name.lower()

    create_table_sql = f"""
    DROP TABLE IF EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name};
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name} (
        date DATE,
        rate FLOAT
    );
    """

    copy_into_sql = f"""
    USE WAREHOUSE COMPUTE_WH;
    COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name}
    FROM 's3://{S3_BUCKET_NAME}/{table_name}/{table_name}.csv'
    CREDENTIALS=(
        AWS_KEY_ID='{AWS_ACCESS_KEY}'
        AWS_SECRET_KEY='{AWS_SECRET_ACCESS_KEY}'
    )
    FILE_FORMAT=(
        TYPE='CSV',
        SKIP_HEADER=1,
        FIELD_OPTIONALLY_ENCLOSED_BY='"',
        NULL_IF = ('', 'NULL')
    );
    """

    extract_task = PythonOperator(
        task_id=f'extract_{currency_name}',
        python_callable=extract,
        op_kwargs={'currency_pair': currency_pair, 'table_name': table_name},
        provide_context=True,
        dag=dag
    )

    transform_task = PythonOperator(
        task_id=f'transform_{currency_name}',
        python_callable=transform,
        op_kwargs={'table_name': table_name},
        provide_context=True,
        dag=dag
    )

    create_table_task = SnowflakeOperator(
        task_id=f'create_table_{currency_name}',
        snowflake_conn_id='snowflake_conn_id',
        sql=create_table_sql,
        dag=dag
    )

    load_task = SnowflakeOperator(
        task_id=f'load_{currency_name}',
        snowflake_conn_id='snowflake_conn_id',
        sql=copy_into_sql,
        dag=dag
    )

    delete_task = PythonOperator(
        task_id=f'delete_temp_{currency_name}',
        python_callable=delete_temp_data,
        op_kwargs={'table_name': table_name},
        provide_context=True,
        dag=dag
    )

    # Task Dependency
    extract_task >> transform_task >> create_table_task >> load_task >> delete_task

# 통화 쌍에 대한 태스크 생성
for currency_name, currency_pair in CURRENCY_PAIRS.items():
    create_tasks(currency_name, currency_pair)
