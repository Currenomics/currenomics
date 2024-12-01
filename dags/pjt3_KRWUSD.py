from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from module.airflow.aws.s3.delete import delete_object
from module.airflow.aws.s3.transform import transform_tmp_object
from module.aws.aws_credential import AwsCredentialInfo
from module.etl.yfinance.extract import yf_extract
from module.sql.sql_loader import load_sql
from module.sql.utils import get_create_table_sql
from module.util.vo.table_info import TableInfo

# airflow변수 설정 필요
AWS_CREDENTIAL_INFO = AwsCredentialInfo(
    aws_access_key=Variable.get("AWS_ACCESS_KEY_ID"),
    aws_secret_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
)
# FRED SERIES ID
TICKER = "KRW=X"
# 데이터베이스, 스키마, 테이블 정보
TABLE_INFO = TableInfo(database="dev", schema="raw_data", table="KOR_USD")


dag = DAG(
    dag_id="pjt3_KRWUSD",
    start_date=datetime(2019, 1, 1),
    schedule="0 9 * * *",
    max_active_runs=1,
    catchup=False,
    # default_args = {
    #     'retries': 1,
    #     'retry_delay': timedelta(minutes=1)
    # }
)

# 1. FRED API 호출 및 S3저장
extract = PythonOperator(
    task_id="extract",
    python_callable=yf_extract,
    provide_context=True,
    dag=dag,
)

# 2. tmp에 저장된 CSV호출 후 데이터 변환 후 S3 본 테이블에 이관
transform = PythonOperator(
    task_id="transform",
    python_callable=transform_tmp_object,
    provide_context=True,
    dag=dag,
)

# 3. 테이블 생성 태스크 (IF NOT EXISTS)
create_table = SnowflakeOperator(
    task_id="create_table_if_not_exists",
    snowflake_conn_id="snowflake_conn_id",
    sql=get_create_table_sql(TABLE_INFO),
    dag=dag,
)

# 4. S3의 정제 데이터를 스누오플레이크로 COPY 태스크
load = SnowflakeOperator(
    task_id="load_to_snowflake",
    snowflake_conn_id="snowflake_conn_id",
    sql=load_sql("copy_info_csv", "dql", **TABLE_INFO.get_dict(), **AWS_CREDENTIAL_INFO.get_dict()),
    dag=dag,
)

# 4. S3의 임시파일 삭제
delete_temp_data = PythonOperator(
    task_id="delete",
    python_callable=delete_object,
    provide_context=True,
    dag=dag,
)

extract >> transform >> create_table >> load >> delete_temp_data
