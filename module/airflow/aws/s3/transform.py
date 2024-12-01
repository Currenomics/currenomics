from io import StringIO

import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from module.airflow.aws.s3.upload import upload_csv_to_S3
from module.airflow.time import get_ds_nodash


def transform_tmp_object(snowflake_table: str, **kwargs):
    ds_nodash = get_ds_nodash(kwargs)
    s3_hook = S3Hook(aws_conn_id="aws_conn_id")
    file_content = s3_hook.read_key(key=f"tmp/{ds_nodash}_{snowflake_table}_tmp.csv", bucket_name="pjt-currenomics")
    # Pandas로 데이터 로드
    df = pd.read_csv(StringIO(file_content))  # S3 데이터를 pandas 데이터프레임으로 읽기
    # Pandas로 데이터 처리로직
    # TO-DO
    modified_df = df
    #
    upload_csv_to_S3(modified_df, False, ds_nodash)
