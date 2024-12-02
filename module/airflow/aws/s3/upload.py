from io import StringIO

import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from module.airflow.time import get_ds_nodash


def upload_csv_to_S3(data: pd.DataFrame, is_tmp: bool, snowflake_table: str, kwargs):
    ds_nodash = get_ds_nodash(kwargs)
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()
    s3_hook = S3Hook(aws_conn_id="aws_conn_id")
    if is_tmp:
        s3_hook.load_string(
            string_data=csv_data,
            key=f"tmp/{ds_nodash}_{snowflake_table}_tmp.csv",
            bucket_name="pjt-currenomics",
            replace=True,
        )
    else:
        s3_hook.load_string(
            string_data=csv_data,
            key=f"{snowflake_table}/{snowflake_table}.csv",
            bucket_name="pjt-currenomics",
            replace=True,
        )
