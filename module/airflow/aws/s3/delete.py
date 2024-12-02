from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from module.airflow.time import get_ds_nodash


def delete_object(snowflake_table: str, **kwargs):
    ds_nodash = get_ds_nodash(kwargs)
    s3_hook = S3Hook(aws_conn_id="aws_conn_id")
    s3_hook.delete_objects(bucket="pjt-currenomics", keys=[f"tmp/{ds_nodash}_{snowflake_table}_tmp.csv"])
