import yfinance as yf

from dags.module.airflow.aws.s3.upload import upload_csv_to_S3


def yf_extract(SNOWFLAKE_TABLE, **kwargs):
    krw_usd = yf.download("KRW=X", start="2019-01-01")
    krw_usd["date"] = krw_usd.index
    krw_usd = krw_usd[["date", "Adj Close", "Close", "High", "Low", "Open"]]
    krw_usd.columns = krw_usd.columns.get_level_values(0)
    # S3로 CSV저장
    upload_csv_to_S3(krw_usd, True, SNOWFLAKE_TABLE, kwargs)
