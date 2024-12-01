USE WAREHOUSE COMPUTE_WH;
COPY INTO '{database}'.'{schema}'.'{table}'
FROM "s3://pjt-currenomics/'{table}'/'{table}'.csv"
CREDENTIALS=(
    AWS_KEY_ID='{aws_access_key}'
    AWS_SECRET_KEY='{aws_secret_access_key}'
)
FILE_FORMAT=(
    TYPE='CSV',
    SKIP_HEADER=1,
    FIELD_OPTIONALLY_ENCLOSED_BY='"',
    NULL_IF = ('', 'NULL')
);