import os.path
import pathlib


def load_sql(query_name: str, query_type: str = "dml", **kwargs) -> str:
    cur_dir = pathlib.Path(__file__).parent.resolve()
    file_path = f"{cur_dir}/{query_type}/{query_name}.sql"

    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"Failed to load file to string: {file_path}")

    with open(file_path, "r", encoding="utf8") as f:
        sql_template = f.read()
        return sql_template.format(**kwargs)