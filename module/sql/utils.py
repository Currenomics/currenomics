from module.sql.sql_loader import load_sql
from module.util.vo.table_info import TableInfo


def get_create_table_sql(table_info: TableInfo) -> str:
    return (
        f"{load_sql('drop_table', 'dml', **table_info.get_dict())}\n"
        f"{load_sql(table_info.table, 'ddl', **table_info.get_dict())}"
    )
