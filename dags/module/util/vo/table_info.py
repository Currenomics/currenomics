from dataclasses import dataclass


@dataclass(frozen=True)
class TableInfo:
    database: str
    schema: str
    table: str

    def get_dict(self) -> dict:
        return {"database": self.database, "schema": self.schema, "table": self.table}
