from dbt.adapters.sql import SQLAdapter
from dbt.adapters.presto import PrestoConnectionManager


class PrestoAdapter(SQLAdapter):
    ConnectionManager = PrestoConnectionManager

    @classmethod
    def date_function(cls):
        return 'datenow()'
