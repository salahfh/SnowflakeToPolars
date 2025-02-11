# [1]: https://docs.pola.rs/user-guide/io/database/
# [2]: https://arrow.apache.org/adbc/current/driver/snowflake.html

from adbc_driver_snowflake import dbapi
from typing import Self
from dotenv import dotenv_values
import polars as pl

config = dotenv_values(".env")

db_kwargs = {
    "adbc.snowflake.sql.uri.protocol": "https",
    "adbc.snowflake.sql.uri.port": "443",
    "adbc.snowflake.sql.account": config["ACCOUNT"],
    "username": config["USERNAME"],
    "password": config["PASSWORD"],
}


class SnowflakePl:
    """
    From SQL on Snowflake to Polars DF.
    """

    def __init__(self, auth_kwargs: dict = db_kwargs):
        self.db_kwargs = auth_kwargs
        self._connection: dbapi.Connection | None = None

    def start_session(f):
        def wrapped(self: Self, *args, **kwargs):
            if self._connection is None:
                self._connection = dbapi.connect(db_kwargs=db_kwargs)
            return f(self, *args, **kwargs)

        return wrapped

    def close_session(self):
        self._connection.close()
        self._connection = None

    @start_session
    def run_sql(self, query: str, schema: dict = None) -> pl.DataFrame:
        return pl.read_database(
            query=query, connection=self._connection, schema_overrides=schema
        )


if __name__ == "__main__":
    table_name = "TABLENAME"
    query = f"Select * from {table_name} limit 10"
    sp = SnowflakePl()
    df = sp.run_sql(query)
    print(df)
