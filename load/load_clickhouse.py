"""
load/load_clickhouse.py

Chargement des DataFrames PySpark vers ClickHouse.
"""

import logging

from pyspark.sql import DataFrame

from PayZenHR_ETL.pipline_test.payzenhr_pyspark.config import (
    CLICKHOUSE_JDBC_URL,
    CLICKHOUSE_CONNECTION_PROPERTIES,
)


logger = logging.getLogger("payzenhr.load")


# ============================================================
# ECRITURE DANS CLICKHOUSE
# ============================================================

def write_to_clickhouse(
    df: DataFrame,
    table_name: str,
    mode: str = "append"
) -> None:

    """
    Ecrit un DataFrame PySpark dans ClickHouse.
    """

    logger.info(
        "Chargement ClickHouse -> [%s]",
        table_name
    )

    (
        df.write
        .format("jdbc")
        .option(
            "url",
            CLICKHOUSE_JDBC_URL
        )
        .option(
            "dbtable",
            table_name
        )
        .option(
            "user",
            CLICKHOUSE_CONNECTION_PROPERTIES["user"]
        )
        .option(
            "password",
            CLICKHOUSE_CONNECTION_PROPERTIES["password"]
        )
        .option(
            "driver",
            CLICKHOUSE_CONNECTION_PROPERTIES["driver"]
        )
        .option(
            "batchsize",
            "10000"
        )
        .mode(mode)
        .save()
    )

    logger.info(
        "Table [%s] chargee avec succes",
        table_name
    )