"""
extract/extract_sqlserver.py

Extraction des données depuis SQL Server
vers des DataFrames PySpark.
"""

import logging
import time

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from PayZenHR_ETL.pipline_test.payzenhr_pyspark.config import (
    SQLSERVER_JDBC_URL,
    SQLSERVER_CONNECTION_PROPERTIES,
)


logger = logging.getLogger("payzenhr.extract")


# ============================================================
# LECTURE D'UNE TABLE SQL SERVER
# ============================================================

def read_sqlserver_table(
    spark: SparkSession,
    table_name: str
) -> DataFrame:

    """
    Lit une table SQL Server avec Spark JDBC.
    """

    start_time = time.time()

    logger.info(
        "Debut extraction SQL Server -> [%s]",
        table_name
    )

    df = (
        spark.read
        .jdbc(
            url=SQLSERVER_JDBC_URL,
            table=f"dbo.{table_name}",
            properties=SQLSERVER_CONNECTION_PROPERTIES
        )
    )

    duration = time.time() - start_time

    logger.info(
        "Table [%s] extraite avec succes en %.2f secondes",
        table_name,
        duration
    )

    return df


# ============================================================
# EXTRACTION DE PLUSIEURS TABLES
# ============================================================

def extract_source_tables(
    spark: SparkSession,
    table_names: list[str]
) -> dict[str, DataFrame]:

    """
    Extrait plusieurs tables SQL Server.

    Retourne :

    {
        "Employees": DataFrame,
        "Departments": DataFrame,
        "Cities": DataFrame
    }
    """

    extracted_tables = {}

    logger.info(
        "Debut extraction de %s tables",
        len(table_names)
    )

    for table_name in table_names:

        try:

            df = read_sqlserver_table(
                spark,
                table_name
            )

            extracted_tables[table_name] = df

            logger.info(
                "Table [%s] ajoutee au pipeline",
                table_name
            )

        except Exception as exc:

            logger.error(
                "Erreur extraction table [%s : %s",
                table_name,
                exc,
                exc_info=True
            )

            raise

    logger.info(
        "Extraction terminee : %s tables",
        len(extracted_tables)
    )

    return extracted_tables