"""
transform/transform_employee.py
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    lower,
    concat_ws,
    lit,
)


logger = logging.getLogger("payzenhr.transform")


# ============================================================
# TRANSFORMATION EMPLOYEES
# ============================================================

def transform_employees(
    employees_df: DataFrame
) -> DataFrame:

    """
    Nettoyage et préparation des employés.
    """

    logger.info(
        "Debut transformation Employees"
    )

    df = employees_df

    # --------------------------------------------------------
    # Suppression des doublons
    # --------------------------------------------------------

    df = df.dropDuplicates()

    # --------------------------------------------------------
    # Nettoyage des textes
    # --------------------------------------------------------

    df = (
        df
        .withColumn(
            "FirstName",
            trim(col("FirstName"))
        )
        .withColumn(
            "LastName",
            trim(col("LastName"))
        )
    )

    # --------------------------------------------------------
    # Nom complet
    # --------------------------------------------------------

    df = df.withColumn(
        "FullName",
        concat_ws(
            " ",
            col("FirstName"),
            col("LastName")
        )
    )

    # --------------------------------------------------------
    # Email en minuscules
    # --------------------------------------------------------

    df = df.withColumn(
        "Email",
        lower(trim(col("Email")))
    )

    logger.info(
        "Transformation Employees terminee"
    )

    return df