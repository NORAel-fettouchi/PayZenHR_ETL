"""
transform/star_schema.py
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    monotonically_increasing_id
)


logger = logging.getLogger(
    "payzenhr.star_schema"
)


# ============================================================
# DIMENSION EMPLOYEE
# ============================================================

def create_dim_employee(
    employees_df: DataFrame
) -> DataFrame:

    """
    Création de la dimension Employee.
    """

    logger.info(
        "Creation de dim_employee"
    )

    dim_employee = (
        employees_df
        .select(
            col("EmployeeId"),
            col("FirstName"),
            col("LastName"),
            col("FullName"),
            col("Email"),
            col("DepartmentId"),
            col("CityId")
        )
        .dropDuplicates(
            ["EmployeeId"]
        )
    )

    return dim_employee