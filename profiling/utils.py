from config import spark, URL, PROPERTIES


def get_tables():
    """
    Retourne toutes les tables SQL Server
    """

    query = """
    (
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE='BASE TABLE'
    ) AS T
    """

    df = spark.read.jdbc(
        url=URL,
        table=query,
        properties=PROPERTIES
    )

    return [row.TABLE_NAME for row in df.collect()]


def read_table(table_name):
    """
    Lit une table SQL Server
    """

    return spark.read.jdbc(
        url=URL,
        table=table_name,
        properties=PROPERTIES
    )


def separator():

    print("=" * 70)