import logging

from extract.extract_sqlserver import (
    get_spark_session,
    extract_table_to_staging
)


# ============================================================
# CONFIGURATION LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 50)
    print("       TEST EXTRACTION EMPLOYEES")
    print("=" * 50)

    spark = None

    try:

        # --------------------------------------------
        # 1. Création de Spark
        # --------------------------------------------

        spark = get_spark_session()

        # --------------------------------------------
        # 2. Extraction Employees
        # --------------------------------------------

        row_count = extract_table_to_staging(
            spark,
            "Employees"
        )

        print()
        print("=" * 50)
        print("EXTRACTION REUSSIE")
        print("=" * 50)
        print(f"Nombre de lignes extraites : {row_count}")

    except Exception as exc:

        logging.error(
            "Erreur pendant le test Employees : %s",
            exc,
            exc_info=True
        )

        raise

    finally:

        if spark is not None:
            spark.stop()

            print()
            print("Session Spark arretee.")


if __name__ == "__main__":
    main()