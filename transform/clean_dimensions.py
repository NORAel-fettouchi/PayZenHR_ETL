"""
transform/clean_dimensions.py
==============================
Construction de chaque dimension du modele en etoile PayZenHR, exactement
telle que specifiee en Partie 1/2 de la conception. Une fonction par
dimension, toutes reutilisant transform/transformations.py.
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from PayZenHR_ETL.pipline_test.payzenhr_pyspark.config import TECHNICAL_COLUMNS_TO_DROP, DIM_DATE_START, DIM_DATE_END
from transform.transformations import (
    trim_columns,
    normalize_title_case,
    normalize_upper_case,
    cast_boolean,
    cast_date,
    drop_technical_columns,
    rename_columns,
    add_surrogate_key,
    select_latest_row_per_group,
    denormalize_reference,
    compute_age,
    compute_age_bracket,
    fill_unknown_for_grouping_axis,
    flag_out_of_range,
)

logger = logging.getLogger("payzenhr.transform.dimensions")


# ---------------------------------------------------------------------------
# dim_city / dim_country (construites en premier : reutilisees par dim_employee
# et dim_company - cf. decision D5, dimensions geographiques conformees)
# ---------------------------------------------------------------------------

def build_dim_country(countries_df: DataFrame) -> DataFrame:
    df = drop_technical_columns(countries_df, TECHNICAL_COLUMNS_TO_DROP)
    df = trim_columns(df, ["CountryName", "CountryCode"])
    df = normalize_title_case(df, ["CountryName"])
    df = normalize_upper_case(df, ["CountryCode"])
    df = rename_columns(df, {"Id": "bk_country_id", "CountryName": "country_name", "CountryCode": "country_code"})
    df = add_surrogate_key(df, "sk_country")
    logger.info("dim_country construite")
    return df


def build_dim_city(cities_df: DataFrame, dim_country: DataFrame) -> DataFrame:
    df = drop_technical_columns(cities_df, TECHNICAL_COLUMNS_TO_DROP)
    df = trim_columns(df, ["CityName"])
    df = normalize_title_case(df, ["CityName"])

    # J7 : resolution de sk_country (FK orpheline -> NULL, jamais de valeur inventee)
    df = df.join(
        dim_country.select(F.col("bk_country_id"), F.col("sk_country")),
        df["CountryId"] == F.col("bk_country_id"),
        how="left",
    ).drop("bk_country_id", "CountryId")

    df = rename_columns(df, {"Id": "bk_city_id", "CityName": "city_name"})
    df = add_surrogate_key(df, "sk_city")
    logger.info("dim_city construite")
    return df


# ---------------------------------------------------------------------------
# dim_status
# ---------------------------------------------------------------------------

def build_dim_status(statuses_df: DataFrame) -> DataFrame:
    df = drop_technical_columns(statuses_df, TECHNICAL_COLUMNS_TO_DROP)
    df = trim_columns(df, ["Code", "NameFr", "NameAr", "NameEn"])
    df = normalize_upper_case(df, ["Code"])
    df = cast_boolean(df, ["AffectsPayroll", "AffectsAttendance", "AffectsAccess"])
    df = rename_columns(
        df,
        {
            "Id": "bk_status_id",
            "Code": "status_code",
            "NameFr": "status_name_fr",
            "NameAr": "status_name_ar",
            "NameEn": "status_name_en",
            "AffectsPayroll": "affects_payroll",
            "AffectsAttendance": "affects_attendance",
            "AffectsAccess": "affects_access",
        },
    )
    df = add_surrogate_key(df, "sk_status")
    logger.info("dim_status construite")
    return df


# ---------------------------------------------------------------------------
# dim_department
# ---------------------------------------------------------------------------

def build_dim_department(departements_df: DataFrame) -> DataFrame:
    df = drop_technical_columns(departements_df, TECHNICAL_COLUMNS_TO_DROP)
    df = trim_columns(df, ["DepartementName"])
    df = normalize_title_case(df, ["DepartementName"])
    df = flag_out_of_range(df, "HierarchyLevel", min_value=0, flag_col="hierarchy_level_negative_flag")
    df = rename_columns(
        df,
        {
            "Id": "bk_department_id",
            "DepartementName": "department_name",
            "HierarchyLevel": "hierarchy_level",
            "ParentDepartementId": "bk_parent_department_id",
            "ManagerId": "bk_manager_employee_id",
        },
    )
    df = add_surrogate_key(df, "sk_department")
    logger.info("dim_department construite (reserve de couverture cf. D6 - conservee malgre 90,52%% de NULL sur Employees.DepartementId)")
    return df


# ---------------------------------------------------------------------------
# dim_company
# ---------------------------------------------------------------------------

def build_dim_company(companies_df: DataFrame, dim_city: DataFrame) -> DataFrame:
    df = drop_technical_columns(companies_df, TECHNICAL_COLUMNS_TO_DROP)
    df = trim_columns(df, ["CompanyName", "Currency", "PayrollPeriodicity", "BusinessSector", "IceNumber", "CnssNumber"])
    df = normalize_title_case(df, ["CompanyName", "PayrollPeriodicity"])
    df = normalize_upper_case(df, ["Currency"])

    # J8 : resolution de sk_city
    df = df.join(
        dim_city.select(F.col("bk_city_id"), F.col("sk_city")),
        df["CityId"] == F.col("bk_city_id"),
        how="left",
    ).drop("bk_city_id", "CityId")

    df = rename_columns(
        df,
        {
            "Id": "bk_company_id",
            "CompanyName": "company_name",
            "Currency": "currency_code",
            "PayrollPeriodicity": "payroll_periodicity",
            "BusinessSector": "business_sector",
            "IceNumber": "ice_number",
            "CnssNumber": "cnss_number",
            "ManagedByCompanyId": "bk_managed_by_company_id",
        },
    )
    df = add_surrogate_key(df, "sk_company")
    logger.info("dim_company construite")
    return df


# ---------------------------------------------------------------------------
# dim_employee (denormalisation Genders/Nationalities/MaritalStatuses/
# EducationLevels + adresse principale D2 + resolution FK Status/Company/Department/City)
# ---------------------------------------------------------------------------

def build_dim_employee(
    employees_df: DataFrame,
    genders_df: DataFrame,
    nationalities_df: DataFrame,
    marital_statuses_df: DataFrame,
    education_levels_df: DataFrame,
    employee_addresses_df: DataFrame,
    dim_status: DataFrame,
    dim_company: DataFrame,
    dim_department: DataFrame,
    dim_city: DataFrame,
) -> DataFrame:
    df = drop_technical_columns(employees_df, TECHNICAL_COLUMNS_TO_DROP)

    # Colonnes a 100% NULL ou hors perimetre (cf. Partie 2.1) - supprimees explicitement
    df = df.drop("ManagerId", "CountryId", "BankAccountId", "PersonalEmail")
    # CategoryId : NULL a 99,74% -> non charge comme attribut actif dans dim_employee
    if "CategoryId" in df.columns:
        df = df.drop("CategoryId")

    df = trim_columns(df, ["LastName", "FirstName", "Matricule"])
    df = normalize_title_case(df, ["LastName", "FirstName"])
    df = normalize_upper_case(df, ["Matricule"])
    df = cast_date(df, ["DateOfBirth"])

    # Age / tranche d'age (flag, jamais suppression automatique en cas de valeur suspecte)
    df = compute_age(df, "DateOfBirth", "age")
    df = compute_age_bracket(df, "age", "age_bracket")
    df = flag_out_of_range(df, "age", min_value=16, max_value=75, flag_col="age_out_of_range_flag")

    # J1 : Genre - denormalise, NULL -> "Non renseigne" (axe de KPI direct, cf. Partie 2)
    df = denormalize_reference(df, genders_df, "GenderId", "Id", "GenderLabel", "gender_label")
    df = fill_unknown_for_grouping_axis(df, "gender_label")

    # J2 : Nationalite - denormalisee, NULL -> "Non renseigne" (axe de KPI direct)
    df = denormalize_reference(df, nationalities_df, "NationalityId", "Id", "NationalityLabel", "nationality_label")
    df = fill_unknown_for_grouping_axis(df, "nationality_label")

    # J3 : Statut marital - denormalise, NULL CONSERVE TEL QUEL (reserve forte D6 : 85,27% NULL)
    df = denormalize_reference(df, marital_statuses_df, "MaritalStatusId", "Id", "MaritalStatusLabel", "marital_status_label")
    df = df.withColumn("has_marital_status", F.col("marital_status_label").isNotNull())

    # J4 : Niveau d'education - denormalise, taux correct -> pas de traitement special
    df = denormalize_reference(df, education_levels_df, "EducationLevelId", "Id", "EducationLevelLabel", "education_level_label")

    # J5 : selection de l'adresse principale (la plus recente par CreatedAt) AVANT jointure
    #      pour eviter la duplication documentee en D2 (1213 adresses / 1561 employes)
    primary_address = select_latest_row_per_group(
        employee_addresses_df, partition_cols=["EmployeeId"], order_col="CreatedAt", descending=True
    )
    primary_address = trim_columns(primary_address, ["AddressLine1", "ZipCode"])
    primary_address = normalize_title_case(primary_address, ["AddressLine1"])

    df = df.join(
        primary_address.select("EmployeeId", "AddressLine1", "ZipCode", "CityId"),
        on="EmployeeId",
        how="left",
    )

    # J6 : resolution de sk_city (adresse) - FK orpheline ou absente -> NULL
    df = df.join(
        dim_city.select(F.col("bk_city_id").alias("__city_bk__"), F.col("sk_city")),
        df["CityId"] == F.col("__city_bk__"),
        how="left",
    ).drop("__city_bk__", "CityId")

    # Resolution FK obligatoires : StatusId, CompanyId (jamais NULL attendu - controle en Partie 5)
    df = df.join(
        dim_status.select(F.col("bk_status_id").alias("__status_bk__"), F.col("sk_status")),
        df["StatusId"] == F.col("__status_bk__"),
        how="left",
    ).drop("__status_bk__", "StatusId")

    df = df.join(
        dim_company.select(F.col("bk_company_id").alias("__company_bk__"), F.col("sk_company")),
        df["CompanyId"] == F.col("__company_bk__"),
        how="left",
    ).drop("__company_bk__", "CompanyId")

    # Resolution FK a reserve : DepartementId - NULL CONSERVE TEL QUEL (90,52% NULL, D6)
    df = df.join(
        dim_department.select(F.col("bk_department_id").alias("__dept_bk__"), F.col("sk_department")),
        df["DepartementId"] == F.col("__dept_bk__"),
        how="left",
    ).drop("__dept_bk__", "DepartementId")

    df = rename_columns(
        df,
        {
            "Id": "bk_employee_id",
            "Matricule": "employee_matricule",
            "LastName": "last_name",
            "FirstName": "first_name",
            "DateOfBirth": "date_of_birth",
            "AddressLine1": "address_line1",
            "ZipCode": "zip_code",
        },
    )
    df = add_surrogate_key(df, "sk_employee")
    logger.info("dim_employee construite (reserve forte sur sk_department, marital_status_label - NULL conserve)")
    return df


# ---------------------------------------------------------------------------
# dim_job_position
# ---------------------------------------------------------------------------

def build_dim_job_position(job_positions_df: DataFrame, dim_company: DataFrame) -> DataFrame:
    df = drop_technical_columns(job_positions_df, TECHNICAL_COLUMNS_TO_DROP)
    df = trim_columns(df, ["Name"])
    df = normalize_title_case(df, ["Name"])

    df = df.join(
        dim_company.select(F.col("bk_company_id").alias("__company_bk__"), F.col("sk_company")),
        df["CompanyId"] == F.col("__company_bk__"),
        how="left",
    ).drop("__company_bk__", "CompanyId")

    df = rename_columns(df, {"Id": "bk_job_position_id", "Name": "job_position_name"})
    df = add_surrogate_key(df, "sk_job_position")
    logger.info("dim_job_position construite")
    return df


# ---------------------------------------------------------------------------
# dim_contract_type (J9-J10 : denormalisation LegalContractTypes/StateEmploymentPrograms)
# ---------------------------------------------------------------------------

def build_dim_contract_type(
    contract_types_df: DataFrame,
    legal_contract_types_df: DataFrame,
    state_employment_programs_df: DataFrame,
) -> DataFrame:
    df = drop_technical_columns(contract_types_df, TECHNICAL_COLUMNS_TO_DROP)
    df = trim_columns(df, ["ContractTypeName"])
    df = normalize_title_case(df, ["ContractTypeName"])

    df = denormalize_reference(
        df, legal_contract_types_df, "LegalContractTypeId", "Id", "Name", "legal_contract_type_name"
    )
    df = denormalize_reference(
        df, state_employment_programs_df, "StateEmploymentProgramId", "Id", "Name", "state_employment_program_name"
    )

    df = rename_columns(df, {"Id": "bk_contract_type_id", "ContractTypeName": "contract_type_name"})
    df = add_surrogate_key(df, "sk_contract_type")
    logger.info("dim_contract_type construite")
    return df


# ---------------------------------------------------------------------------
# dim_employee_contract (SCD2 - historique complet conserve, cf. Partie 4)
# ---------------------------------------------------------------------------

def build_dim_employee_contract(
    employee_contracts_df: DataFrame,
    dim_employee: DataFrame,
    dim_job_position: DataFrame,
    dim_contract_type: DataFrame,
) -> DataFrame:
    df = drop_technical_columns(employee_contracts_df, TECHNICAL_COLUMNS_TO_DROP)
    df = cast_date(df, ["StartDate", "EndDate"])

    # StartDate ne doit jamais etre NULL (Partie 2.5) : lignes invalides isolees
    invalid = df.filter(F.col("StartDate").isNull())
    invalid_count = invalid.count()
    if invalid_count > 0:
        logger.warning("%s contrats sans StartDate rejetes de dim_employee_contract", invalid_count)
    df = df.filter(F.col("StartDate").isNotNull())

    # EndDate < StartDate -> flag, ligne conservee mais signalee (pas de correction automatique)
    df = df.withColumn("date_range_invalid_flag", F.col("EndDate").isNotNull() & (F.col("EndDate") < F.col("StartDate")))

    df = df.join(
        dim_employee.select(F.col("bk_employee_id").alias("__emp_bk__"), F.col("sk_employee")),
        df["EmployeeId"] == F.col("__emp_bk__"),
        how="left",
    ).drop("__emp_bk__", "EmployeeId")

    df = df.join(
        dim_job_position.select(F.col("bk_job_position_id").alias("__jp_bk__"), F.col("sk_job_position")),
        df["JobPositionId"] == F.col("__jp_bk__"),
        how="left",
    ).drop("__jp_bk__", "JobPositionId")

    df = df.join(
        dim_contract_type.select(F.col("bk_contract_type_id").alias("__ct_bk__"), F.col("sk_contract_type")),
        df["ContractTypeId"] == F.col("__ct_bk__"),
        how="left",
    ).drop("__ct_bk__", "ContractTypeId")

    df = rename_columns(df, {"Id": "bk_contract_id", "StartDate": "start_date", "EndDate": "end_date"})
    df = add_surrogate_key(df, "sk_employee_contract")
    logger.info("dim_employee_contract construite (SCD2, EndDate NULL = contrat en cours)")
    return df


# ---------------------------------------------------------------------------
# dim_employee_salary (SCD2)
# ---------------------------------------------------------------------------

def build_dim_employee_salary(employee_salaries_df: DataFrame, dim_employee: DataFrame) -> DataFrame:
    df = drop_technical_columns(employee_salaries_df, TECHNICAL_COLUMNS_TO_DROP)
    df = cast_date(df, ["EffectiveDate", "EndDate"])

    from PayZenHR_ETL.transform.transformations import cast_decimal
    df = cast_decimal(df, ["BaseSalary"])

    # BaseSalary NULL = probleme potentiel (Partie 2.6) : signale, pas rejete automatiquement
    df = df.withColumn("base_salary_null_flag", F.col("BaseSalary").isNull())
    df = df.withColumn(
        "date_range_invalid_flag",
        F.col("EndDate").isNotNull() & (F.col("EndDate") < F.col("EffectiveDate")),
    )

    df = df.join(
        dim_employee.select(F.col("bk_employee_id").alias("__emp_bk__"), F.col("sk_employee")),
        df["EmployeeId"] == F.col("__emp_bk__"),
        how="left",
    ).drop("__emp_bk__", "EmployeeId")

    df = rename_columns(
        df,
        {
            "Id": "bk_employee_salary_id",
            "BaseSalary": "base_salary",
            "ContractId": "bk_contract_id",
            "EffectiveDate": "effective_date",
            "EndDate": "end_date",
        },
    )
    df = add_surrogate_key(df, "sk_employee_salary")
    logger.info("dim_employee_salary construite (SCD2, EndDate NULL = salaire courant)")
    return df


# ---------------------------------------------------------------------------
# dim_leave_type (+ satellite dim_leave_type_legal_rule, J11-J12)
# ---------------------------------------------------------------------------

def build_dim_leave_type(leave_types_df: DataFrame, leave_type_policies_df: DataFrame, dim_company: DataFrame) -> DataFrame:
    df = drop_technical_columns(leave_types_df, TECHNICAL_COLUMNS_TO_DROP)
    df = trim_columns(df, ["LastName"])
    df = normalize_title_case(df, ["LastName"])

    # J11 : une politique par type attendue (N:1) - si plusieurs, la plus recente est retenue
    # et le cas est logue (a confirmer avec l'equipe metier avant code final, cf. Partie 3.1)
    policy_dedup = select_latest_row_per_group(
        leave_type_policies_df, partition_cols=["LeaveTypeId"], order_col="Id", descending=True
    )
    df = df.join(
        policy_dedup.drop("Id"),
        df["Id"] == policy_dedup["LeaveTypeId"],
        how="left",
    ).drop("LeaveTypeId")

    df = df.join(
        dim_company.select(F.col("bk_company_id").alias("__company_bk__"), F.col("sk_company")),
        df["CompanyId"] == F.col("__company_bk__"),
        how="left",
    ).drop("__company_bk__", "CompanyId")

    df = rename_columns(df, {"Id": "bk_leave_type_id", "LastName": "leave_type_name"})
    df = add_surrogate_key(df, "sk_leave_type")
    logger.info("dim_leave_type construite")
    return df


def build_dim_leave_type_legal_rule(leave_type_legal_rules_df: DataFrame, dim_leave_type: DataFrame) -> DataFrame:
    """J12 : relation 1-N reelle conservee comme table satellite (pas aplatie dans dim_leave_type)."""
    df = drop_technical_columns(leave_type_legal_rules_df, TECHNICAL_COLUMNS_TO_DROP)

    df = df.join(
        dim_leave_type.select(F.col("bk_leave_type_id").alias("__lt_bk__"), F.col("sk_leave_type")),
        df["LeaveTypeId"] == F.col("__lt_bk__"),
        how="left",
    ).drop("__lt_bk__", "LeaveTypeId")

    df = rename_columns(df, {"Id": "bk_legal_rule_id"})
    df = add_surrogate_key(df, "sk_leave_legal_rule")
    logger.info("dim_leave_type_legal_rule construite (satellite 1-N)")
    return df


# ---------------------------------------------------------------------------
# dim_holiday / dim_working_calendar (D3 : NON fusionnees dans dim_date)
# ---------------------------------------------------------------------------

def build_dim_holiday(holidays_df: DataFrame, dim_company: DataFrame) -> DataFrame:
    df = drop_technical_columns(holidays_df, TECHNICAL_COLUMNS_TO_DROP)
    df = cast_date(df, ["HolidayDate"])
    df = cast_boolean(df, ["IsMandatory", "IsPaid", "AffectPayroll", "AffectAttendance"])

    df = df.join(
        dim_company.select(F.col("bk_company_id").alias("__company_bk__"), F.col("sk_company")),
        df["CompanyId"] == F.col("__company_bk__"),
        how="left",
    ).drop("__company_bk__", "CompanyId")

    df = rename_columns(df, {"Id": "bk_holiday_id", "HolidayDate": "holiday_date"})
    df = add_surrogate_key(df, "sk_holiday")
    logger.info("dim_holiday construite (satellite de calcul, non liee a dim_date - decision D3)")
    return df


def build_dim_working_calendar(working_calendars_df: DataFrame, dim_company: DataFrame) -> DataFrame:
    df = drop_technical_columns(working_calendars_df, TECHNICAL_COLUMNS_TO_DROP)
    df = cast_boolean(df, ["IsWorkingDay"])
    df = flag_out_of_range(df, "DayOfWeek", min_value=0, max_value=6, flag_col="day_of_week_out_of_range_flag")

    df = df.join(
        dim_company.select(F.col("bk_company_id").alias("__company_bk__"), F.col("sk_company")),
        df["CompanyId"] == F.col("__company_bk__"),
        how="left",
    ).drop("__company_bk__", "CompanyId")

    df = rename_columns(df, {"Id": "bk_working_calendar_id", "DayOfWeek": "day_of_week", "IsWorkingDay": "is_working_day"})
    df = add_surrogate_key(df, "sk_working_calendar")
    logger.info("dim_working_calendar construite (satellite de calcul, non liee a dim_date - decision D3)")
    return df


# ---------------------------------------------------------------------------
# dim_alert_definition (J13-J14)
# ---------------------------------------------------------------------------

def build_dim_alert_definition(
    alert_definitions_df: DataFrame,
    alert_categories_df: DataFrame,
    alert_severities_df: DataFrame,
) -> DataFrame:
    # Description/SourceField : 100% NULL, supprimees (Partie 2.10)
    df = alert_definitions_df.drop("Description", "SourceField")
    df = drop_technical_columns(df, TECHNICAL_COLUMNS_TO_DROP)
    df = trim_columns(df, ["Code", "Name"])
    df = normalize_upper_case(df, ["Code"])
    df = normalize_title_case(df, ["Name"])
    df = cast_boolean(df, ["IsActive"])

    df = denormalize_reference(df, alert_categories_df, "CategoryId", "Id", "Name", "alert_category_name")
    df = denormalize_reference(df, alert_severities_df, "SeverityId", "Id", "Label", "alert_severity_label")

    df = rename_columns(
        df, {"Id": "bk_alert_definition_id", "Code": "alert_definition_code", "Name": "alert_definition_name", "IsActive": "is_active"}
    )
    df = add_surrogate_key(df, "sk_alert_definition")
    logger.info("dim_alert_definition construite (severite conservee en clair)")
    return df


# ---------------------------------------------------------------------------
# dim_date (construite - aucune source, cf. Partie 4.0)
# ---------------------------------------------------------------------------

def build_dim_date(spark: SparkSession, start_date: str = DIM_DATE_START, end_date: str = DIM_DATE_END) -> DataFrame:
    """
    Genere dim_date par sequence procedurale (F.sequence), de maniere
    entierement distribuee : aucune boucle Python, aucun collect().
    """
    df = spark.sql(
        f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS full_date"
    )

    df = (
        df.withColumn("sk_date", F.date_format(F.col("full_date"), "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("day_of_week_name", F.date_format("full_date", "EEEE"))
        .withColumn("week_number", F.weekofyear("full_date"))
        .withColumn("is_weekend", F.dayofweek("full_date").isin([1, 7]))
        .withColumn("is_first_day_of_month", F.dayofmonth("full_date") == 1)
    )

    logger.info("dim_date construite : %s -> %s", start_date, end_date)
    return df