"""
clean_facts.py
================
Construction des tables de faits du modèle en étoile PayZenHR.

Chaque fonction build_fact_* résout les clés étrangères vers les dimensions
déjà construites (clean_dimensions.py) et applique les règles de nettoyage
de la Partie 2 du modèle (aucun remplacement automatique de NULL par 0).
"""

import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from PayZenHR_ETL.pipline_test.payzenhr_pyspark.config import (
    PAYROLL_MEASURE_COLUMNS,
    PAYROLL_TECHNICAL_COLUMNS_TO_DROP,
    TECHNICAL_AUDIT_COLUMNS,
)
from transform.transformations import (
    drop_columns_if_present,
    flag_negative,
    generate_surrogate_key,
    rename_columns,
    resolve_date_key_from_year_month,
)

logger = logging.getLogger("payzenhr.transform.facts")


def _payroll_measure_mapping() -> dict:
    """Convertit PascalCase -> snake_case pour les colonnes de mesures de paie."""
    mapping = {}
    for m in PAYROLL_MEASURE_COLUMNS:
        snake = "".join(["_" + ch.lower() if ch.isupper() else ch for ch in m]).lstrip("_")
        mapping[m] = snake
    return mapping


def _resolve_job_position_at_payroll(payroll: DataFrame, dim_employee_contract: DataFrame,
                                      dim_employee: DataFrame) -> DataFrame:
    """
    Jointure temporelle J15-J17 (décision D1) : résout le poste et le type
    de contrat en vigueur à la date du bulletin de paie.

    En cas de chevauchement de contrats actifs pour un même employé à la
    même date (anomalie de données source), un seul contrat est retenu
    (StartDate le plus récent) et le cas est loggé comme anomalie - jamais
    résolu silencieusement sans trace.
    """
    payroll_with_date = payroll.withColumn(
        "_payroll_month_date",
        F.to_date(F.concat_ws("-", F.col("Year").cast("string"), F.col("Month").cast("string"), F.lit("01"))),
    )

    emp = dim_employee.select("sk_employee", F.col("bk_employee_id").alias("_emp_bk"))
    contracts = dim_employee_contract.select(
        F.col("sk_employee").alias("_ct_sk_employee"), "sk_job_position", "sk_contract_type", "start_date", "end_date"
    )

    joined = (
        payroll_with_date
        .join(emp, payroll_with_date["EmployeeId"] == emp["_emp_bk"], "left")
        .join(
            contracts,
            (payroll_with_date["_payroll_month_date"] >= contracts["start_date"])
            & (
                contracts["end_date"].isNull()
                | (payroll_with_date["_payroll_month_date"] <= contracts["end_date"])
            )
            & (emp["sk_employee"] == contracts["_ct_sk_employee"]),
            "left",
        )
    )

    # Détection des chevauchements : plus d'un contrat résolu pour un même bulletin
    window = Window.partitionBy(payroll_with_date["Id"])
    joined = joined.withColumn("_nb_contracts_resolved", F.count(F.lit(1)).over(window))
    overlap_count = joined.filter(F.col("_nb_contracts_resolved") > 1).select(payroll_with_date["Id"]).distinct().count()
    if overlap_count > 0:
        logger.warning(
            "Chevauchement de contrats détecté sur %d bulletin(s) - un seul contrat "
            "conservé (StartDate le plus récent), anomalie à investiguer (Partie 5)",
            overlap_count,
        )

    priority_window = Window.partitionBy(payroll_with_date["Id"]).orderBy(F.col("start_date").desc_nulls_last())
    joined = (
        joined.withColumn("_rn", F.row_number().over(priority_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_nb_contracts_resolved", "_payroll_month_date", "_emp_bk", "_ct_sk_employee", "start_date", "end_date")
    )

    joined = joined.withColumnRenamed("sk_job_position", "sk_job_position_at_payroll")
    joined = joined.withColumnRenamed("sk_contract_type", "sk_contract_type_at_payroll")
    return joined


def build_fact_payroll(raw: dict, dim_employee: DataFrame, dim_company: DataFrame,
                        dim_employee_contract: DataFrame) -> DataFrame:
    """
    fact_payroll <- PayrollResults. Grain : 1 ligne = 1 bulletin de paie
    (employé x mois x année). Unicité NON confirmée par le profilage - un
    contrôle explicite bloquant est exécuté en amont du chargement
    (quality/data_quality.py + warehouse/star_schema.py).
    """
    df = drop_columns_if_present(
        raw["payroll_results"], TECHNICAL_AUDIT_COLUMNS + PAYROLL_TECHNICAL_COLUMNS_TO_DROP
    )

    df = _resolve_job_position_at_payroll(df, dim_employee_contract, dim_employee)

    company = dim_company.select("sk_company", F.col("bk_company_id").alias("_company_bk"))
    df = df.join(company, df["CompanyId"] == company["_company_bk"], "left").drop("_company_bk")

    df = resolve_date_key_from_year_month(df, "Year", "Month", "sk_date")

    df = rename_columns(df, {"Id": "bk_payroll_id"})
    measure_mapping = _payroll_measure_mapping()
    df = rename_columns(df, measure_mapping)

    df = flag_negative(df, list(measure_mapping.values()), "has_negative_amount")

    keep_cols = (
        ["bk_payroll_id", "sk_employee", "sk_company", "sk_date",
         "sk_job_position_at_payroll", "sk_contract_type_at_payroll", "has_negative_amount"]
        + list(measure_mapping.values())
    )
    df = df.select(*[c for c in keep_cols if c in df.columns])
    df = generate_surrogate_key(df, ["bk_payroll_id"], "sk_payroll")
    logger.info("fact_payroll construite")
    return df


def build_fact_payroll_premium(raw: dict, fact_payroll: DataFrame) -> DataFrame:
    """
    fact_payroll_premium <- PayrollResultPrimes. Grain : 1 ligne = 1 prime x
    1 bulletin. sk_employee/sk_company/sk_date dénormalisées depuis le fait
    parent pour un usage direct en Power BI (jointure J21).
    """
    df = drop_columns_if_present(raw["payroll_result_primes"], TECHNICAL_AUDIT_COLUMNS)
    parent = fact_payroll.select(
        "sk_payroll", F.col("bk_payroll_id").alias("_parent_bk"), "sk_employee", "sk_company", "sk_date"
    )
    df = df.join(parent, df["PayrollResultId"] == parent["_parent_bk"], "inner").drop("_parent_bk")
    df = rename_columns(df, {"Id": "bk_payroll_premium_id", "Montant": "amount",
                              "Label": "premium_label", "IsTaxable": "is_taxable", "Ordre": "display_order"})
    df = df.select("bk_payroll_premium_id", "sk_payroll", "sk_employee", "sk_company", "sk_date",
                    "amount", "premium_label", "is_taxable", "display_order")
    df = generate_surrogate_key(df, ["bk_payroll_premium_id"], "sk_payroll_premium")
    logger.info("fact_payroll_premium construite")
    return df


def build_fact_payroll_tax_snapshot(raw: dict, fact_payroll: DataFrame) -> DataFrame:
    """
    fact_payroll_tax_snapshot <- PayrollTaxSnapshots. Relation 1:1 attendue
    avec fact_payroll (jointure J22) - contrôlée en Partie 5.
    """
    df = drop_columns_if_present(raw["payroll_tax_snapshots"], TECHNICAL_AUDIT_COLUMNS)
    parent = fact_payroll.select(
        "sk_payroll", F.col("bk_payroll_id").alias("_parent_bk"), "sk_employee", "sk_company", "sk_date"
    )
    df = df.join(parent, df["PayrollResultId"] == parent["_parent_bk"], "inner").drop("_parent_bk")
    df = rename_columns(
        df,
        {"Id": "bk_tax_snapshot_id", "CumulBrut": "cumul_brut", "CumulCnss": "cumul_cnss",
         "CumulAmo": "cumul_amo", "CumulIr": "cumul_ir", "CumulNet": "cumul_net",
         "CumulFp": "cumul_fp", "TauxEffectif": "taux_effectif"},
    )
    df = df.select("bk_tax_snapshot_id", "sk_payroll", "sk_employee", "sk_company", "sk_date",
                    "cumul_brut", "cumul_cnss", "cumul_amo", "cumul_ir", "cumul_net",
                    "cumul_fp", "taux_effectif")
    df = generate_surrogate_key(df, ["bk_tax_snapshot_id"], "sk_payroll_tax_snapshot")
    logger.info("fact_payroll_tax_snapshot construite")
    return df


def _build_monthly_fact(raw_df, measure_source_col: str, measure_target_col: str,
                         dim_employee: DataFrame, dim_company: DataFrame, bk_prefix: str,
                         sk_fact_name: str) -> DataFrame:
    """Fonction générique pour les faits mensuels pré-agrégés (grain confirmé Employee x Year x Month)."""
    df = drop_columns_if_present(raw_df, TECHNICAL_AUDIT_COLUMNS)
    emp = dim_employee.select("sk_employee", F.col("bk_employee_id").alias("_emp_bk"), "sk_company")
    df = df.join(emp, df["EmployeeId"] == emp["_emp_bk"], "inner").drop("_emp_bk")
    df = resolve_date_key_from_year_month(df, "Year", "Month", "sk_date")

    df = rename_columns(df, {"Id": f"bk_{bk_prefix}_id", measure_source_col: measure_target_col})
    df = df.select(f"bk_{bk_prefix}_id", "sk_employee", "sk_company", "sk_date", measure_target_col)
    df = generate_surrogate_key(df, [f"bk_{bk_prefix}_id"], sk_fact_name)
    return df


def build_fact_absence_monthly(raw: dict, dim_employee: DataFrame, dim_company: DataFrame) -> DataFrame:
    """fact_absence_monthly <- EmployeeMonthlyAbsenceSummaries (unicité confirmée EmployeeId x Year x Month)."""
    df = _build_monthly_fact(raw["employee_monthly_absence_summaries"], "AbsenceDays", "absence_days",
                              dim_employee, dim_company, "absence_monthly", "sk_absence_monthly")
    logger.info("fact_absence_monthly construite")
    return df


def build_fact_leave_monthly(raw: dict, dim_employee: DataFrame, dim_company: DataFrame) -> DataFrame:
    """fact_leave_monthly <- EmployeeMonthlyLeaveSummaries (pas de sk_leave_type : colonne non confirmée en source)."""
    df = _build_monthly_fact(raw["employee_monthly_leave_summaries"], "LeaveDays", "leave_days",
                              dim_employee, dim_company, "leave_monthly", "sk_leave_monthly")
    logger.info("fact_leave_monthly construite")
    return df


def build_fact_worked_days_monthly(raw: dict, dim_employee: DataFrame, dim_company: DataFrame) -> DataFrame:
    """fact_worked_days_monthly <- EmployeeMonthlyWorkedDaysSummaries."""
    df = _build_monthly_fact(raw["employee_monthly_worked_days_summaries"], "WorkedDays", "worked_days",
                              dim_employee, dim_company, "worked_days_monthly", "sk_worked_days_monthly")
    logger.info("fact_worked_days_monthly construite")
    return df


def build_fact_overtime_monthly(raw: dict, dim_employee: DataFrame, dim_company: DataFrame) -> DataFrame:
    """
    fact_overtime_monthly <- EmployeeMonthlyOvertimeSummaries. Chargée pour
    la cohérence du modèle mais NON exposée dans le dashboard v1 (4 lignes
    seulement dans l'extrait profilé).
    """
    df = drop_columns_if_present(raw["employee_monthly_overtime_summaries"], TECHNICAL_AUDIT_COLUMNS)
    emp = dim_employee.select("sk_employee", F.col("bk_employee_id").alias("_emp_bk"), "sk_company")
    df = df.join(emp, df["EmployeeId"] == emp["_emp_bk"], "inner").drop("_emp_bk")
    df = resolve_date_key_from_year_month(df, "Year", "Month", "sk_date")
    df = rename_columns(
        df,
        {"Id": "bk_overtime_monthly_id", "Hours25Pct": "hours_25_pct",
         "Hours50Pct": "hours_50_pct", "Hours100Pct": "hours_100_pct"},
    )
    df = df.select("bk_overtime_monthly_id", "sk_employee", "sk_company", "sk_date",
                    "hours_25_pct", "hours_50_pct", "hours_100_pct")
    df = generate_surrogate_key(df, ["bk_overtime_monthly_id"], "sk_overtime_monthly")
    logger.info("fact_overtime_monthly construite (non exposée au dashboard v1 - volumétrie faible)")
    return df


def build_fact_leave_balance(raw: dict, dim_employee: DataFrame, dim_leave_type: DataFrame) -> DataFrame:
    """
    fact_leave_balance <- LeaveBalances. Grain : employé x type de congé x
    mois x année (unicité confirmée). Profondeur historique 2019-2026.
    """
    df = drop_columns_if_present(raw["leave_balances"], TECHNICAL_AUDIT_COLUMNS)
    emp = dim_employee.select("sk_employee", F.col("bk_employee_id").alias("_emp_bk"), "sk_company")
    lt = dim_leave_type.select("sk_leave_type", F.col("bk_leave_type_id").alias("_lt_bk"))

    df = (
        df.join(emp, df["EmployeeId"] == emp["_emp_bk"], "inner")
        .join(lt, df["LeaveTypeId"] == lt["_lt_bk"], "left")
        .drop("_emp_bk", "_lt_bk")
    )
    df = resolve_date_key_from_year_month(df, "Year", "Month", "sk_date")
    df = rename_columns(
        df,
        {"Id": "bk_leave_balance_id", "OpeningDays": "opening_days", "AccruedDays": "accrued_days",
         "UsedDays": "used_days", "CarryInDays": "carry_in_days", "CarryOutDays": "carry_out_days",
         "ClosingDays": "closing_days", "CarryoverExpiresOn": "carryover_expires_on"},
    )
    # Flag de cohérence (jamais de recalcul silencieux, cf. Partie 2.16 du modèle)
    df = df.withColumn(
        "balance_inconsistency_flag",
        F.when(
            F.abs(
                F.coalesce(F.col("closing_days"), F.lit(0.0))
                - (
                    F.coalesce(F.col("opening_days"), F.lit(0.0))
                    + F.coalesce(F.col("accrued_days"), F.lit(0.0))
                    + F.coalesce(F.col("carry_in_days"), F.lit(0.0))
                    - F.coalesce(F.col("used_days"), F.lit(0.0))
                )
            ) > 0.5,
            F.lit(True),
        ).otherwise(F.lit(False)),
    )
    df = df.select("bk_leave_balance_id", "sk_employee", "sk_leave_type", "sk_company", "sk_date",
                    "opening_days", "accrued_days", "used_days", "carry_in_days", "carry_out_days",
                    "closing_days", "carryover_expires_on", "balance_inconsistency_flag")
    df = generate_surrogate_key(df, ["bk_leave_balance_id"], "sk_leave_balance")
    logger.info("fact_leave_balance construite")
    return df


def build_fact_salary_component(raw: dict, dim_employee_salary: DataFrame) -> DataFrame:
    """fact_salary_component <- EmployeeSalaryComponents. sk_employee dénormalisée (jointures J28-J29)."""
    df = drop_columns_if_present(raw["employee_salary_components"], TECHNICAL_AUDIT_COLUMNS)
    salary = dim_employee_salary.select(
        "sk_employee_salary", F.col("bk_employee_salary_id").alias("_salary_bk"), "sk_employee"
    )
    df = df.join(salary, df["EmployeeSalaryId"] == salary["_salary_bk"], "inner").drop("_salary_bk")
    df = rename_columns(
        df,
        {"Id": "bk_salary_component_id", "Amount": "amount", "Percentage": "percentage",
         "ComponentType": "component_type", "IsTaxable": "is_taxable", "IsSocial": "is_social",
         "IsCIMR": "is_cimr", "EndDate": "end_date"},
    )
    df = df.select("bk_salary_component_id", "sk_employee_salary", "sk_employee", "amount",
                    "percentage", "component_type", "is_taxable", "is_social", "is_cimr", "end_date")
    df = generate_surrogate_key(df, ["bk_salary_component_id"], "sk_salary_component")
    logger.info("fact_salary_component construite")
    return df


def build_fact_alert(raw: dict, dim_company: DataFrame, dim_alert_definition: DataFrame) -> DataFrame:
    """
    fact_alert <- Alerts. EntityType/EntityId conservés en attributs texte
    dégénérés, JAMAIS utilisés en jointure (décision D4 - référence
    polymorphe sans FK réelle). CreatedAt sert de date d'événement (dérivée
    avant suppression de la colonne technique).
    """
    df = raw["alerts"].withColumn("sk_date", F.date_format(F.col("CreatedAt"), "yyyyMMdd").cast("int"))
    df = drop_columns_if_present(df, TECHNICAL_AUDIT_COLUMNS)

    company = dim_company.select("sk_company", F.col("bk_company_id").alias("_company_bk"))
    alert_def = dim_alert_definition.select("sk_alert_definition", F.col("bk_alert_definition_id").alias("_def_bk"))

    df = (
        df.join(company, df["CompanyId"] == company["_company_bk"], "inner")
        .join(alert_def, df["AlertDefinitionId"] == alert_def["_def_bk"], "left")
        .drop("_company_bk", "_def_bk")
    )
    df = rename_columns(
        df,
        {"Id": "bk_alert_id", "Status": "status", "IsAcknowledged": "is_acknowledged",
         "EntityType": "entity_type", "EntityId": "entity_id", "ResolvedAt": "resolved_at",
         "AcknowledgedAt": "acknowledged_at", "AcknowledgedByUserId": "acknowledged_by_user_id"},
    )
    keep_cols = ["bk_alert_id", "sk_company", "sk_alert_definition", "sk_date", "status", "is_acknowledged",
                 "entity_type", "entity_id", "resolved_at", "acknowledged_at", "acknowledged_by_user_id"]
    df = df.select(*[c for c in keep_cols if c in df.columns])
    df = generate_surrogate_key(df, ["bk_alert_id"], "sk_alert")
    logger.info("fact_alert construite")
    return df


def build_fact_leave_request(raw: dict, dim_employee: DataFrame, dim_leave_type: DataFrame) -> DataFrame:
    """
    fact_leave_request <- LeaveRequests. Grain : 1 demande de congé.
    Réserve de volumétrie (2 lignes dans l'extrait profilé) - table chargée
    mais à ne pas mettre en avant dans le dashboard tant que non revalidée.
    """
    df = drop_columns_if_present(raw["leave_requests"], TECHNICAL_AUDIT_COLUMNS)
    emp = dim_employee.select("sk_employee", F.col("bk_employee_id").alias("_emp_bk"), "sk_company")
    lt = dim_leave_type.select("sk_leave_type", F.col("bk_leave_type_id").alias("_lt_bk"))

    df = (
        df.join(emp, df["EmployeeId"] == emp["_emp_bk"], "inner")
        .join(lt, df["LeaveTypeId"] == lt["_lt_bk"], "left")
        .drop("_emp_bk", "_lt_bk")
    )
    df = df.withColumn("sk_date", F.date_format(F.col("RequestedAt"), "yyyyMMdd").cast("int"))
    df = rename_columns(
        df,
        {"Id": "bk_leave_request_id", "CalendarDays": "calendar_days",
         "WorkingDaysDeducted": "working_days_deducted", "Status": "status",
         "RequestedAt": "requested_at", "DecisionAt": "decision_at"},
    )
    df = df.withColumn(
        "decision_before_request_flag",
        F.when(
            F.col("decision_at").isNotNull() & (F.col("decision_at") < F.col("requested_at")), F.lit(True)
        ).otherwise(F.lit(False)),
    )
    df = df.select("bk_leave_request_id", "sk_employee", "sk_company", "sk_leave_type", "sk_date",
                    "calendar_days", "working_days_deducted", "status", "requested_at",
                    "decision_at", "decision_before_request_flag")
    df = generate_surrogate_key(df, ["bk_leave_request_id"], "sk_leave_request")
    logger.info("fact_leave_request construite (réserve de volumétrie)")
    return df


def build_fact_absence_event(raw: dict, dim_employee: DataFrame) -> DataFrame:
    """
    fact_absence_event <- EmployeeAbsences. Grain : 1 absence constatée.
    Réserve de volumétrie (2 lignes).
    """
    df = drop_columns_if_present(raw["employee_absences"], TECHNICAL_AUDIT_COLUMNS)
    emp = dim_employee.select("sk_employee", F.col("bk_employee_id").alias("_emp_bk"), "sk_company")
    df = df.join(emp, df["EmployeeId"] == emp["_emp_bk"], "inner").drop("_emp_bk")

    if "AbsenceDate" in df.columns:
        df = df.withColumn("sk_date", F.date_format(F.col("AbsenceDate"), "yyyyMMdd").cast("int"))

    df = rename_columns(df, {"Id": "bk_absence_event_id", "DurationType": "duration_type", "Status": "status"})
    keep_cols = ["bk_absence_event_id", "sk_employee", "sk_company", "sk_date", "duration_type", "status"]
    df = df.select(*[c for c in keep_cols if c in df.columns])
    df = generate_surrogate_key(df, ["bk_absence_event_id"], "sk_absence_event")
    logger.info("fact_absence_event construite (réserve de volumétrie)")
    return df


def build_all_facts(raw: dict, dims: dict) -> dict:
    """Orchestration : construit tous les faits en résolvant leurs dimensions."""
    logger.info("Construction de l'ensemble des faits")
    fact_payroll = build_fact_payroll(raw, dims["dim_employee"], dims["dim_company"], dims["dim_employee_contract"])

    facts = {
        "fact_payroll": fact_payroll,
        "fact_payroll_premium": build_fact_payroll_premium(raw, fact_payroll),
        "fact_payroll_tax_snapshot": build_fact_payroll_tax_snapshot(raw, fact_payroll),
        "fact_absence_monthly": build_fact_absence_monthly(raw, dims["dim_employee"], dims["dim_company"]),
        "fact_leave_monthly": build_fact_leave_monthly(raw, dims["dim_employee"], dims["dim_company"]),
        "fact_worked_days_monthly": build_fact_worked_days_monthly(raw, dims["dim_employee"], dims["dim_company"]),
        "fact_overtime_monthly": build_fact_overtime_monthly(raw, dims["dim_employee"], dims["dim_company"]),
        "fact_leave_balance": build_fact_leave_balance(raw, dims["dim_employee"], dims["dim_leave_type"]),
        "fact_salary_component": build_fact_salary_component(raw, dims["dim_employee_salary"]),
        "fact_alert": build_fact_alert(raw, dims["dim_company"], dims["dim_alert_definition"]),
        "fact_leave_request": build_fact_leave_request(raw, dims["dim_employee"], dims["dim_leave_type"]),
        "fact_absence_event": build_fact_absence_event(raw, dims["dim_employee"]),
    }
    return facts