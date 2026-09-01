"""
data_quality.py
=================
Contrôles qualité génériques et spécifiques (Partie 5 du document de
conception PayZenHR_Conception_DW.md).

Chaque fonction retourne un dict structuré {table, controle, regle, seuil,
observe, statut, action} et écrit un log clair. Les contrôles évitent les
collect() sur les données ligne à ligne : seuls des count()/agg() sur de
petits résultats agrégés sont exécutés (actions Spark nécessaires au
contrôle lui-même, jamais un rapatriement de données brutes).
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger("payzenhr.quality")


def _log_result(table: str, control: str, rule: str, threshold: str, observed, status: str, action: str) -> dict:
    result = {
        "table": table, "controle": control, "regle": rule, "seuil": threshold,
        "observe": observed, "statut": status, "action": action,
    }
    log_line = (
        f"[{table}] [{control}] regle='{rule}' observe={observed} "
        f"seuil={threshold} statut={status} action='{action}'"
    )
    if status == "FAIL":
        logger.error(log_line)
    elif status == "WARNING":
        logger.warning(log_line)
    else:
        logger.info(log_line)
    return result


def check_row_count(df: DataFrame, table_name: str) -> dict:
    """Contrôle : nombre de lignes."""
    count = df.count()
    status = "OK" if count > 0 else "FAIL"
    return _log_result(
        table_name, "nombre_de_lignes", "COUNT(*) > 0", ">0", count, status,
        "Aucune donnée chargée - vérifier l'extraction" if status == "FAIL" else "Aucune",
    )


def compare_row_counts(source_df: DataFrame, target_df: DataFrame, table_name: str,
                        expected_loss_pct: float = 0.0, tolerance_pct: float = 1.0) -> dict:
    """Contrôle : comparaison du nombre de lignes avant/après transformation."""
    source_count = source_df.count()
    target_count = target_df.count()
    loss_pct = 0.0 if source_count == 0 else 100.0 * (source_count - target_count) / source_count
    status = "OK" if abs(loss_pct - expected_loss_pct) <= tolerance_pct else "WARNING"
    return _log_result(
        table_name, "comparaison_avant_apres",
        f"perte attendue={expected_loss_pct}% (+/-{tolerance_pct}%)",
        f"{expected_loss_pct}+/-{tolerance_pct}%",
        f"{loss_pct:.2f}% (source={source_count}, cible={target_count})",
        status, "Vérifier la règle de filtrage/dédoublonnage appliquée" if status == "WARNING" else "Aucune",
    )


def check_duplicates_on_key(df: DataFrame, key_cols: list, table_name: str, blocking: bool = True) -> dict:
    """Contrôle : doublons sur clé technique ou combinaison métier (grain du fait/dimension)."""
    total = df.count()
    distinct = df.select(*key_cols).distinct().count()
    duplicates = total - distinct
    status = "OK" if duplicates == 0 else ("FAIL" if blocking else "WARNING")
    action = "Aucune"
    if duplicates > 0:
        action = (
            "Chargement bloqué - isoler les doublons en table de rejet, revoir le grain"
            if blocking else "Ligne conservée mais anomalie à investiguer"
        )
    return _log_result(table_name, "doublons_cle", f"unicite sur {key_cols}", "0 doublon", duplicates, status, action)


def check_nulls(df: DataFrame, columns: list, table_name: str, max_null_pct: float = 0.0) -> dict:
    """Contrôle : taux de NULL sur des colonnes structurantes (FK obligatoires, clés)."""
    existing = [c for c in columns if c in df.columns]
    total = df.count()
    if total == 0 or not existing:
        return _log_result(table_name, "valeurs_null", f"NULL sur {columns}", f"<= {max_null_pct}%", "N/A", "WARNING", "Vérifier l'extraction")
    agg_row = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in existing]).collect()[0].asDict()
    worst_pct = max((v / total * 100.0 for v in agg_row.values()), default=0.0)
    status = "OK" if worst_pct <= max_null_pct else "WARNING"
    detail = {k: f"{v}/{total} ({v/total*100:.1f}%)" for k, v in agg_row.items()}
    return _log_result(
        table_name, "valeurs_null", f"NULL sur {columns}", f"<= {max_null_pct}%", detail, status,
        "Réserve déjà documentée dans le modèle - vérifier dérive vs profilage initial" if status == "WARNING" else "Aucune",
    )


def check_orphan_fk(df: DataFrame, fk_col: str, dim_df: DataFrame, dim_key_col: str,
                     table_name: str, blocking: bool = True) -> dict:
    """Contrôle : clés étrangères orphelines (FK sans correspondance en dimension)."""
    if fk_col not in df.columns:
        return _log_result(table_name, "fk_orphelines", f"{fk_col} existe", "colonne presente", "colonne absente", "WARNING", "Vérifier le schéma source")
    dim_keys = dim_df.select(F.col(dim_key_col).alias("_dim_key")).distinct()
    orphan_count = (
        df.filter(F.col(fk_col).isNotNull())
        .join(dim_keys, F.col(fk_col) == F.col("_dim_key"), "left_anti")
        .count()
    )
    status = "OK" if orphan_count == 0 else ("FAIL" if blocking else "WARNING")
    action = "Aucune"
    if orphan_count > 0:
        action = "Ligne rejetée (FK obligatoire)" if blocking else "sk resolu a NULL, ligne conservee et loguee"
    return _log_result(table_name, "fk_orphelines", f"{fk_col} -> dimension.{dim_key_col}", "0 orpheline", orphan_count, status, action)


def check_negative_values(df: DataFrame, columns: list, table_name: str, blocking: bool = False) -> dict:
    """Contrôle : valeurs négatives incorrectes sur des mesures normalement positives."""
    existing = [c for c in columns if c in df.columns]
    if not existing:
        return _log_result(table_name, "valeurs_negatives", f"{columns} >= 0", "0", "N/A", "OK", "Aucune")
    condition = None
    for c in existing:
        cond = F.col(c) < 0
        condition = cond if condition is None else (condition | cond)
    negative_count = df.filter(condition).count()
    status = "OK" if negative_count == 0 else ("FAIL" if blocking else "WARNING")
    action = "Aucune" if negative_count == 0 else "Ligne exclue des agrégats et loguée"
    return _log_result(table_name, "valeurs_negatives", f"{existing} >= 0", "0", negative_count, status, action)


def check_date_order(df: DataFrame, start_col: str, end_col: str, table_name: str) -> dict:
    """Contrôle : dates invalides (ex. EndDate < StartDate)."""
    if start_col not in df.columns or end_col not in df.columns:
        return _log_result(table_name, "dates_invalides", f"{end_col} >= {start_col}", "0", "N/A", "OK", "Aucune")
    invalid_count = df.filter(F.col(end_col).isNotNull() & (F.col(end_col) < F.col(start_col))).count()
    status = "OK" if invalid_count == 0 else "WARNING"
    return _log_result(
        table_name, "dates_invalides", f"{end_col} >= {start_col}", "0", invalid_count, status,
        "Ligne conservée, flaguée pour revue métier" if invalid_count > 0 else "Aucune",
    )


def run_dimension_quality_checks(dim_df: DataFrame, table_name: str, business_key_col: str) -> list:
    """Batterie standard de contrôles pour une dimension."""
    return [
        check_row_count(dim_df, table_name),
        check_duplicates_on_key(dim_df, [business_key_col], table_name, blocking=True),
        check_nulls(dim_df, [business_key_col], table_name, max_null_pct=0.0),
    ]


def run_fact_quality_checks(fact_df: DataFrame, table_name: str, grain_cols: list,
                             mandatory_fk_cols: list, dim_lookups: dict,
                             monetary_cols: list = None, blocking_grain: bool = False) -> list:
    """
    Batterie standard de contrôles pour un fait.
    dim_lookups : dict {fk_col: (dim_df, dim_key_col)} pour les contrôles d'orphelins.
    """
    results = [check_row_count(fact_df, table_name)]
    results.append(check_duplicates_on_key(fact_df, grain_cols, table_name, blocking=blocking_grain))
    results.append(check_nulls(fact_df, mandatory_fk_cols, table_name, max_null_pct=0.0))
    for fk_col, (dim_df, dim_key_col) in dim_lookups.items():
        results.append(check_orphan_fk(fact_df, fk_col, dim_df, dim_key_col, table_name, blocking=fk_col in mandatory_fk_cols))
    if monetary_cols:
        results.append(check_negative_values(fact_df, monetary_cols, table_name, blocking=False))
    return results