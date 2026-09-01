"""
transform/transformations.py
=============================
Fonctions generiques et reutilisables de nettoyage/transformation, appliquees
par transform/clean_dimensions.py et transform/clean_facts.py.

Aucune fonction ci-dessous n'est specifique a une table : elles sont ecrites une
fois et reutilisees partout, conformement au plan de nettoyage (Partie 2) et au
plan de jointures (Partie 3) de la conception.

Aucun collect() : toutes les operations restent des transformations
DataFrame->DataFrame executees de maniere distribuee. Les seules actions
(count, show) sont reservees au module quality.
"""

import logging
from typing import List

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, BooleanType, DateType

logger = logging.getLogger("payzenhr.transform")


# ---------------------------------------------------------------------------
# Nettoyage de texte generique
# ---------------------------------------------------------------------------

def trim_columns(df: DataFrame, columns: List[str]) -> DataFrame:
    """Supprime les espaces en debut/fin sur les colonnes texte indiquees."""
    for col in columns:
        if col in df.columns:
            df = df.withColumn(col, F.trim(F.col(col)))
    return df


def normalize_title_case(df: DataFrame, columns: List[str]) -> DataFrame:
    """Normalise la casse (Title Case) sur des colonnes texte (noms, libelles)."""
    for col in columns:
        if col in df.columns:
            df = df.withColumn(col, F.initcap(F.trim(F.col(col))))
    return df


def normalize_upper_case(df: DataFrame, columns: List[str]) -> DataFrame:
    """Normalise en majuscules (codes : devise, pays, matricule)."""
    for col in columns:
        if col in df.columns:
            df = df.withColumn(col, F.upper(F.trim(F.col(col))))
    return df


# ---------------------------------------------------------------------------
# Conversion de types
# ---------------------------------------------------------------------------

def cast_decimal(df: DataFrame, columns: List[str], precision: int = 18, scale: int = 2) -> DataFrame:
    """Cast strict des colonnes monetaires/mesures en Decimal(precision, scale)."""
    for col in columns:
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(DecimalType(precision, scale)))
    return df


def cast_boolean(df: DataFrame, columns: List[str]) -> DataFrame:
    """Cast strict des colonnes booleennes (AffectsPayroll, IsTaxable, etc.)."""
    for col in columns:
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(BooleanType()))
    return df


def cast_date(df: DataFrame, columns: List[str]) -> DataFrame:
    """Cast strict des colonnes date."""
    for col in columns:
        if col in df.columns:
            df = df.withColumn(col, F.to_date(F.col(col)))
    return df


# ---------------------------------------------------------------------------
# Colonnes techniques
# ---------------------------------------------------------------------------

def drop_technical_columns(df: DataFrame, columns: List[str]) -> DataFrame:
    """Supprime les colonnes techniques EF Core (CreatedAt/By, UpdatedAt/By, DeletedAt/By, ...)."""
    existing = [c for c in columns if c in df.columns]
    if existing:
        df = df.drop(*existing)
    return df


def rename_columns(df: DataFrame, mapping: dict) -> DataFrame:
    """Renomme des colonnes source -> cible selon un dictionnaire {source: cible}."""
    for source, target in mapping.items():
        if source in df.columns:
            df = df.withColumnRenamed(source, target)
    return df


# ---------------------------------------------------------------------------
# Cle technique (surrogate key)
# ---------------------------------------------------------------------------

def add_surrogate_key(
    df: DataFrame,
    key_name: str,
    business_key_col: str
) -> DataFrame:

    window = Window.orderBy(F.col(business_key_col))

    return df.withColumn(
        key_name,
        F.row_number().over(window).cast("long")
    )


# ---------------------------------------------------------------------------
# Resolution de cle etrangere (denormalisation vers une dimension deja construite)
# ---------------------------------------------------------------------------

def resolve_fk(
    df: DataFrame,
    dim_df: DataFrame,
    fk_col: str,
    dim_bk_col: str,
    dim_sk_col: str,
    output_col: str,
    keep_unmatched: bool = True,
) -> DataFrame:
    """
    Resout une cle etrangere source vers la cle technique (sk_*) d'une dimension
    deja construite, par LEFT JOIN sur la cle metier.

    keep_unmatched=True (par defaut) : une FK orpheline ou NULL produit un
    output_col NULL (jamais une valeur inventee) - conforme a la regle
    "pas de remplacement automatique des NULL".
    """
    dim_small = dim_df.select(
        F.col(dim_bk_col).alias("__dim_bk__"),
        F.col(dim_sk_col).alias(output_col),
    )
    joined = df.join(
        dim_small,
        df[fk_col] == dim_small["__dim_bk__"],
        how="left",
    ).drop("__dim_bk__")

    if not keep_unmatched:
        joined = joined.filter(F.col(output_col).isNotNull())

    return joined


# ---------------------------------------------------------------------------
# D2 - selection de la ligne la plus recente par groupe (adresse principale,
# version SCD2 courante) - evite la duplication documentee en Partie 1/2/3.
# ---------------------------------------------------------------------------

def select_latest_row_per_group(
    df: DataFrame,
    partition_cols: List[str],
    order_col: str,
    descending: bool = True,
) -> DataFrame:
    """
    Pour chaque groupe (ex. EmployeeId), ne conserve que la ligne la plus
    recente selon order_col (ex. CreatedAt). Utilise pour D2 (adresse
    principale) et pour toute selection "version courante" d'une SCD2.
    """
    order_expr = F.col(order_col).desc() if descending else F.col(order_col).asc()
    window = Window.partitionBy(*partition_cols).orderBy(order_expr)
    return (
        df.withColumn("__rn__", F.row_number().over(window))
        .filter(F.col("__rn__") == 1)
        .drop("__rn__")
    )


# ---------------------------------------------------------------------------
# D1 - resolution temporelle contrat/bulletin (jointure d'intervalle)
# ---------------------------------------------------------------------------

def temporal_join_contract(
    fact_df: DataFrame,
    contract_df: DataFrame,
    employee_col: str,
    fact_date_col: str,
    contract_employee_col: str = "EmployeeId",
    contract_start_col: str = "StartDate",
    contract_end_col: str = "EndDate",
) -> DataFrame:
    """
    Resout le contrat (poste, type de contrat) en vigueur a la date d'un
    bulletin de paie (decision D1 de la conception).

    - jointure sur EmployeeId + condition d'intervalle (StartDate <= date
      <= EndDate ou EndDate NULL) ;
    - en cas de chevauchement de contrats (anomalie source), on ne garde que
      le contrat dont le StartDate est le plus recent, et on logue le nombre
      de cas concernes (cf. controle qualite J15 / Partie 5).
    """
    condition = (
        (fact_df[employee_col] == contract_df[contract_employee_col])
        & (contract_df[contract_start_col] <= fact_df[fact_date_col])
        & (
            contract_df[contract_end_col].isNull()
            | (contract_df[contract_end_col] >= fact_df[fact_date_col])
        )
    )

    joined = fact_df.join(contract_df, condition, how="left")

    window = Window.partitionBy(
        fact_df[employee_col], fact_df[fact_date_col]
    ).orderBy(F.col(contract_start_col).desc())

    joined = joined.withColumn("__rn__", F.row_number().over(window))

    overlap_count = joined.filter(F.col("__rn__") > 1).count()
    if overlap_count > 0:
        logger.warning(
            "Chevauchement de contrats detecte sur %s combinaisons (employe, date bulletin) "
            "- seul le contrat au StartDate le plus recent est conserve. "
            "A signaler comme anomalie de donnees source (cf. controle qualite J15).",
            overlap_count,
        )

    return joined.filter(F.col("__rn__") == 1).drop("__rn__")


# ---------------------------------------------------------------------------
# Calculs metier
# ---------------------------------------------------------------------------

def compute_age(df: DataFrame, birth_date_col: str, output_col: str = "age") -> DataFrame:
    """Calcule l'age en annees a partir de DateOfBirth, sans modifier la colonne source."""
    return df.withColumn(
        output_col,
        F.floor(F.datediff(F.current_date(), F.col(birth_date_col)) / 365.25).cast("int"),
    )


def compute_age_bracket(df: DataFrame, age_col: str = "age", output_col: str = "age_bracket") -> DataFrame:
    """Regroupe l'age calcule en tranches (pyramide des ages, KPI RH)."""
    return df.withColumn(
        output_col,
        F.when(F.col(age_col).isNull(), None)
        .when(F.col(age_col) < 25, "< 25 ans")
        .when(F.col(age_col) < 35, "25-34 ans")
        .when(F.col(age_col) < 45, "35-44 ans")
        .when(F.col(age_col) < 55, "45-54 ans")
        .otherwise("55 ans et +"),
    )


def denormalize_reference(
    df: DataFrame,
    ref_df: DataFrame,
    fk_col: str,
    ref_key_col: str,
    ref_label_col: str,
    output_col: str,
) -> DataFrame:
    """
    Denormalise le libelle d'une table de reference Groupe C (Genders,
    Nationalities, ...) directement dans la dimension appelante, par LEFT JOIN.
    Le NULL est conserve si la FK est NULL (aucune valeur inventee ici -
    la conversion eventuelle en "Non renseigne" est faite explicitement au
    niveau de clean_dimensions, table par table, jamais ici de maniere generique).
    """
    ref_small = ref_df.select(
        F.col(ref_key_col).alias("__ref_key__"),
        F.col(ref_label_col).alias(output_col),
    )
    joined = df.join(ref_small, df[fk_col] == ref_small["__ref_key__"], how="left")
    return joined.drop("__ref_key__")


def fill_unknown_for_grouping_axis(df: DataFrame, column: str, label: str = "Non renseigne") -> DataFrame:
    """
    Remplace un NULL par un libelle "Non renseigne" UNIQUEMENT pour les colonnes
    explicitement designees comme axe de regroupement obligatoire d'un KPI
    catalogue comme calculable (ex. genre, nationalite).
    Ne doit JAMAIS etre appelee sur les colonnes a reserve forte (departement,
    statut marital, categorie) - cf. Partie 2 / decision D6.
    """
    return df.withColumn(column, F.coalesce(F.col(column), F.lit(label)))


def flag_out_of_range(
    df: DataFrame, column: str, min_value=None, max_value=None, flag_col: str = None
) -> DataFrame:
    """
    Ajoute une colonne booleenne signalant une valeur hors plage plausible
    (age, montant negatif, etc.), sans jamais corriger ou supprimer la ligne
    automatiquement - conforme a la regle "flag, jamais correction silencieuse".
    """
    flag_col = flag_col or f"{column}_out_of_range"
    condition = F.lit(False)
    if min_value is not None:
        condition = condition | (F.col(column) < min_value)
    if max_value is not None:
        condition = condition | (F.col(column) > max_value)
    return df.withColumn(flag_col, condition)