-- ============================================================
-- PAYZENHR — SOCLE BI — CREATION DES TABLES CLICKHOUSE
-- ============================================================
-- A exécuter AVANT le script etl/etl_socle_bi.py
-- (le script Python écrit en mode "append", il ne crée pas
-- les tables lui-même — voir CORRECTION #7 du script ETL).
--
-- Base cible : payzen (cf. clickhouse_url dans etl_socle_bi.py)
-- ============================================================

CREATE DATABASE IF NOT EXISTS payzen;

-- ------------------------------------------------------------
-- DIM_CLIENTS
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS payzen.dim_clients
(
    client_id   Int64,
    nom         String,
    secteur     Nullable(String),
    taille      Nullable(String),
    cabinet_id  Int64
)
ENGINE = MergeTree()
ORDER BY (client_id);


-- ------------------------------------------------------------
-- DIM_EMPLOYEES
-- ------------------------------------------------------------
-- poste / niveau / date_embauche : NULL tant que la source n'est
-- pas confirmée avec Mohamed (voir commentaires etl_socle_bi.py).
CREATE TABLE IF NOT EXISTS payzen.dim_employees
(
    employee_id       Int64,
    nom               String,
    client_id         Int64,
    departement_id    Nullable(Int64),
    service           Nullable(String),
    matricule         Nullable(String),
    date_creation     Nullable(DateTime),
    date_suppression  Nullable(DateTime),
    poste             Nullable(String),
    niveau            Nullable(String),
    date_embauche     Nullable(DateTime)
)
ENGINE = MergeTree()
ORDER BY (client_id, employee_id);


-- ------------------------------------------------------------
-- DIM_CALENDAR
-- ------------------------------------------------------------
-- "jour" est toujours 1 (proxy mensuel) : la source PayrollResults
-- ne fournit que Month/Year, pas de vraie date journalière.
CREATE TABLE IF NOT EXISTS payzen.dim_calendar
(
    date        Date,
    jour        UInt8,
    mois        UInt8,
    trimestre   UInt8,
    annee       UInt16
)
ENGINE = MergeTree()
ORDER BY (date);


-- ------------------------------------------------------------
-- DIM_COMPOSANT_PAIE
-- ------------------------------------------------------------
-- composant_id = ComponentType (PayComponents est vide côté source,
-- à valider avec Mohamed — voir cahier des charges, "Étape 0").
CREATE TABLE IF NOT EXISTS payzen.dim_composant_paie
(
    composant_id  String,
    libelle       String,
    categorie     String
)
ENGINE = MergeTree()
ORDER BY (composant_id);


-- ------------------------------------------------------------
-- FACT_BULLETIN_LIGNE
-- ------------------------------------------------------------
-- Grain : 1 composant de paie, pour 1 collaborateur, pour 1 mois
-- (conforme au cahier des charges — remplace fact_bulletins).
CREATE TABLE IF NOT EXISTS payzen.fact_bulletin_ligne
(
    employee_id         Nullable(Int64),
    employee_name       Nullable(String),
    client_id           Int64,
    client_name         String,
    service             Nullable(String),
    composant_id        String,
    composant_libelle   String,
    montant             Decimal(18, 2),
    employee_salary_id  Int64,
    date_effet          Nullable(DateTime),
    date_fin             Nullable(DateTime),
    is_taxable           Nullable(UInt8),
    is_social             Nullable(UInt8),
    is_cimr                Nullable(UInt8),
    mois                    Nullable(UInt8),
    annee                    Nullable(UInt16)
)
ENGINE = MergeTree()
ORDER BY (client_id, annee, mois, composant_id);

-- ============================================================
-- FIN — 5 tables créées, conformes au socle défini dans
-- PayZen_Cahier_Charges_v3_Architecture_BI.md
-- ============================================================
