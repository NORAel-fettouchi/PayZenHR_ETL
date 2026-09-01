CREATE DATABASE IF NOT EXISTS payzen_bi;

USE payzen_bi;


/* ============================================================
   DIM CLIENTS
   ============================================================ */

CREATE TABLE IF NOT EXISTS dim_clients
(
    client_id UInt64,
    nom String,
    secteur Nullable(String),
    taille Nullable(String),
    cabinet_id Nullable(UInt64)
)
ENGINE = MergeTree
ORDER BY client_id;


/* ============================================================
   DIM EMPLOYEES
   ============================================================ */

CREATE TABLE IF NOT EXISTS dim_employees
(
    employee_id UInt64,
    nom String,
    poste Nullable(String),
    niveau Nullable(String),
    service Nullable(String),
    date_embauche Nullable(Date)
)
ENGINE = MergeTree
ORDER BY employee_id;


/* ============================================================
   DIM CALENDAR
   ============================================================ */

CREATE TABLE IF NOT EXISTS dim_calendar
(
    date Date,
    jour UInt8,
    mois UInt8,
    trimestre UInt8,
    annee UInt16
)
ENGINE = MergeTree
ORDER BY date;


/* ============================================================
   DIM COMPOSANT PAIE
   ============================================================ */

CREATE TABLE IF NOT EXISTS dim_composant_paie
(
    composant_id UInt64,
    libelle String,
    categorie String
)
ENGINE = MergeTree
ORDER BY composant_id;


/* ============================================================
   DIM ECHEANCE LEGALE
   ============================================================ */

CREATE TABLE IF NOT EXISTS dim_echeance_legale
(
    echeance_id UInt64,
    type String,
    periodicite String,
    regle_date_limite String
)
ENGINE = MergeTree
ORDER BY echeance_id;


/* ============================================================
   FACT BULLETIN LIGNE
   ============================================================ */

CREATE TABLE IF NOT EXISTS fact_bulletin_ligne
(
    employee_id UInt64,
    client_id UInt64,
    composant_id UInt64,
    mois Date,
    montant Decimal(18,2)
)
ENGINE = MergeTree
ORDER BY
(
    client_id,
    employee_id,
    mois,
    composant_id
);