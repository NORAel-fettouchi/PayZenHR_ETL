USE payzen_bi;


/* ============================================================
   DIM CLIENTS
   ============================================================ */

SELECT
    count() AS nombre_clients
FROM dim_clients;


/* ============================================================
   DIM EMPLOYEES
   ============================================================ */

SELECT
    count() AS nombre_employes
FROM dim_employees;


/* ============================================================
   CALENDAR
   ============================================================ */

SELECT
    min(date) AS date_min,
    max(date) AS date_max,
    count() AS nombre_dates
FROM dim_calendar;


/* ============================================================
   COMPOSANTS DE PAIE
   ============================================================ */

SELECT
    count() AS nombre_composants
FROM dim_composant_paie;


/* ============================================================
   FACT
   ============================================================ */

SELECT
    count() AS nombre_lignes,
    min(mois) AS premier_mois,
    max(mois) AS dernier_mois,
    sum(montant) AS montant_total
FROM fact_bulletin_ligne;


/* ============================================================
   CONTROLE DU GRAIN
   ============================================================ */

SELECT
    employee_id,
    client_id,
    composant_id,
    mois,
    count() AS nombre
FROM fact_bulletin_ligne
GROUP BY
    employee_id,
    client_id,
    composant_id,
    mois
HAVING nombre > 1
ORDER BY nombre DESC;