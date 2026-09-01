USE test_db;
GO

/* ============================================================
   1. TABLES PRINCIPALES
   ============================================================ */

SELECT
    TABLE_SCHEMA,
    TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;


/* ============================================================
   2. COLONNES DES TABLES RH / PAIE
   ============================================================ */

SELECT
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME IN
(
    'Companies',
    'Employees',
    'Departements',
    'JobPositions',
    'EmployeeContracts',
    'ContractTypes',
    'EmployeeSalaries',
    'EmployeeSalaryComponents',
    'PayrollResults',
    'PayrollResultPrimes',
    'PayComponents',
    'ReferentielElements',
    'SalaryPackages',
    'SalaryPackageItems'
)
ORDER BY
    TABLE_NAME,
    ORDINAL_POSITION;


/* ============================================================
   3. RELATIONS DES TABLES DE PAIE
   ============================================================ */

SELECT
    fk.name AS FK_Name,
    OBJECT_NAME(fk.parent_object_id) AS Table_Source,
    COL_NAME(fkc.parent_object_id, fkc.parent_column_id)
        AS Colonne_Source,
    OBJECT_NAME(fk.referenced_object_id) AS Table_Cible,
    COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id)
        AS Colonne_Cible
FROM sys.foreign_keys fk
INNER JOIN sys.foreign_key_columns fkc
    ON fk.object_id = fkc.constraint_object_id
WHERE
    OBJECT_NAME(fk.parent_object_id) IN
    (
        'PayrollResults',
        'PayrollResultPrimes',
        'EmployeeSalaries',
        'EmployeeSalaryComponents',
        'SalaryPackageItems',
        'SalaryPackages'
    )
ORDER BY Table_Source;