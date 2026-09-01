-- ============================================================================
-- PayZenHR -- Data Warehouse ClickHouse -- Script de creation du schema en etoile
-- Genere a partir de : Audit_Structurel_BDD.md, Modele_DW_Groupes_ABCD.md,
-- Catalogue_KPI.md, PayZenHR_Conception_DW.md (fournis par l'utilisateur)
-- Aucune colonne ni relation non presente dans le profilage source n'est ajoutee.
-- ============================================================================

CREATE DATABASE IF NOT EXISTS payzenhr_dw;


-- ====================================================================================================
-- DIMENSION : dim_date (construite, hors source -- cf. Conception 1.2 / 2.12)
-- ====================================================================================================

DROP TABLE IF EXISTS payzenhr_dw.dim_date;
CREATE TABLE payzenhr_dw.dim_date
(
    `sk_date` UInt32 COMMENT 'Cle technique format AAAAMMJJ',
    `full_date` Date COMMENT 'Date calendaire',
    `annee_civile` UInt16,
    `mois` UInt8 COMMENT '1-12',
    `nom_mois` LowCardinality(String),
    `trimestre` UInt8 COMMENT '1-4',
    `jour_semaine` UInt8 COMMENT '1=Lundi ... 7=Dimanche (ISO)',
    `nom_jour_semaine` LowCardinality(String),
    `numero_semaine` UInt8,
    `est_weekend` Bool,
    `premier_jour_mois` Date COMMENT 'Utilise pour joindre les faits mensuels (grain mois)'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_date)
COMMENT 'Dimension calendaire construite, plage 2019-01-01 a 2027-12-31 (cf. D3 : pas d''attribut ferie generique, Holidays reste table satellite par societe)';


-- ====================================================================================================
-- DIMENSION : dim_country (<- Countries) -- racine geographique partagee (D5)
-- ====================================================================================================

DROP TABLE IF EXISTS payzenhr_dw.dim_country;
CREATE TABLE payzenhr_dw.dim_country
(
    `sk_country` UInt32,
    `bk_country_id` UInt32 COMMENT 'Countries.Id',
    `country_name` Nullable(String) COMMENT 'Countries.CountryName',
    `country_name_ar` Nullable(String) COMMENT 'Countries.CountryNameAr',
    `country_code` LowCardinality(Nullable(String)) COMMENT 'Countries.CountryCode',
    `country_phone_code` Nullable(String) COMMENT 'Countries.CountryPhoneCode'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_country)
COMMENT 'Referentiel pays (2 lignes au profilage) - partage entre dim_city, dim_company, dim_employee';


-- ====================================================================================================
-- DIMENSION : dim_city (<- Cities) -- partagee entre dim_employee et dim_company (D5)
-- ====================================================================================================

DROP TABLE IF EXISTS payzenhr_dw.dim_city;
CREATE TABLE payzenhr_dw.dim_city
(
    `sk_city` UInt32,
    `bk_city_id` UInt32 COMMENT 'Cities.Id',
    `city_name` Nullable(String) COMMENT 'Cities.CityName',
    `sk_country` Nullable(UInt32) COMMENT 'FK vers dim_country, resolue via Cities.CountryId ; orpheline -> NULL'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_city)
COMMENT 'Referentiel villes (37 lignes au profilage)';


-- ====================================================================================================
-- DIMENSION : dim_company (<- Companies) -- cf. Conception 1.2 / 2.2
-- ====================================================================================================

DROP TABLE IF EXISTS payzenhr_dw.dim_company;
CREATE TABLE payzenhr_dw.dim_company
(
    `sk_company` UInt32,
    `bk_company_id` UInt32 COMMENT 'Companies.Id',
    `company_name` Nullable(String) COMMENT 'Companies.CompanyName',
    `ice_number` Nullable(String) COMMENT 'Companies.IceNumber - identifiant legal, conserve en texte',
    `cnss_number` Nullable(String) COMMENT 'Companies.CnssNumber',
    `if_number` Nullable(String) COMMENT 'Companies.IfNumber',
    `rc_number` Nullable(String) COMMENT 'Companies.RcNumber',
    `legal_form` Nullable(String) COMMENT 'Companies.LegalForm',
    `currency_code` LowCardinality(Nullable(String)) COMMENT 'Companies.Currency',
    `payroll_periodicity` LowCardinality(Nullable(String)) COMMENT 'Companies.PayrollPeriodicity',
    `fiscal_year_start_month` Nullable(UInt8) COMMENT 'Companies.FiscalYearStartMonth',
    `business_sector` Nullable(String) COMMENT 'Companies.BusinessSector - texte libre, aucun referentiel source',
    `is_cabinet_expert` Nullable(Bool) COMMENT 'Companies.IsCabinetExpert',
    `bk_managed_by_company_id` Nullable(UInt32) COMMENT 'Companies.ManagedByCompanyId - auto-reference, NULL = societe racine',
    `is_active` Nullable(Bool) COMMENT 'Companies.isActive',
    `sk_city` Nullable(UInt32) COMMENT 'FK dim_city, resolue via Companies.CityId'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_company)
COMMENT 'Dimension transverse a tous les faits (29 FK entrantes au profilage) - axe multi-tenant obligatoire';


-- ====================================================================================================
-- DIMENSION : dim_department (<- Departements) -- cf. Conception 1.2 / 2.3
-- ====================================================================================================

DROP TABLE IF EXISTS payzenhr_dw.dim_department;
CREATE TABLE payzenhr_dw.dim_department
(
    `sk_department` UInt32,
    `bk_department_id` UInt32 COMMENT 'Departements.Id',
    `department_name` Nullable(String) COMMENT 'Departements.DepartementName - unicite non garantie, ne jamais dedupliquer sur ce libelle',
    `sk_company` Nullable(UInt32) COMMENT 'FK dim_company, resolue via Departements.CompanyId',
    `hierarchy_level` Nullable(Int16) COMMENT 'Departements.HierarchyLevel',
    `bk_parent_department_id` Nullable(UInt32) COMMENT 'Departements.ParentDepartementId - NULL = departement racine (etat normal)',
    `bk_manager_employee_id` Nullable(UInt32) COMMENT 'Departements.ManagerId - taux de remplissage a verifier en controle qualite',
    `description` Nullable(String) COMMENT 'Departements.Description'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_department)
COMMENT '585 departements au profilage. RESERVE : Employees.DepartementId est NULL a 90,52% -> couverture reelle limitee sur les faits';


-- ====================================================================================================
-- DIMENSION : dim_job_position (<- JobPositions) -- cf. Conception 1.2 / 2.4
-- ====================================================================================================

DROP TABLE IF EXISTS payzenhr_dw.dim_job_position;
CREATE TABLE payzenhr_dw.dim_job_position
(
    `sk_job_position` UInt32,
    `bk_job_position_id` UInt32 COMMENT 'JobPositions.Id',
    `job_position_name` Nullable(String) COMMENT 'JobPositions.Name',
    `sk_company` Nullable(UInt32) COMMENT 'FK dim_company, resolue via JobPositions.CompanyId'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_job_position)
COMMENT '387 postes. Accessible uniquement via EmployeeContracts (pas de FK directe sur Employees ni PayrollResults)';


-- ====================================================================================================
-- DIMENSION : dim_contract_type (<- ContractTypes + LegalContractTypes + StateEmploymentPrograms) -- 2.4
-- ====================================================================================================

DROP TABLE IF EXISTS payzenhr_dw.dim_contract_type;
CREATE TABLE payzenhr_dw.dim_contract_type
(
    `sk_contract_type` UInt32,
    `bk_contract_type_id` UInt32 COMMENT 'ContractTypes.Id',
    `contract_type_name` Nullable(String) COMMENT 'ContractTypes.ContractTypeName',
    `sk_company` Nullable(UInt32) COMMENT 'FK dim_company, resolue via ContractTypes.CompanyId',
    `legal_contract_type_code` LowCardinality(Nullable(String)) COMMENT 'LegalContractTypes.Code (denormalise)',
    `legal_contract_type_name` Nullable(String) COMMENT 'LegalContractTypes.Name (denormalise, 5 valeurs)',
    `state_employment_program_code` LowCardinality(Nullable(String)) COMMENT 'StateEmploymentPrograms.Code (denormalise)',
    `state_employment_program_name` Nullable(String) COMMENT 'StateEmploymentPrograms.Name (denormalise, 3 valeurs) - NULL = pas de programme rattache (cas normal)',
    `state_program_is_ir_exempt` Nullable(Bool) COMMENT 'StateEmploymentPrograms.IsIrExempt',
    `state_program_is_cnss_employee_exempt` Nullable(Bool) COMMENT 'StateEmploymentPrograms.IsCnssEmployeeExempt',
    `state_program_is_cnss_employer_exempt` Nullable(Bool) COMMENT 'StateEmploymentPrograms.IsCnssEmployerExempt'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_contract_type)
COMMENT '174 types de contrat. Denormalisation a un seul niveau (faible cardinalite des tables satellites)';


-- ====================================================================================================
-- DIMENSION SATELLITE (SCD2) : dim_employee_contract (<- EmployeeContracts) -- cf. Conception 1.2/2.5, D1
-- ====================================================================================================

DROP TABLE IF EXISTS payzenhr_dw.dim_employee_contract;
CREATE TABLE payzenhr_dw.dim_employee_contract
(
    `sk_employee_contract` UInt32,
    `bk_contract_id` UInt32 COMMENT 'EmployeeContracts.Id',
    `sk_employee` UInt32 COMMENT 'FK dim_employee, resolue via EmployeeContracts.EmployeeId - jamais NULL attendu',
    `sk_company` Nullable(UInt32) COMMENT 'FK dim_company, resolue via EmployeeContracts.CompanyId',
    `sk_job_position` Nullable(UInt32) COMMENT 'FK dim_job_position, resolue via EmployeeContracts.JobPositionId ; FK orpheline -> NULL + flag qualite',
    `sk_contract_type` Nullable(UInt32) COMMENT 'FK dim_contract_type, resolue via EmployeeContracts.ContractTypeId',
    `start_date` Date COMMENT 'EmployeeContracts.StartDate - jamais NULL (un contrat a toujours une date de debut)',
    `end_date` Nullable(Date) COMMENT 'EmployeeContracts.EndDate - NULL conserve = contrat en cours (80,78% des cas), etat normal',
    `exoneration_end_date` Nullable(Date) COMMENT 'EmployeeContracts.ExonerationEndDate'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_employee_contract)
COMMENT '1337 contrats (SCD2). Ne recoit aucun fait directement - sert de table de resolution temporelle pour fact_payroll (D1)';


-- ====================================================================================================
-- DIMENSION SATELLITE (SCD2) : dim_employee_salary (<- EmployeeSalaries) -- cf. Conception 1.2/2.6
-- ====================================================================================================

DROP TABLE IF EXISTS payzenhr_dw.dim_employee_salary;
CREATE TABLE payzenhr_dw.dim_employee_salary
(
    `sk_employee_salary` UInt32,
    `bk_employee_salary_id` UInt32 COMMENT 'EmployeeSalaries.Id',
    `sk_employee` UInt32 COMMENT 'FK dim_employee, resolue via EmployeeSalaries.EmployeeId',
    `bk_contract_id` Nullable(UInt32) COMMENT 'EmployeeSalaries.ContractId',
    `base_salary` Nullable(Decimal(18,2)) COMMENT 'EmployeeSalaries.BaseSalary - NULL = probleme potentiel a signaler, jamais remplace par 0',
    `base_salary_hourly` Nullable(Decimal(18,4)) COMMENT 'EmployeeSalaries.BaseSalaryHourly',
    `effective_date` Date COMMENT 'EmployeeSalaries.EffectiveDate - jamais NULL',
    `end_date` Nullable(Date) COMMENT 'EmployeeSalaries.EndDate - NULL conserve = salaire courant (87,10% des cas)'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_employee_salary)
COMMENT '1225 versions de salaire (SCD2). Aucun fait direct - parent de fact_salary_component, alimente le KPI evolution du salaire de base';


-- ====================================================================================================
-- DIMENSION : dim_status (<- Statuses) -- cf. Conception 1.2/2.7
-- ====================================================================================================

DROP TABLE IF EXISTS payzenhr_dw.dim_status;
CREATE TABLE payzenhr_dw.dim_status
(
    `sk_status` UInt32,
    `bk_status_id` UInt32 COMMENT 'Statuses.Id',
    `status_code` LowCardinality(Nullable(String)) COMMENT 'Statuses.Code',
    `status_name_fr` Nullable(String) COMMENT 'Statuses.NameFr',
    `status_name_ar` Nullable(String) COMMENT 'Statuses.NameAr',
    `status_name_en` Nullable(String) COMMENT 'Statuses.NameEn',
    `is_active` Nullable(Bool) COMMENT 'Statuses.IsActive',
    `affects_payroll` Nullable(Bool) COMMENT 'Statuses.AffectsPayroll - flag central du KPI effectif actif',
    `affects_attendance` Nullable(Bool) COMMENT 'Statuses.AffectsAttendance',
    `affects_access` Nullable(Bool) COMMENT 'Statuses.AffectsAccess'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_status)
COMMENT '5 statuts. Seule dimension avec des flags metier directement exploitables sans transformation';


-- ====================================================================================================
-- DIMENSION : dim_leave_type (<- LeaveTypes + LeaveTypePolicies) -- cf. Conception 1.2/2.8
-- ====================================================================================================

DROP TABLE IF EXISTS payzenhr_dw.dim_leave_type;
CREATE TABLE payzenhr_dw.dim_leave_type
(
    `sk_leave_type` UInt32,
    `bk_leave_type_id` UInt32 COMMENT 'LeaveTypes.Id',
    `leave_code` LowCardinality(Nullable(String)) COMMENT 'LeaveTypes.LeaveCode',
    `leave_type_name_fr` Nullable(String) COMMENT 'LeaveTypes.LeaveNameFr',
    `leave_type_name_ar` Nullable(String) COMMENT 'LeaveTypes.LeaveNameAr',
    `leave_type_name_en` Nullable(String) COMMENT 'LeaveTypes.LeaveNameEn',
    `leave_description` Nullable(String) COMMENT 'LeaveTypes.LeaveDescription',
    `scope` Nullable(Int16) COMMENT 'LeaveTypes.Scope',
    `is_active` Nullable(Bool) COMMENT 'LeaveTypes.IsActive',
    `sk_company` Nullable(UInt32) COMMENT 'FK dim_company, resolue via LeaveTypes.CompanyId',
    `policy_is_enabled` Nullable(Bool) COMMENT 'LeaveTypePolicies.IsEnabled (denormalise, 6 lignes)',
    `policy_requires_balance` Nullable(Bool) COMMENT 'LeaveTypePolicies.RequiresBalance',
    `policy_allow_negative_balance` Nullable(Bool) COMMENT 'LeaveTypePolicies.AllowNegativeBalance',
    `policy_is_paid` Nullable(Bool) COMMENT 'LeaveTypePolicies.IsPaid',
    `policy_requires_eligibility_6months` Nullable(Bool) COMMENT 'LeaveTypePolicies.RequiresEligibility6Months',
    `policy_accrual_method` Nullable(Int16) COMMENT 'LeaveTypePolicies.AccrualMethod',
    `policy_days_per_month_adult` Nullable(Decimal(5,2)) COMMENT 'LeaveTypePolicies.DaysPerMonthAdult',
    `policy_days_per_month_minor` Nullable(Decimal(5,2)) COMMENT 'LeaveTypePolicies.DaysPerMonthMinor',
    `policy_bonus_days_after_5years` Nullable(Decimal(5,2)) COMMENT 'LeaveTypePolicies.BonusDaysPerYearAfter5Years',
    `policy_annual_cap_days` Nullable(Int16) COMMENT 'LeaveTypePolicies.AnnualCapDays',
    `policy_allow_carryover` Nullable(Bool) COMMENT 'LeaveTypePolicies.AllowCarryover',
    `policy_max_carryover_years` Nullable(Int16) COMMENT 'LeaveTypePolicies.MaxCarryoverYears',
    `policy_min_consecutive_days` Nullable(Int16) COMMENT 'LeaveTypePolicies.MinConsecutiveDays',
    `policy_use_working_calendar` Nullable(Bool) COMMENT 'LeaveTypePolicies.UseWorkingCalendar'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_leave_type)
COMMENT '7 types de conges, enrichis par leur politique (6 lignes) - conserve NULL si aucune politique definie (cas normal)';


-- ====================================================================================================
-- DIMENSION SATELLITE : dim_leave_type_legal_rule (<- LeaveTypeLegalRules) -- cf. Conception 1.2/2.8
-- ====================================================================================================

DROP TABLE IF EXISTS payzenhr_dw.dim_leave_type_legal_rule;
CREATE TABLE payzenhr_dw.dim_leave_type_legal_rule
(
    `sk_leave_legal_rule` UInt32,
    `bk_leave_legal_rule_id` UInt32 COMMENT 'LeaveTypeLegalRules.Id',
    `sk_leave_type` UInt32 COMMENT 'FK dim_leave_type (relation 1-N reelle : 10 regles / 7 types) - jamais aplatie dans dim_leave_type',
    `event_case_code` LowCardinality(Nullable(String)) COMMENT 'LeaveTypeLegalRules.EventCaseCode',
    `description` Nullable(String) COMMENT 'LeaveTypeLegalRules.Description',
    `days_granted` Nullable(Int16) COMMENT 'LeaveTypeLegalRules.DaysGranted - NULL = a signaler, 0 aurait un sens metier different',
    `legal_article` Nullable(String) COMMENT 'LeaveTypeLegalRules.LegalArticle',
    `can_be_discontinuous` Nullable(Bool) COMMENT 'LeaveTypeLegalRules.CanBeDiscountinuous',
    `must_be_used_within_days` Nullable(Int16) COMMENT 'LeaveTypeLegalRules.MustBeUsedWithinDays'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_leave_legal_rule)
COMMENT '10 regles legales. Table satellite separee (jamais aplatie dans dim_leave_type) pour ne pas dupliquer/perdre la granularite regle';


-- ====================================================================================================
-- DIMENSION SATELLITE DE CALCUL : dim_holiday (<- Holidays) -- cf. Conception 1.2/2.9, D3
-- ====================================================================================================

DROP TABLE IF EXISTS payzenhr_dw.dim_holiday;
CREATE TABLE payzenhr_dw.dim_holiday
(
    `sk_holiday` UInt32,
    `bk_holiday_id` UInt32 COMMENT 'Holidays.Id',
    `holiday_name_fr` Nullable(String) COMMENT 'Holidays.NameFr',
    `holiday_name_ar` Nullable(String) COMMENT 'Holidays.NameAr',
    `holiday_name_en` Nullable(String) COMMENT 'Holidays.NameEn',
    `holiday_date` Date COMMENT 'Holidays.HolidayDate - jamais NULL',
    `sk_company` Nullable(UInt32) COMMENT 'FK dim_company, resolue via Holidays.CompanyId - specifique par societe, cf. D3',
    `sk_country` Nullable(UInt32) COMMENT 'FK dim_country, resolue via Holidays.CountryId',
    `scope` Nullable(Int16) COMMENT 'Holidays.Scope',
    `holiday_type` LowCardinality(Nullable(String)) COMMENT 'Holidays.HolidayType',
    `is_mandatory` Nullable(Bool) COMMENT 'Holidays.IsMandatory',
    `is_paid` Nullable(Bool) COMMENT 'Holidays.IsPaid',
    `is_recurring` Nullable(Bool) COMMENT 'Holidays.IsRecurring',
    `recurrence_rule` Nullable(String) COMMENT 'Holidays.RecurrenceRule',
    `holiday_year` Nullable(Int16) COMMENT 'Holidays.Year',
    `affect_payroll` Nullable(Bool) COMMENT 'Holidays.AffectPayroll',
    `affect_attendance` Nullable(Bool) COMMENT 'Holidays.AffectAttendance',
    `is_active` Nullable(Bool) COMMENT 'Holidays.IsActive'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_holiday)
COMMENT '34 jours feries. Table satellite jointe par (Date, Company) au moment du calcul KPI - pas de FK entrante depuis un fait (D3)';


-- ====================================================================================================
-- DIMENSION SATELLITE DE CALCUL : dim_working_calendar (<- WorkingCalendars) -- cf. Conception 1.2/2.9, D3
-- ====================================================================================================

DROP TABLE IF EXISTS payzenhr_dw.dim_working_calendar;
CREATE TABLE payzenhr_dw.dim_working_calendar
(
    `sk_working_calendar` UInt32,
    `bk_working_calendar_id` UInt32 COMMENT 'WorkingCalendars.Id',
    `sk_company` Nullable(UInt32) COMMENT 'FK dim_company, resolue via WorkingCalendars.CompanyId - specifique par societe, cf. D3',
    `day_of_week` Nullable(UInt8) COMMENT 'WorkingCalendars.DayOfWeek (0-6)',
    `is_working_day` Nullable(Bool) COMMENT 'WorkingCalendars.IsWorkingDay',
    `start_time` Nullable(DateTime) COMMENT 'WorkingCalendars.StartTime',
    `end_time` Nullable(DateTime) COMMENT 'WorkingCalendars.EndTime'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_working_calendar)
COMMENT '325 lignes. Dimension de calcul jointe par (Date, Company), jamais ligne a ligne sur un fait (D3)';


-- ====================================================================================================
-- DIMENSION : dim_alert_definition (<- AlertDefinitions + AlertCategories + AlertSeverities) -- 1.2/2.10
-- ====================================================================================================

DROP TABLE IF EXISTS payzenhr_dw.dim_alert_definition;
CREATE TABLE payzenhr_dw.dim_alert_definition
(
    `sk_alert_definition` UInt32,
    `bk_alert_definition_id` UInt32 COMMENT 'AlertDefinitions.Id',
    `alert_definition_code` LowCardinality(Nullable(String)) COMMENT 'AlertDefinitions.Code',
    `alert_definition_name` Nullable(String) COMMENT 'AlertDefinitions.Name',
    `alert_definition_name_en` Nullable(String) COMMENT 'AlertDefinitions.NameEn',
    `alert_definition_name_ar` Nullable(String) COMMENT 'AlertDefinitions.NameAr',
    `alert_category_name` LowCardinality(Nullable(String)) COMMENT 'AlertCategories.Name (denormalise, 6 valeurs)',
    `alert_severity_code` LowCardinality(Nullable(String)) COMMENT 'AlertSeverities.Code (denormalise)',
    `alert_severity_label` LowCardinality(Nullable(String)) COMMENT 'AlertSeverities.Label (denormalise, conserve en clair : Info/Warning/Critical)',
    `alert_severity_level` Nullable(Int16) COMMENT 'AlertSeverities.Level',
    `trigger_type` LowCardinality(Nullable(String)) COMMENT 'AlertDefinitions.TriggerType',
    `applies_to` LowCardinality(Nullable(String)) COMMENT 'AlertDefinitions.AppliesTo',
    `threshold_value` Nullable(Decimal(18,2)) COMMENT 'AlertDefinitions.ThresholdValue',
    `is_active` Nullable(Bool) COMMENT 'AlertDefinitions.IsActive',
    `is_optional` Nullable(Bool) COMMENT 'AlertDefinitions.IsOptional',
    `requires_acknowledgement` Nullable(Bool) COMMENT 'AlertSeverities.RequiresAcknowledgement'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_alert_definition)
COMMENT '12 definitions d''alerte. Axe de filtre principal du dashboard Alertes (severite, categorie). Description/SourceField exclus (100% NULL)';


-- ====================================================================================================
-- DIMENSION CENTRALE : dim_employee (<- Employees + Genders + Nationalities + MaritalStatuses + EducationLevels + EmployeeAddresses) -- 1.2/2.1, D2, D6
-- ====================================================================================================

DROP TABLE IF EXISTS payzenhr_dw.dim_employee;
CREATE TABLE payzenhr_dw.dim_employee
(
    `sk_employee` UInt32,
    `bk_employee_id` UInt32 COMMENT 'Employees.Id - jamais NULL, ligne rejetee en staging sinon',
    `employee_matricule` Nullable(String) COMMENT 'Employees.Matricule - attribut d''affichage, PAS une cle (unicite non confirmee)',
    `first_name` Nullable(String) COMMENT 'Employees.FirstName',
    `last_name` Nullable(String) COMMENT 'Employees.LastName',
    `cin_number` Nullable(String) COMMENT 'Employees.CinNumber',
    `date_of_birth` Nullable(Date) COMMENT 'Employees.DateOfBirth',
    `email` Nullable(String) COMMENT 'Employees.Email',
    `phone` Nullable(String) COMMENT 'Employees.Phone',
    `gender_label` LowCardinality(Nullable(String)) COMMENT 'Genders.NameFr (denormalise, 2 valeurs) - ''Non renseigne'' si FK orpheline, NULL si FK NULL',
    `nationality_label` Nullable(String) COMMENT 'Nationalities.Name (denormalise, 5 valeurs)',
    `education_level_label` Nullable(String) COMMENT 'EducationLevels.NameFr (denormalise, 11 valeurs)',
    `marital_status_label` Nullable(String) COMMENT 'MaritalStatuses.NameFr (denormalise) - RESERVE : Employees.MaritalStatusId NULL a 85,27%, NULL conserve tel quel (jamais force a ''Non renseigne'')',
    `sk_status` Nullable(UInt32) COMMENT 'FK dim_status via Employees.StatusId - ne doit jamais etre NULL en theorie, flague en controle qualite sinon',
    `sk_company` UInt32 COMMENT 'FK dim_company via Employees.CompanyId - axe multi-tenant obligatoire, ne doit jamais etre NULL (ligne rejetee en controle qualite sinon)',
    `sk_department` Nullable(UInt32) COMMENT 'FK dim_department via Employees.DepartementId - RESERVE FORTE : NULL a 90,52%, conserve NULL (jamais de departement invente)',
    `sk_city` Nullable(UInt32) COMMENT 'FK dim_city via l''adresse principale selectionnee (regle D2 : adresse la plus recente par CreatedAt)',
    `address_line1` Nullable(String) COMMENT 'EmployeeAddresses.AddressLine1 (adresse principale, D2)',
    `zip_code` Nullable(String) COMMENT 'EmployeeAddresses.ZipCode (adresse principale, D2)',
    `cnss_number` Nullable(String) COMMENT 'Employees.CnssNumber',
    `cimr_number` Nullable(String) COMMENT 'Employees.CimrNumber',
    `cimr_employee_rate` Nullable(Decimal(5,4)) COMMENT 'Employees.CimrEmployeeRate',
    `cimr_company_rate` Nullable(Decimal(5,4)) COMMENT 'Employees.CimrCompanyRate',
    `has_private_insurance` Nullable(Bool) COMMENT 'Employees.HasPrivateInsurance',
    `disable_amo` Nullable(Bool) COMMENT 'Employees.DisableAmo',
    `payment_method` LowCardinality(Nullable(String)) COMMENT 'Employees.PaymentMethod',
    `annual_leave_opening_days` Nullable(Decimal(10,2)) COMMENT 'Employees.AnnualLeaveOpeningDays',
    `housing_deduction` Nullable(Decimal(18,2)) COMMENT 'Employees.HousingDeduction'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_employee)
COMMENT '1561 employes - dimension centrale du dashboard. CategoryId (99,74% NULL), ManagerId (100% NULL), CountryId sur Employees (100% NULL), BankAccountId (100% NULL) et PersonalEmail (100% NULL) sont EXCLUS (cf. D6 et Conception 2.1) - aucune valeur exploitable au profilage';


-- ====================================================================================================
-- FAIT : fact_payroll (<- PayrollResults) -- grain = 1 bulletin de paie (employe x mois x annee)
-- ====================================================================================================

DROP TABLE IF EXISTS payzenhr_dw.fact_payroll;
CREATE TABLE payzenhr_dw.fact_payroll
(
    `sk_payroll` UInt32,
    `bk_payroll_id` UInt32 COMMENT 'PayrollResults.Id',
    `sk_employee` UInt32 COMMENT 'FK dim_employee via PayrollResults.EmployeeId - jamais NULL attendu',
    `sk_company` UInt32 COMMENT 'FK dim_company via PayrollResults.CompanyId',
    `sk_date` UInt32 COMMENT 'FK dim_date (1er jour du mois), resolue depuis PayrollResults.Month/Year',
    `payroll_year` UInt16 COMMENT 'PayrollResults.Year - conserve aussi en clair pour partitionnement',
    `payroll_month` UInt8 COMMENT 'PayrollResults.Month',
    `sk_job_position_at_payroll` Nullable(UInt32) COMMENT 'Resolue en ETL par jointure temporelle sur EmployeeContracts (D1) - pas de FK source directe',
    `sk_contract_type_at_payroll` Nullable(UInt32) COMMENT 'Resolue en ETL par jointure temporelle sur EmployeeContracts (D1) - pas de FK source directe',
    `payroll_status` Nullable(Int16) COMMENT 'PayrollResults.Status - statut du calcul (degenere)',
    `pay_half` Nullable(Int16) COMMENT 'PayrollResults.PayHalf - degenere',
    `salaire_base` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.SalaireBase',
    `heures_supp25` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.HeuresSupp25',
    `heures_supp50` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.HeuresSupp50',
    `heures_supp100` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.HeuresSupp100',
    `conges` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.Conges',
    `jours_feries` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.JoursFeries',
    `prime_anciennete` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.PrimeAnciennete',
    `prime_anciennte_rate` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.PrimeAnciennteRate',
    `prime_imposable1` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.PrimeImposable1',
    `prime_imposable2` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.PrimeImposable2',
    `prime_imposable3` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.PrimeImposable3',
    `total_primes_imposables` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.TotalPrimesImposables',
    `total_brut` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.TotalBrut',
    `frais_professionnels` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.FraisProfessionnels',
    `indemnite_representation` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.IndemniteRepresentation',
    `prime_transport` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.PrimeTransport',
    `prime_panier` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.PrimePanier',
    `indemnite_deplacement` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.IndemniteDeplacement',
    `indemnite_caisse` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.IndemniteCaisse',
    `prime_salissure` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.PrimeSalissure',
    `gratifications_familial` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.GratificationsFamilial',
    `prime_voyage_mecque` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.PrimeVoyageMecque',
    `indemnite_licenciement` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.IndemniteLicenciement',
    `indemnite_kilometrique` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.IndemniteKilometrique',
    `prime_tourne` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.PrimeTourne',
    `prime_outillage` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.PrimeOutillage',
    `aide_medicale` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.AideMedicale',
    `autres_primes_non_imposable` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.AutresPrimesNonImposable',
    `total_indemnites` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.TotalIndemnites',
    `total_ni_excedent_imposable` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.TotalNiExcedentImposable',
    `cnss_part_salariale` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.CnssPartSalariale',
    `cnss_base` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.CnssBase',
    `cimr_part_salariale` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.CimrPartSalariale',
    `cimr_base` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.CimrBase',
    `amo_part_salariale` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.AmoPartSalariale',
    `amo_base` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.AmoBase',
    `mutuelle_part_salariale` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.MutuellePartSalariale',
    `mutuelle_base` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.MutuelleBase',
    `total_cotisations_salariales` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.TotalCotisationsSalariales',
    `cnss_part_patronale` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.CnssPartPatronale',
    `cimr_part_patronale` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.CimrPartPatronale',
    `amo_part_patronale` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.AmoPartPatronale',
    `mutuelle_part_patronale` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.MutuellePartPatronale',
    `total_cotisations_patronales` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.TotalCotisationsPatronales',
    `impot_revenu` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.ImpotRevenu',
    `ir_taux` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.IrTaux',
    `arrondi` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.Arrondi',
    `avance_sur_salaire` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.AvanceSurSalaire',
    `interet_sur_logement` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.InteretSurLogement',
    `brut_imposable` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.BrutImposable',
    `net_imposable` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.NetImposable',
    `total_gains` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.TotalGains',
    `total_retenues` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.TotalRetenues',
    `net_a_payer` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.NetAPayer',
    `total_net` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.TotalNet',
    `total_net2` Nullable(Decimal(18,2)) COMMENT 'PayrollResults.TotalNet2'
)
ENGINE = ReplacingMergeTree
PARTITION BY payroll_year
ORDER BY (sk_company, sk_date, sk_employee, sk_payroll)
COMMENT '870 bulletins (2026 uniquement au profilage). Grain : 1 ligne = 1 employe x 1 mois x 1 annee - unicite (EmployeeId,Month,Year) NON confirmee par le fichier doublons -> controle qualite obligatoire avant charge (Partie 5 de la conception). Colonnes techniques exclues : ClaudeModel, TokensUsed, ResultatJson, ErrorMessage, ProcessedAt, ImportBatchNumber, ImportMetadataJson, ImportSource, CreatedAt/By, UpdatedAt/By, DeletedAt/By';


-- ====================================================================================================
-- FAIT DE DETAIL : fact_payroll_premium (<- PayrollResultPrimes) -- grain = 1 prime versee dans 1 bulletin
-- ====================================================================================================

DROP TABLE IF EXISTS payzenhr_dw.fact_payroll_premium;
CREATE TABLE payzenhr_dw.fact_payroll_premium
(
    `sk_payroll_premium` UInt32,
    `bk_payroll_premium_id` UInt32 COMMENT 'PayrollResultPrimes.Id',
    `sk_payroll` UInt32 COMMENT 'FK fact_payroll (fait parent) via PayrollResultPrimes.PayrollResultId',
    `sk_employee` UInt32 COMMENT 'Denormalisee depuis le bulletin parent - evite une jointure fait-a-fait en Power BI',
    `sk_company` UInt32 COMMENT 'Denormalisee depuis le bulletin parent',
    `sk_date` UInt32 COMMENT 'Denormalisee depuis le bulletin parent',
    `premium_label` Nullable(String) COMMENT 'PayrollResultPrimes.Label - attribut degenere, aucun dictionnaire de correspondance disponible',
    `amount` Nullable(Decimal(18,2)) COMMENT 'PayrollResultPrimes.Montant - NE JAMAIS SOMMER avec fact_payroll.total_primes_imposables (double comptage)',
    `display_order` Nullable(Int16) COMMENT 'PayrollResultPrimes.Ordre - technique, exclu des mesures agregables',
    `is_taxable` Nullable(Bool) COMMENT 'PayrollResultPrimes.IsTaxable'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_company, sk_date, sk_employee, sk_payroll_premium)
COMMENT '399 lignes de prime. Grain : 1 ligne = 1 prime versee dans un bulletin de paie donne. RISQUE DE DOUBLE COMPTAGE documente : amount ne doit jamais etre additionne a fact_payroll.total_primes_imposables dans le modele semantique Power BI';


-- ====================================================================================================
-- FAIT : fact_payroll_tax_snapshot (<- PayrollTaxSnapshots) -- grain = cumuls fiscaux/sociaux d'1 bulletin (1:1)
-- ====================================================================================================

DROP TABLE IF EXISTS payzenhr_dw.fact_payroll_tax_snapshot;
CREATE TABLE payzenhr_dw.fact_payroll_tax_snapshot
(
    `sk_payroll_tax_snapshot` UInt32,
    `bk_payroll_tax_snapshot_id` UInt32 COMMENT 'PayrollTaxSnapshots.Id',
    `sk_payroll` UInt32 COMMENT 'FK fact_payroll (relation 1:1) via PayrollTaxSnapshots.PayrollResultId',
    `sk_employee` UInt32 COMMENT 'Denormalisee depuis le bulletin parent',
    `sk_company` UInt32 COMMENT 'Denormalisee depuis le bulletin parent',
    `sk_date` UInt32 COMMENT 'Denormalisee depuis le bulletin parent',
    `cumul_brut` Nullable(Decimal(18,2)) COMMENT 'PayrollTaxSnapshots.CumulBrut',
    `cumul_cnss` Nullable(Decimal(18,2)) COMMENT 'PayrollTaxSnapshots.CumulCnss',
    `cumul_amo` Nullable(Decimal(18,2)) COMMENT 'PayrollTaxSnapshots.CumulAmo',
    `cumul_sni` Nullable(Decimal(18,2)) COMMENT 'PayrollTaxSnapshots.CumulSni',
    `cumul_ir` Nullable(Decimal(18,2)) COMMENT 'PayrollTaxSnapshots.CumulIr',
    `cumul_net` Nullable(Decimal(18,2)) COMMENT 'PayrollTaxSnapshots.CumulNet',
    `cumul_fp` Nullable(Decimal(18,2)) COMMENT 'PayrollTaxSnapshots.CumulFp',
    `taux_effectif` Nullable(Decimal(18,2)) COMMENT 'PayrollTaxSnapshots.TauxEffectif - ratio, a lire/moyenner, jamais sommer'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_company, sk_date, sk_employee, sk_payroll_tax_snapshot)
COMMENT '581 lignes. Grain : 1 ligne = les cumuls fiscaux/sociaux a date d''un bulletin de paie donne (1:1 avec fact_payroll)';


-- ====================================================================================================
-- FAITS PRE-AGREGES MENSUELS (grain commun : employe x mois x annee)
-- ====================================================================================================

DROP TABLE IF EXISTS payzenhr_dw.fact_absence_monthly;
CREATE TABLE payzenhr_dw.fact_absence_monthly
(
    `sk_absence_monthly` UInt32,
    `bk_absence_monthly_id` UInt32 COMMENT 'EmployeeMonthlyAbsenceSummaries.Id',
    `sk_employee` UInt32 COMMENT 'FK dim_employee via EmployeeMonthlyAbsenceSummaries.EmployeeId',
    `sk_company` UInt32 COMMENT 'Denormalisee depuis dim_employee.sk_company (pas de CompanyId direct sur cette table source) - non-nullable car dim_employee.sk_company est obligatoire',
    `sk_date` UInt32 COMMENT 'FK dim_date (1er jour du mois), resolue depuis Year/Month',
    `summary_year` UInt16 COMMENT 'EmployeeMonthlyAbsenceSummaries.Year',
    `summary_month` UInt8 COMMENT 'EmployeeMonthlyAbsenceSummaries.Month',
    `absence_days` Nullable(Decimal(10,2)) COMMENT 'EmployeeMonthlyAbsenceSummaries.AbsenceDays',
    `import_reference` Nullable(String) COMMENT 'EmployeeMonthlyAbsenceSummaries.ImportReference'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_company, sk_date, sk_employee, sk_absence_monthly)
COMMENT '150 lignes (2026, janv.-juil.). Grain : 1 ligne represente le nombre de jours d''absence d''un employe pour un mois et une annee donnes (unicite EmployeeId x Year x Month CONFIRMEE par contrainte applicative)';

DROP TABLE IF EXISTS payzenhr_dw.fact_leave_monthly;
CREATE TABLE payzenhr_dw.fact_leave_monthly
(
    `sk_leave_monthly` UInt32,
    `bk_leave_monthly_id` UInt32 COMMENT 'EmployeeMonthlyLeaveSummaries.Id',
    `sk_employee` UInt32 COMMENT 'FK dim_employee via EmployeeMonthlyLeaveSummaries.EmployeeId',
    `sk_company` UInt32 COMMENT 'Denormalisee depuis dim_employee.sk_company (pas de CompanyId direct sur cette table source) - non-nullable car dim_employee.sk_company est obligatoire',
    `sk_date` UInt32 COMMENT 'FK dim_date (1er jour du mois), resolue depuis Year/Month',
    `summary_year` UInt16 COMMENT 'EmployeeMonthlyLeaveSummaries.Year',
    `summary_month` UInt8 COMMENT 'EmployeeMonthlyLeaveSummaries.Month',
    `leave_days` Nullable(Decimal(10,2)) COMMENT 'EmployeeMonthlyLeaveSummaries.LeaveDays',
    `import_reference` Nullable(String) COMMENT 'EmployeeMonthlyLeaveSummaries.ImportReference'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_company, sk_date, sk_employee, sk_leave_monthly)
COMMENT '150 lignes (2026, janv.-juil.). Grain : 1 ligne represente le nombre de jours de conge pris par un employe pour un mois et une annee donnes (unicite EmployeeId x Year x Month CONFIRMEE par contrainte applicative)';

-- NOTE : aucune colonne LeaveTypeId n'existe reellement sur EmployeeMonthlyLeaveSummaries (confirme par columns_tables.csv).
-- fact_leave_monthly n'a donc PAS de sk_leave_type, conformement a la conception (pas de colonne inventee).

DROP TABLE IF EXISTS payzenhr_dw.fact_worked_days_monthly;
CREATE TABLE payzenhr_dw.fact_worked_days_monthly
(
    `sk_worked_days_monthly` UInt32,
    `bk_worked_days_monthly_id` UInt32 COMMENT 'EmployeeMonthlyWorkedDaysSummaries.Id',
    `sk_employee` UInt32 COMMENT 'FK dim_employee via EmployeeMonthlyWorkedDaysSummaries.EmployeeId',
    `sk_company` UInt32 COMMENT 'Denormalisee depuis dim_employee.sk_company (pas de CompanyId direct sur cette table source) - non-nullable car dim_employee.sk_company est obligatoire',
    `sk_date` UInt32 COMMENT 'FK dim_date (1er jour du mois), resolue depuis Year/Month',
    `summary_year` UInt16 COMMENT 'EmployeeMonthlyWorkedDaysSummaries.Year',
    `summary_month` UInt8 COMMENT 'EmployeeMonthlyWorkedDaysSummaries.Month',
    `worked_days` Nullable(Decimal(10,2)) COMMENT 'EmployeeMonthlyWorkedDaysSummaries.WorkedDays',
    `import_reference` Nullable(String) COMMENT 'EmployeeMonthlyWorkedDaysSummaries.ImportReference'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_company, sk_date, sk_employee, sk_worked_days_monthly)
COMMENT '26 lignes. Grain : 1 ligne represente le nombre de jours travailles par un employe pour un mois et une annee donnes (unicite EmployeeId x Year x Month CONFIRMEE par contrainte applicative)';

DROP TABLE IF EXISTS payzenhr_dw.fact_overtime_monthly;
CREATE TABLE payzenhr_dw.fact_overtime_monthly
(
    `sk_overtime_monthly` UInt32,
    `bk_overtime_monthly_id` UInt32 COMMENT 'EmployeeMonthlyOvertimeSummaries.Id',
    `sk_employee` UInt32 COMMENT 'FK dim_employee via EmployeeMonthlyOvertimeSummaries.EmployeeId',
    `sk_company` UInt32 COMMENT 'Denormalisee depuis dim_employee.sk_company (pas de CompanyId direct sur cette table source) - non-nullable car dim_employee.sk_company est obligatoire',
    `sk_date` UInt32 COMMENT 'FK dim_date (1er jour du mois), resolue depuis Year/Month',
    `summary_year` UInt16 COMMENT 'EmployeeMonthlyOvertimeSummaries.Year',
    `summary_month` UInt8 COMMENT 'EmployeeMonthlyOvertimeSummaries.Month',
    `hours25_pct` Nullable(Decimal(10,2)) COMMENT 'EmployeeMonthlyOvertimeSummaries.Hours25Pct',
    `hours50_pct` Nullable(Decimal(10,2)) COMMENT 'EmployeeMonthlyOvertimeSummaries.Hours50Pct',
    `hours100_pct` Nullable(Decimal(10,2)) COMMENT 'EmployeeMonthlyOvertimeSummaries.Hours100Pct',
    `import_reference` Nullable(String) COMMENT 'EmployeeMonthlyOvertimeSummaries.ImportReference'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_company, sk_date, sk_employee, sk_overtime_monthly)
COMMENT '4 lignes SEULEMENT - table chargee pour la coherence du modele mais NON exposee dans le dashboard v1 (volumetrie non representative). Grain : 1 ligne represente le cumul d''heures supplementaires d''un employe pour un mois et une annee donnes (unicite EmployeeId x Year x Month CONFIRMEE par contrainte applicative)';


-- ====================================================================================================
-- FAIT : fact_leave_balance (<- LeaveBalances) -- grain = solde d'1 employe x 1 type de conge x mois x annee
-- ====================================================================================================

DROP TABLE IF EXISTS payzenhr_dw.fact_leave_balance;
CREATE TABLE payzenhr_dw.fact_leave_balance
(
    `sk_leave_balance` UInt32,
    `bk_leave_balance_id` UInt32 COMMENT 'LeaveBalances.Id',
    `sk_employee` UInt32 COMMENT 'FK dim_employee via LeaveBalances.EmployeeId',
    `sk_leave_type` UInt32 COMMENT 'FK dim_leave_type via LeaveBalances.LeaveTypeId',
    `sk_company` UInt32 COMMENT 'FK dim_company via LeaveBalances.CompanyId',
    `sk_date` UInt32 COMMENT 'FK dim_date (1er jour du mois), resolue depuis Year/Month',
    `balance_year` UInt16 COMMENT 'LeaveBalances.Year',
    `balance_month` UInt8 COMMENT 'LeaveBalances.Month',
    `opening_days` Nullable(Decimal(10,2)) COMMENT 'LeaveBalances.OpeningDays',
    `accrued_days` Nullable(Decimal(10,2)) COMMENT 'LeaveBalances.AccruedDays',
    `used_days` Nullable(Decimal(10,2)) COMMENT 'LeaveBalances.UsedDays',
    `carry_in_days` Nullable(Decimal(10,2)) COMMENT 'LeaveBalances.CarryInDays',
    `carry_out_days` Nullable(Decimal(10,2)) COMMENT 'LeaveBalances.CarryOutDays',
    `closing_days` Nullable(Decimal(10,2)) COMMENT 'LeaveBalances.ClosingDays - NE PAS SOMMER entre mois (fait de type snapshot periodique, non additif dans le temps)',
    `carryover_expires_on` Nullable(Date) COMMENT 'LeaveBalances.CarryoverExpiresOn - NULL conserve = pas de report avec expiration (cas normal)',
    `last_recalculated_at` Nullable(DateTime) COMMENT 'LeaveBalances.LastRecalculatedAt'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_company, sk_date, sk_employee, sk_leave_type)
COMMENT '109 lignes (2019-2026, profondeur historique la plus longue). Grain : 1 ligne = solde de conges d''un employe, pour un type de conge, un mois et une annee donnes (unicite EmployeeId x LeaveTypeId x Year x Month CONFIRMEE). FAIT DE TYPE SNAPSHOT PERIODIQUE : closing_days/opening_days ne sont pas additifs entre periodes';


-- ====================================================================================================
-- FAIT : fact_salary_component (<- EmployeeSalaryComponents) -- grain = 1 composant rattache a 1 version de salaire
-- ====================================================================================================

DROP TABLE IF EXISTS payzenhr_dw.fact_salary_component;
CREATE TABLE payzenhr_dw.fact_salary_component
(
    `sk_salary_component` UInt32,
    `bk_salary_component_id` UInt32 COMMENT 'EmployeeSalaryComponents.Id',
    `sk_employee_salary` UInt32 COMMENT 'FK dim_employee_salary via EmployeeSalaryComponents.EmployeeSalaryId',
    `sk_employee` UInt32 COMMENT 'Denormalisee depuis dim_employee_salary (pas d''EmployeeId direct sur cette table source)',
    `component_type` LowCardinality(Nullable(String)) COMMENT 'EmployeeSalaryComponents.ComponentType - attribut degenere de filtre',
    `is_taxable` Nullable(Bool) COMMENT 'EmployeeSalaryComponents.IsTaxable',
    `is_social` Nullable(Bool) COMMENT 'EmployeeSalaryComponents.IsSocial',
    `is_cimr` Nullable(Bool) COMMENT 'EmployeeSalaryComponents.IsCIMR',
    `amount` Nullable(Decimal(18,2)) COMMENT 'EmployeeSalaryComponents.Amount - NULL = a signaler si percentage egalement NULL',
    `percentage` Nullable(Decimal(5,2)) COMMENT 'EmployeeSalaryComponents.Percentage - NULL conserve = composant en montant fixe (>99% des cas, normal)',
    `effective_date` Nullable(Date) COMMENT 'EmployeeSalaryComponents.EffectiveDate',
    `end_date` Nullable(Date) COMMENT 'EmployeeSalaryComponents.EndDate - NULL conserve = composant actif (>99% des cas)'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_employee, sk_employee_salary, sk_salary_component)
COMMENT '1112 lignes. Grain : 1 ligne = un composant de remuneration rattache a une version de salaire d''un employe';


-- ====================================================================================================
-- FAIT : fact_alert (<- Alerts) -- grain = 1 occurrence d'alerte declenchee
-- ====================================================================================================

DROP TABLE IF EXISTS payzenhr_dw.fact_alert;
CREATE TABLE payzenhr_dw.fact_alert
(
    `sk_alert` UInt32,
    `bk_alert_id` UInt32 COMMENT 'Alerts.Id',
    `sk_company` UInt32 COMMENT 'FK dim_company via Alerts.CompanyId',
    `sk_alert_definition` UInt32 COMMENT 'FK dim_alert_definition via Alerts.DefinitionId',
    `sk_date` UInt32 COMMENT 'FK dim_date, resolue depuis Alerts.CreatedAt (date de creation de l''alerte)',
    `entity_type` LowCardinality(Nullable(String)) COMMENT 'Alerts.EntityType - texte degenere, JAMAIS utilise en jointure (D4, reference polymorphe non-FK)',
    `entity_id` Nullable(String) COMMENT 'Alerts.EntityId - texte informatif, JAMAIS utilise en jointure (D4)',
    `status` LowCardinality(Nullable(String)) COMMENT 'Alerts.Status',
    `is_acknowledged` Nullable(Bool) COMMENT 'Alerts.IsAcknowledged',
    `acknowledged_at` Nullable(DateTime) COMMENT 'Alerts.AcknowledgedAt - NULL a 100% au profilage, conserve mais inexploitable en l''etat (aucun KPI construit dessus)',
    `resolved_at` Nullable(DateTime) COMMENT 'Alerts.ResolvedAt - NULL a 92,19% au profilage, conserve tel quel (jamais de date fictive)',
    `expires_at` Nullable(DateTime) COMMENT 'Alerts.ExpiresAt - NULL a 100% au profilage',
    `message` Nullable(String) COMMENT 'Alerts.Message',
    `message_en` Nullable(String) COMMENT 'Alerts.MessageEn',
    `navigation_url` Nullable(String) COMMENT 'Alerts.NavigationUrl'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_company, sk_date, sk_alert_definition, sk_alert)
COMMENT '64 lignes. Grain : 1 ligne = une occurrence d''alerte. Aucune mesure numerique agregable au sens strict - les KPI Alertes sont des comptages/taux, pas des sommes de montants';


-- ====================================================================================================
-- FAIT : fact_leave_request (<- LeaveRequests) -- grain = 1 demande de conge pour 1 employe -- RESERVE VOLUMETRIE
-- ====================================================================================================

DROP TABLE IF EXISTS payzenhr_dw.fact_leave_request;
CREATE TABLE payzenhr_dw.fact_leave_request
(
    `sk_leave_request` UInt32,
    `bk_leave_request_id` UInt32 COMMENT 'LeaveRequests.Id',
    `sk_employee` UInt32 COMMENT 'FK dim_employee via LeaveRequests.EmployeeId',
    `sk_company` UInt32 COMMENT 'FK dim_company via LeaveRequests.CompanyId',
    `sk_leave_type` Nullable(UInt32) COMMENT 'FK dim_leave_type via LeaveRequests.LeaveTypeId',
    `sk_date` UInt32 COMMENT 'FK dim_date, resolue depuis LeaveRequests.StartDate (date de la demande)',
    `start_date` Nullable(Date) COMMENT 'LeaveRequests.StartDate',
    `end_date` Nullable(Date) COMMENT 'LeaveRequests.EndDate',
    `status` Nullable(Int16) COMMENT 'LeaveRequests.Status - attribut degenere, valeurs a cataloguer (echantillon non fourni au profilage)',
    `requested_at` Nullable(DateTime) COMMENT 'LeaveRequests.RequestedAt',
    `submitted_at` Nullable(DateTime) COMMENT 'LeaveRequests.SubmittedAt',
    `decision_at` Nullable(DateTime) COMMENT 'LeaveRequests.DecisionAt - utilisee pour calculer un delai de traitement, pas une mesure sommable',
    `calendar_days` Nullable(Int16) COMMENT 'LeaveRequests.CalendarDays',
    `working_days_deducted` Nullable(Decimal(10,2)) COMMENT 'LeaveRequests.WorkingDaysDeducted',
    `has_min_consecutive_block` Nullable(Bool) COMMENT 'LeaveRequests.HasMinConsecutiveBlock',
    `is_renounced` Nullable(Bool) COMMENT 'LeaveRequests.IsRenounced'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_company, sk_date, sk_employee, sk_leave_request)
COMMENT 'SEULEMENT 2 LIGNES au profilage. Grain : 1 ligne = une demande de conge pour un employe. Table modelisee et chargeable (grain/colonnes fiables) mais A NE PAS METTRE EN AVANT dans le dashboard tant qu''un volume representatif n''est pas confirme en production';


-- ====================================================================================================
-- FAIT : fact_absence_event (<- EmployeeAbsences) -- grain = 1 evenement d'absence -- RESERVE VOLUMETRIE
-- ====================================================================================================

DROP TABLE IF EXISTS payzenhr_dw.fact_absence_event;
CREATE TABLE payzenhr_dw.fact_absence_event
(
    `sk_absence_event` UInt32,
    `bk_absence_event_id` UInt32 COMMENT 'EmployeeAbsences.Id',
    `sk_employee` UInt32 COMMENT 'FK dim_employee via EmployeeAbsences.EmployeeId',
    `sk_company` UInt32 COMMENT 'Denormalisee depuis dim_employee.sk_company (pas de CompanyId direct sur cette table source) - non-nullable car dim_employee.sk_company est obligatoire',
    `sk_date` UInt32 COMMENT 'FK dim_date, resolue depuis EmployeeAbsences.AbsenceDate',
    `absence_date` Nullable(Date) COMMENT 'EmployeeAbsences.AbsenceDate',
    `duration_type` Nullable(Int16) COMMENT 'EmployeeAbsences.DurationType - attribut degenere',
    `status` Nullable(Int16) COMMENT 'EmployeeAbsences.Status - attribut degenere, valeurs a cataloguer',
    `absence_type` Nullable(String) COMMENT 'EmployeeAbsences.AbsenceType - attribut degenere',
    `is_morning` Nullable(Bool) COMMENT 'EmployeeAbsences.IsMorning',
    `decision_at` Nullable(DateTime) COMMENT 'EmployeeAbsences.DecisionAt',
    `reason` Nullable(String) COMMENT 'EmployeeAbsences.Reason'
)
ENGINE = ReplacingMergeTree
ORDER BY (sk_company, sk_date, sk_employee, sk_absence_event)
COMMENT 'SEULEMENT 2 LIGNES au profilage. Grain : 1 ligne = un evenement d''absence. Meme reserve de volumetrie que fact_leave_request - a ne pas mettre en avant dans le dashboard v1';


-- ====================================================================================================
-- SEED : peuplement de dim_date (2019-01-01 -> 2027-12-31, cf. Conception 2.12)
-- ====================================================================================================

INSERT INTO payzenhr_dw.dim_date
SELECT
    toUInt32(formatDateTime(d, '%Y%m%d'))                              AS sk_date,
    d                                                                    AS full_date,
    toUInt16(toYear(d))                                                 AS annee_civile,
    toUInt8(toMonth(d))                                                 AS mois,
    formatDateTime(d, '%B')                                             AS nom_mois,
    toUInt8(toQuarter(d))                                               AS trimestre,
    toUInt8(toDayOfWeek(d))                                             AS jour_semaine,
    formatDateTime(d, '%A')                                             AS nom_jour_semaine,
    toUInt8(toISOWeek(d))                                               AS numero_semaine,
    toDayOfWeek(d) IN (6, 7)                                            AS est_weekend,
    toStartOfMonth(d)                                                   AS premier_jour_mois
FROM
(
    SELECT toDate('2019-01-01') + number AS d
    FROM numbers(toUInt32(dateDiff('day', toDate('2019-01-01'), toDate('2027-12-31'))) + 1)
);