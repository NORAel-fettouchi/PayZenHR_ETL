# PayZenHR — Conception du Data Warehouse (modèle en étoile ClickHouse)

Ce document s'appuie exclusivement sur les trois fichiers d'analyse fournis (`Catalogue_KPI`, `Modele_DW_Groupes_ABCD`, `Audit_Structurel_BDD`). Aucune table, colonne, relation ou KPI n'est ajoutée si elle n'y est pas justifiée par le profilage réel. Aucun code n'est produit à ce stade.

---

# PARTIE 1 — Modèle conceptuel du Data Warehouse

## 1.0 Décisions de modélisation à trancher avant la suite (posées ici, appliquées partout ensuite)

| # | Point ouvert dans l'audit | Décision retenue | Justification |
|---|---|---|---|
| D1 | `EmployeeContracts`/`EmployeeSalaries` sont des SCD2 sans lien direct avec `PayrollResults` | **Résolution au moment du chargement (ETL-time), pas au moment de la requête Power BI.** On calcule, pour chaque ligne de `fact_payroll`, le poste et le type de contrat en vigueur à la date du bulletin par jointure temporelle sur `EmployeeContracts` (`StartDate ≤ date bulletin ≤ EndDate ou EndDate NULL`), et on stocke le résultat comme deux clés étrangères supplémentaires directement sur `fact_payroll` (`sk_job_position_at_payroll`, `sk_contract_type_at_payroll`). | Évite une jointure temporelle en DAX/Power BI (lente, source d'erreurs) ; garde le grain du fait intact (1 ligne = 1 bulletin) ; conforme à la règle "ne jamais mélanger les grains". |
| D2 | `EmployeeAddresses` n'est pas strictement 1:1 avec `Employees` (1213 adresses / 1561 employés) | Une seule adresse "principale" est sélectionnée par employé lors de la construction de `dim_employee` (règle : adresse la plus récente par `CreatedAt`, à défaut de flag `IsPrimary` dans les colonnes réelles). | Évite la duplication de la ligne employé dans `dim_employee` si l'on joignait brut sur `EmployeeId`. |
| D3 | `Holidays` et `WorkingCalendars` sont spécifiques par société (`CompanyId`) | Ces deux tables **ne sont pas fusionnées dans `dim_date`** (qui doit rester générique et partagée par toutes les sociétés). Elles restent des tables satellites jointes par `(Date, CompanyId)` uniquement au moment du calcul agrégé des KPI (jours ouvrés, taux d'absentéisme), jamais comme attribut plat de `dim_date`. | Un jour férié ou ouvré n'a de sens qu'au regard d'une société ; le mettre dans `dim_date` romprait la conformité de la dimension entre sociétés. |
| D4 | `Alerts.EntityType/EntityId` et polymorphismes similaires | Conservés comme **attributs texte dégénérés** sur `fact_alert`, jamais utilisés comme clé de jointure vers une autre dimension. | Absence de contrainte FK réelle confirmée par le schéma applicatif — toute jointure dessus serait non garantie et risquerait un mauvais rattachement. |
| D5 | Répartition géographique (ville/pays) | `dim_city` et `dim_country` sont conservées comme **dimensions conformées séparées** (pas dénormalisées en texte libre dans `dim_employee`/`dim_company`), car elles sont partagées par les deux. | Évite la divergence d'orthographe/libellé d'une même ville entre la fiche employé et la fiche société ; permet une hiérarchie Pays > Ville réutilisable. |
| D6 | `EmployeeCategories`, `MaritalStatuses` | Colonnes conservées dans `dim_employee` mais **marquées "fiabilité faible"** dans le modèle et dans la documentation du dashboard (NULL à 99,74 % et 85,27 %) plutôt qu'exclues, pour ne pas devoir modifier le schéma si la donnée se remplit en production. | Cohérent avec l'explication technique du schéma applicatif (`SetNull` en cascade) — la colonne n'est pas "cassée", elle est simplement peu peuplée aujourd'hui. |
| D7 | Aucune relation plusieurs-à-plusieurs (N:N) structurante identifiée dans le périmètre retenu | Toutes les relations retenues sont 1:1 ou 1:N. Les seules relations N:N du schéma applicatif (sécurité `RolesPermissions`/`UsersRoles`) sont hors périmètre RH/Paie et exclues. | Confirmé par l'audit structurel — pas de table de liaison métier dans le Groupe A/B retenu. |

---

## 1.1 Tables de faits

### fact_payroll (← PayrollResults)
- **Événement métier** : calcul d'un bulletin de paie pour un employé sur une période.
- **Grain** : *Une ligne représente le bulletin de paie calculé d'un employé pour un mois et une année donnés.*
  ⚠️ L'unicité (EmployeeId, Month, Year) n'est **pas confirmée** par le fichier de doublons (qui ne teste que la clé technique `Id`) — ce contrôle est inscrit en Partie 5 comme préalable obligatoire au chargement.
- **Clé primaire technique** : `sk_payroll` (entier auto-incrémenté).
- **Clés étrangères** : `sk_employee`, `sk_company`, `sk_date` (1er jour du mois du bulletin), `sk_job_position_at_payroll` (résolue ETL, cf. D1), `sk_contract_type_at_payroll` (résolue ETL, cf. D1).
- **Mesures** : `SalaireBase`, `TotalBrut`, `NetAPayer`, `ImpotRevenu`, `BrutImposable`, `CnssPartSalariale`, `CnssPartPatronale`, `CimrPartSalariale`, `CimrPartPatronale`, `AmoPartSalariale`, `AmoPartPatronale`, `HeuresSupp25`, `HeuresSupp50`, `HeuresSupp100`, `Conges`, `JoursFeries`, `PrimeAnciennete`, `TotalCotisationsSalariales`, `TotalCotisationsPatronales`, `TotalPrimesImposables`, `AutresPrimesNonImposable`, + les autres rubriques numériques de paie non nommées individuellement dans le profilage (~50 colonnes de montants, à cataloguer une à une en phase de code).
- **Dimensions liées** : dim_employee, dim_company, dim_date, dim_job_position (résolue), dim_contract_type (résolue).

### fact_payroll_premium (← PayrollResultPrimes)
- **Événement métier** : ligne de prime versée à l'intérieur d'un bulletin.
- **Grain** : *Une ligne représente une prime versée dans un bulletin de paie donné.*
- **Clé primaire technique** : `sk_payroll_premium`.
- **Clés étrangères** : `sk_payroll` (fait parent), et — dénormalisées à la construction pour éviter une jointure fait-à-fait en Power BI — `sk_employee`, `sk_company`, `sk_date` (identiques à celles du bulletin parent).
- **Mesures** : `Montant`, `Ordre` (position d'affichage, technique — à exclure des mesures agrégables).
- **Dimensions liées** : dim_employee, dim_company, dim_date (+ attribut dégénéré `Label`, `IsTaxable`).
- **Risque** : sommer `Montant` en même temps que `TotalPrimesImposables` de `fact_payroll` créerait un double comptage — à documenter clairement dans le modèle sémantique Power BI (deux mesures distinctes, jamais additionnées entre elles).

### fact_payroll_tax_snapshot (← PayrollTaxSnapshots)
- **Événement métier** : cumul fiscal et social calculé au moment d'un bulletin.
- **Grain** : *Une ligne représente les cumuls fiscaux/sociaux à date d'un bulletin de paie donné* (relation 1:1 avec fact_payroll, confirmée par le schéma applicatif).
- **Clé primaire technique** : `sk_payroll_tax_snapshot`.
- **Clés étrangères** : `sk_payroll` (1:1), `sk_employee`, `sk_company`, `sk_date` (dénormalisées comme ci-dessus).
- **Mesures** : `CumulBrut`, `CumulCnss`, `CumulAmo`, `CumulIr`, `CumulNet`, `CumulFp`, `TauxEffectif` (ratio, à ne pas sommer — uniquement moyenner/lire directement).
- **Dimensions liées** : dim_employee, dim_company, dim_date.

### fact_absence_monthly (← EmployeeMonthlyAbsenceSummaries)
- **Événement métier** : cumul mensuel des jours d'absence.
- **Grain** : *Une ligne représente le nombre de jours d'absence d'un employé pour un mois et une année donnés* (unicité EmployeeId×Year×Month **confirmée** par contrainte applicative).
- **Clé primaire technique** : `sk_absence_monthly`.
- **Clés étrangères** : `sk_employee`, `sk_company` (via employé), `sk_date` (1er jour du mois).
- **Mesures** : `AbsenceDays`.
- **Dimensions liées** : dim_employee, dim_company, dim_date.

### fact_leave_monthly (← EmployeeMonthlyLeaveSummaries)
- **Événement métier** : cumul mensuel des jours de congé pris.
- **Grain** : *Une ligne représente le nombre de jours de congé pris par un employé pour un mois et une année donnés* (unicité EmployeeId×Year×Month **confirmée**).
- **Clé primaire technique** : `sk_leave_monthly`.
- **Clés étrangères** : `sk_employee`, `sk_company`, `sk_date`.
- **Mesures** : `LeaveDays`.
- **Dimensions liées** : dim_employee, dim_company, dim_date.
  ⚠️ Pas de `sk_leave_type` : la présence d'une colonne `LeaveTypeId` sur cette table n'est pas confirmée par le profilage (le catalogue KPI la mentionne comme "à vérifier"). Elle n'est donc **pas ajoutée** au modèle tant que la colonne réelle n'est pas confirmée dans `columns_tables.csv`.

### fact_leave_balance (← LeaveBalances)
- **Événement métier** : solde de congés d'un employé pour une période.
- **Grain** : *Une ligne représente le solde de congés d'un employé, pour un type de congé, un mois et une année donnés* (unicité EmployeeId×LeaveTypeId×Year×Month **confirmée**).
- **Clé primaire technique** : `sk_leave_balance`.
- **Clés étrangères** : `sk_employee`, `sk_leave_type`, `sk_company`, `sk_date`.
- **Mesures** : `OpeningDays`, `AccruedDays`, `UsedDays`, `CarryInDays`, `CarryOutDays`, `ClosingDays`.
- **Attribut conservé (non-mesure)** : `CarryoverExpiresOn` (date, utilisée pour l'alerte "risque de perte de solde", pas sommable).
- **Dimensions liées** : dim_employee, dim_leave_type, dim_company, dim_date.
- **Profondeur historique** : 2019–2026 — la plus longue disponible, aucune raison de limiter le chargement dans le temps.

### fact_salary_component (← EmployeeSalaryComponents)
- **Événement métier** : composant de rémunération (prime, indemnité) attaché à une version de salaire.
- **Grain** : *Une ligne représente un composant de rémunération rattaché à une version de salaire d'un employé.*
- **Clé primaire technique** : `sk_salary_component`.
- **Clés étrangères** : `sk_employee_salary` (dimension SCD2 parente), `sk_employee` (dénormalisée depuis le parent).
- **Mesures** : `Amount`, `Percentage` (très majoritairement NULL — cohérent avec des composants en montant fixe, cf. Partie 2).
- **Attributs dégénérés (utilisés en filtre, pas en jointure)** : `ComponentType`, `IsTaxable`, `IsSocial`, `IsCIMR`.
- **Dimensions liées** : dim_employee_salary, dim_employee.

### fact_alert (← Alerts)
- **Événement métier** : déclenchement d'une alerte par le moteur de règles.
- **Grain** : *Une ligne représente une occurrence d'alerte.*
- **Clé primaire technique** : `sk_alert`.
- **Clés étrangères** : `sk_company`, `sk_alert_definition`, `sk_date` (date de création de l'alerte).
- **Mesures** : aucune mesure numérique agrégable au sens strict — les KPI Alertes sont des comptages (`COUNT`) et des taux, pas des sommes de montants.
- **Attributs dégénérés** : `Status`, `IsAcknowledged`, `EntityType` (texte, jamais joint — cf. D4), `EntityId` (texte informatif, jamais joint), `ResolvedAt`/`AcknowledgedAt` (conservés mais très majoritairement NULL, cf. Partie 2).
- **Dimensions liées** : dim_company, dim_alert_definition, dim_date.

### fact_leave_request (← LeaveRequests) — réserve de volumétrie
- **Événement métier** : demande individuelle de congé.
- **Grain** : *Une ligne représente une demande de congé pour un employé.*
- **Clé primaire technique** : `sk_leave_request`.
- **Clés étrangères** : `sk_employee`, `sk_company`, `sk_leave_type`, `sk_date` (date de la demande).
- **Mesures** : `CalendarDays`, `WorkingDaysDeducted`.
- **Attributs dégénérés** : `Status`, `DecisionAt` (utilisée pour calculer un délai de traitement, pas une mesure sommable).
- **Dimensions liées** : dim_employee, dim_company, dim_leave_type, dim_date.
- ⚠️ **2 lignes seulement dans l'extrait profilé** — la table est modélisée et sera chargée (le grain et les colonnes sont fiables), mais **ne doit pas être mise en avant dans le dashboard tant qu'un volume représentatif n'est pas confirmé en production.**

### fact_absence_event (← EmployeeAbsences) — réserve de volumétrie
- **Événement métier** : absence individuelle constatée.
- **Grain** : *Une ligne représente un événement d'absence.*
- **Clé primaire technique** : `sk_absence_event`.
- **Clés étrangères** : `sk_employee`, `sk_company`, `sk_date`.
- **Mesures** : aucune mesure numérique confirmée (durée déduite de `DurationType`, attribut catégoriel, pas un nombre).
- **Attributs dégénérés** : `DurationType`, `Status`.
- **Dimensions liées** : dim_employee, dim_company, dim_date.
- ⚠️ **2 lignes seulement** — même réserve que `fact_leave_request`.

### fact_worked_days_monthly (← EmployeeMonthlyWorkedDaysSummaries)
- **Événement métier** : cumul mensuel de jours travaillés.
- **Grain** : *Une ligne représente le nombre de jours travaillés par un employé pour un mois et une année donnés* (unicité **confirmée**).
- **Clé primaire technique** : `sk_worked_days_monthly`.
- **Clés étrangères** : `sk_employee`, `sk_company`, `sk_date`.
- **Mesures** : `WorkedDays`.
- **Dimensions liées** : dim_employee, dim_company, dim_date.
- Volumétrie faible (26 lignes) mais structure fiable — chargée sans réserve particulière au-delà de la taille de l'échantillon.

### fact_overtime_monthly (← EmployeeMonthlyOvertimeSummaries) — réserve forte
- **Événement métier** : cumul mensuel d'heures supplémentaires.
- **Grain** : *Une ligne représente le cumul d'heures supplémentaires d'un employé pour un mois et une année donnés* (unicité **confirmée**).
- **Clé primaire technique** : `sk_overtime_monthly`.
- **Clés étrangères** : `sk_employee`, `sk_company`, `sk_date`.
- **Mesures** : `Hours25Pct`, `Hours50Pct`, `Hours100Pct`.
- **Dimensions liées** : dim_employee, dim_company, dim_date.
- ⚠️ **4 lignes seulement.** Recommandation : la table est chargée dans le DW pour la cohérence du modèle et pour ne pas bloquer un futur usage, mais **elle n'est pas exposée dans le dashboard v1** (aucun visuel Power BI dessus) tant que le volume de production n'est pas confirmé — un KPI sur 4 lignes serait trompeur.

---

## 1.2 Dimensions

| Dimension | Clé technique | Clé métier | Attributs conservés | Attributs supprimés | Raison de conservation |
|---|---|---|---|---|---|
| **dim_employee** (← Employees + Genders + Nationalities + MaritalStatuses + EducationLevels + EmployeeAddresses) | `sk_employee` | `Employees.Id` (Matricule gardé en attribut d'affichage, pas en clé — unicité non confirmée) | Nom/Prénom, DateOfBirth (→ âge/tranche d'âge calculés), Genre (dénormalisé, 2 valeurs), Nationalité (dénormalisée, 5 valeurs), NiveauEducation (dénormalisé, 11 valeurs), StatutMarital (dénormalisé, **réserve : NULL à 85,27 %**), `sk_status` (FK), `sk_company` (FK), `sk_department` (FK, **réserve forte : NULL à 90,52 %**), `sk_city` (FK, via adresse principale sélectionnée — cf. D2), AddressLine1/ZipCode (dénormalisés) | CreatedAt/By, UpdatedAt/By, DeletedAt/By (techniques EF Core) ; `CategoryId` retiré comme attribut actif (staging seulement, NULL à 99,74 %) ; `ManagerId` (NULL à 100 %, aucune valeur) ; `CountryId` sur Employees (NULL à 100 %) ; `BankAccountId` (NULL à 100 %) ; `PersonalEmail` (NULL à 100 %) | Dimension centrale du dashboard (1561 employés) ; chaque attribut conservé sert au moins un KPI directement calculable (genre, nationalité, pyramide des âges, effectif société) ; les attributs à forte réserve sont conservés mais signalés, jamais supprimés, car le schéma applicatif explique leur NULL par un `SetNull` en cascade — la structure reste valide même si peu peuplée aujourd'hui. |
| **dim_company** (← Companies) | `sk_company` | `Companies.Id` (IceNumber/CnssNumber en attributs, pas en clé — unicité non garantie par le profilage) | CompanyName, Currency, PayrollPeriodicity, BusinessSector (texte libre), `ManagedByCompanyId` (auto-référence hiérarchie cabinet/société), `sk_city` (FK) | Techniques EF Core ; champs secondaires à très fort taux de NULL non chiffrés précisément dans le profilage (signataire, site web) — exclus par prudence, à réévaluer si un taux de remplissage est confirmé | Axe transverse à **toutes** les tables de faits, table la plus référencée du schéma (29 FK entrantes) — indispensable comme filtre multi-tenant. |
| **dim_department** (← Departements) | `sk_department` | `Departements.Id` | DepartementName, HierarchyLevel, `ParentDepartementId` (auto-référence hiérarchique), `ManagerId` (conservé en attribut informatif, taux de remplissage propre à Departements à vérifier) | Techniques EF Core | Structurellement nécessaire au besoin métier "Départements" du dashboard, mais **usage réel à valider en production** : `Employees.DepartementId` est NULL à 90,52 %, donc la jointure vers les faits (via employé) ne couvre aujourd'hui qu'une minorité d'employés. |
| **dim_job_position** (← JobPositions) | `sk_job_position` | `JobPositions.Id` | Name, `sk_company` (FK) | Techniques EF Core | Accessible uniquement via `EmployeeContracts` (aucune FK directe sur `Employees` ni `PayrollResults`) — utilisée pour résoudre le poste "au moment du bulletin" via jointure temporelle ETL (cf. D1). |
| **dim_contract_type** (← ContractTypes + LegalContractTypes + StateEmploymentPrograms) | `sk_contract_type` | `ContractTypes.Id` | ContractTypeName, LegalContractTypeName (dénormalisé, 5 valeurs), StateEmploymentProgramName (dénormalisé, 3 valeurs) | Techniques EF Core | Faible cardinalité des deux tables satellites → dénormalisation à un seul niveau justifiée, simplifie le modèle sans perte d'information. |
| **dim_employee_contract** (← EmployeeContracts) | `sk_employee_contract` | `EmployeeContracts.Id` | `sk_employee` (FK), `sk_job_position` (FK), `sk_contract_type` (FK), StartDate, EndDate (SCD2 — EndDate NULL = contrat en cours, cohérent avec 80,78 % de NULL) | Techniques EF Core | Ne reçoit **aucun fait directement** — sert de table de résolution temporelle pour enrichir `fact_payroll` (poste/type de contrat en vigueur, cf. D1) ; exposable en Power BI comme axe d'historique de carrière dans une itération future. |
| **dim_employee_salary** (← EmployeeSalaries) | `sk_employee_salary` | `EmployeeSalaries.Id` | `sk_employee` (FK), BaseSalary, `ContractId`, EffectiveDate, EndDate (SCD2 — EndDate NULL = salaire courant, cohérent avec 87,10 % de NULL) | Techniques EF Core | Aucun fait direct — sert de parent à `fact_salary_component` et alimente le KPI "évolution du salaire de base" (comparaison de deux `EffectiveDate` successives, calcul à faire en transformation, pas un simple agrégat). |
| **dim_status** (← Statuses) | `sk_status` | `Statuses.Id` / `Code` | NameFr/Ar/En, `AffectsPayroll`, `AffectsAttendance`, `AffectsAccess` (flags booléens directement filtrables) | Techniques EF Core | Axe central du KPI "effectif actif" — seule dimension avec des flags métier directement exploitables sans transformation. |
| **dim_leave_type** (← LeaveTypes + LeaveTypePolicies) | `sk_leave_type` | `LeaveTypes.Id` | Nom, `sk_company` (FK), attributs de politique dénormalisés (solde négatif autorisé, etc. depuis LeaveTypePolicies, 6 lignes) | Techniques EF Core | Utilisée par `fact_leave_balance` et `fact_leave_request`. |
| **dim_leave_type_legal_rule** (← LeaveTypeLegalRules) — dimension satellite | `sk_leave_legal_rule` | `LeaveTypeLegalRules.Id` | `sk_leave_type` (FK, 1-N), jours accordés, article de loi | Techniques EF Core | Relation 1-N réelle entre type de congé et règles légales (10 règles / 7 types) — conservée comme table satellite séparée plutôt qu'aplatie dans `dim_leave_type`, pour ne pas perdre la granularité "règle" ni dupliquer `dim_leave_type`. |
| **dim_holiday** (← Holidays) | `sk_holiday` | `Holidays.Id` | HolidayDate, IsMandatory, IsPaid, AffectPayroll, AffectAttendance, `sk_company` (FK) | Techniques EF Core | Table satellite de calcul (cf. D3), utilisée pour déterminer les jours ouvrés au dénominateur du taux d'absentéisme — **pas de FK entrante directe depuis un fait.** |
| **dim_working_calendar** (← WorkingCalendars) | `sk_working_calendar` | `WorkingCalendars.Id` | DayOfWeek, IsWorkingDay, StartTime, EndTime, `sk_company` (FK) | Techniques EF Core | Idem `dim_holiday` — dimension de calcul, jointe par (Date, Company) au moment du calcul agrégé, jamais ligne à ligne sur un fait. |
| **dim_alert_definition** (← AlertDefinitions + AlertCategories + AlertSeverities) | `sk_alert_definition` | `AlertDefinitions.Id` / `Code` | Name, CategoryName (dénormalisé, 6 valeurs), SeverityLabel (dénormalisé et **conservé en clair** : Info/Warning/Critical), IsActive | Description, SourceField (NULL à 100 %), techniques EF Core | Axe de filtre principal du dashboard Alertes (sévérité, catégorie). |
| **dim_city** (← Cities) | `sk_city` | `Cities.Id` | CityName, `sk_country` (FK) | Techniques EF Core | Dimension géographique **partagée** entre `dim_employee` et `dim_company` (cf. D5) — évite la duplication/incohérence de libellé de ville. |
| **dim_country** (← Countries) | `sk_country` | `Countries.Id` / `CountryCode` | CountryName | Techniques EF Core | Racine de la hiérarchie géographique partagée ; très faible cardinalité (2 lignes), utile uniquement comme parent de `dim_city`. |
| **dim_date** *(construite, hors source)* | `sk_date` (format AAAAMMJJ) | Date calendaire | AnnéeCivile, Mois, NomMois, Trimestre, JourSemaine, NomJourSemaine, NuméroSemaine, EstWeekEnd | — (aucun "EstFérié" générique, cf. D3) | Dimension conformée partagée par tous les faits ayant une date/mois-année (`fact_payroll`, `fact_leave_balance`, tous les `*_monthly`). Plage à construire : 2019-01-01 (borne basse observée sur `LeaveBalances`) jusqu'à une date future de marge (ex. fin 2027) pour ne pas devoir reconstruire la dimension à chaque nouvel export. |

---

## 1.3 Vérifications demandées

- **Relations plusieurs-à-plusieurs** : aucune identifiée dans le périmètre retenu (cf. D7). Les seules relations N:N du schéma applicatif appartiennent au module sécurité (hors périmètre).
- **Tables de liaison** : aucune conservée dans le modèle en étoile — `PayrollResultPrimes` et `EmployeeSalaryComponents` sont des faits de détail 1:N vers un fait/dimension parent, pas des tables de liaison N:N.
- **Risques de duplication identifiés et traités** : (1) `EmployeeAddresses` non strictement 1:1 avec Employees → règle de sélection D2 ; (2) unicité (Employee, Month, Year) de `PayrollResults` non confirmée → contrôle qualité obligatoire avant chargement (Partie 5) ; (3) double comptage potentiel `fact_payroll_premium.Montant` vs `fact_payroll.TotalPrimesImposables` → à documenter comme deux mesures non additives dans le modèle sémantique.
- **Dimensions partagées entre plusieurs faits** : dim_employee, dim_company, dim_date (toutes les tables de faits) ; dim_leave_type (fact_leave_balance, fact_leave_request) ; dim_employee_salary (fact_salary_component uniquement, mais conçue pour rester réutilisable).
- **Dimension Date** : construite (pas de table source), grain jour, alimente aussi bien les faits journaliers (`fact_alert`, `fact_leave_request`) que les faits mensuels (jointure sur le 1er jour du mois).
- **Dimensions dégénérées** : `EntityType`/`EntityId` sur `fact_alert` (référence polymorphe non-FK, cf. D4) ; `Label`/`IsTaxable` sur `fact_payroll_premium` ; `ComponentType`/`IsTaxable`/`IsSocial`/`IsCIMR` sur `fact_salary_component` ; `Status`/`DurationType` sur `fact_absence_event`/`fact_leave_request`.

---

## 1.4 Modèle sous forme textuelle

```
dim_date ───────────────────────────┐
dim_employee ────────────────────────┤
dim_company ──────────────────────────┼──→ fact_payroll
dim_job_position (résolu ETL, D1) ────┤          │
dim_contract_type (résolu ETL, D1) ───┘          │
                                                  ├──→ fact_payroll_premium   (FK sk_payroll, + sk_employee/company/date dénormalisées)
                                                  └──→ fact_payroll_tax_snapshot (FK sk_payroll 1:1, + sk_employee/company/date dénormalisées)

dim_employee ──┐
dim_company ───┼──→ fact_absence_monthly        (← dim_date)
dim_employee ──┐
dim_company ───┼──→ fact_leave_monthly          (← dim_date)
dim_employee ──┐
dim_company ───┼──→ fact_worked_days_monthly    (← dim_date)
dim_employee ──┐
dim_company ───┼──→ fact_overtime_monthly       (← dim_date)  [non exposé au dashboard v1]

dim_employee ──┐
dim_leave_type ┼──→ fact_leave_balance          (← dim_date, dim_company)
dim_employee ──┐
dim_leave_type ┼──→ fact_leave_request          (← dim_date, dim_company)  [réserve volumétrie]
dim_employee ──┐
dim_company ───┼──→ fact_absence_event          (← dim_date)  [réserve volumétrie]

dim_employee_salary ──→ fact_salary_component   (← dim_employee dénormalisé)

dim_company ─────────┐
dim_alert_definition ┼──→ fact_alert            (← dim_date)

dim_city ←── dim_employee, dim_company
dim_country ←── dim_city
dim_leave_type_legal_rule ──→ dim_leave_type (1-N, satellite)
dim_holiday, dim_working_calendar : satellites de calcul, joints par (Date, Company) au moment du KPI — pas de FK entrante depuis un fait
```

**Explication des relations** : chaque fait pointe vers `dim_date` au grain adapté (jour pour les événements individuels, 1er jour du mois pour les cumuls mensuels) ; `dim_employee` et `dim_company` sont présentes sur pratiquement tous les faits car ce sont les deux axes d'analyse transverses du dashboard (par employé, par société) ; `dim_job_position` et `dim_contract_type` n'atteignent `fact_payroll` que par résolution temporelle en ETL, jamais par une FK directe de la source ; `dim_holiday`/`dim_working_calendar` ne sont reliées à aucun fait par FK — elles interviennent uniquement dans le calcul du dénominateur des KPI de taux (jours ouvrés).

---
---

# PARTIE 2 — Plan de nettoyage

## 2.0 Règles générales appliquées à toutes les tables

- **Colonnes techniques systématiquement supprimées** (sauf besoin explicite de traçabilité) : `CreatedAt`, `CreatedBy`, `UpdatedAt`, `UpdatedBy`, `DeletedAt`, `DeletedBy` — présentes sur la quasi-totalité des 103 tables sources.
- **Colonnes exclues par construction** car sans rapport avec le DW métier : `ClaudeModel`, `TokensUsed`, `ResultatJson`, `ErrorMessage` sur `PayrollResults` (100 % NULL, colonnes techniques d'un moteur de calcul assisté).
- **Nettoyage des espaces** : `TRIM()` systématique sur toutes les colonnes texte conservées (noms, libellés, codes) avant toute comparaison ou agrégation.
- **Normalisation du texte** : casse harmonisée (`Title Case` pour les noms propres, majuscules pour les codes type `IceNumber`/`CnssNumber`), suppression des espaces multiples internes.
- **Règle NULL — jamais de remplacement automatique** : chaque colonne est traitée selon son sens métier (détail table par table ci-dessous). Un NULL n'est remplacé par "Unknown"/"Non renseigné" que si (a) la colonne est utilisée comme axe de filtre/regroupement en Power BI et (b) laisser la ligne NULL casserait l'agrégation (ex. GROUP BY sur une FK).
- **Traitement des doublons** : le fichier de profilage ne détecte aucun doublon sur les clés `Id` (auto-incrémentées). Pour les tables à unicité métier confirmée par le schéma applicatif (`LeaveBalances`, les 4 `EmployeeMonthly*Summaries`, `EmployeeBankAccountAllocations`), un contrôle explicite sur la combinaison de colonnes métier est ajouté en Partie 5 — le profilage `Id` seul ne suffit pas à garantir l'absence de doublons fonctionnels.

## 2.1 dim_employee (← Employees + tables Groupe C associées)

| Colonne source | Table source | Colonne cible | Type source → cible | Espaces | Normalisation | Conversion | NULL | Valeurs invalides | Doublons | Dates | Aberrantes |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `Id` | Employees | `bk_employee_id` (clé métier) | Int → Int | — | — | — | Ne doit jamais être NULL (clé) — ligne rejetée en staging si NULL | Rejet si NULL | Vérifier unicité en staging | — | — |
| `Matricule` | Employees | `employee_matricule` | String → String | Trim | Uppercase si format code interne | — | Conservé NULL si absent (pas de fallback inventé) | — | Non utilisé comme clé (unicité non confirmée) | — | — |
| `Nom`, `Prénom` | Employees | `last_name`, `first_name` | String → String | Trim | Title Case | — | Conservé NULL (rare, à signaler si taux élevé lors du contrôle qualité) | — | — | — | — |
| `DateOfBirth` | Employees | `date_of_birth` | Date/Datetime → Date | — | — | Cast Date strict | Conservé NULL — un employé sans date de naissance connue n'est pas une anomalie automatiquement corrigible | Rejeter/flaguer si date future ou < 1900 | — | Format ISO, fuseau supprimé | Âge calculé < 16 ans ou > 75 ans → flag "à vérifier", pas de suppression automatique |
| `GenderId` → Genders | Employees/Genders | `gender_label` | Int (FK) → String | Trim (libellé source) | Title Case | Dénormalisation par jointure | Conservé NULL si FK NULL — **ne devient "Unknown" que pour permettre le GROUP BY du KPI répartition H/F**, avec un libellé explicite "Non renseigné" distinct de "Homme"/"Femme" | FK orpheline → "Non renseigné" | — | — | — |
| `NationalityId` → Nationalities | idem | `nationality_label` | idem | Trim | Title Case | Dénormalisation | Idem — "Non renseigné" seulement pour l'axe de regroupement KPI | FK orpheline → "Non renseigné" | — | — | — |
| `MaritalStatusId` → MaritalStatuses | idem | `marital_status_label` | idem | Trim | Title Case | Dénormalisation | **NULL conservé tel quel dans le fait/dimension** (85,27 % NULL) — ne pas transformer en "Non renseigné" par défaut ; à la place, exposer un flag `has_marital_status` en Power BI pour que l'utilisateur voie la couverture réelle | FK orpheline → NULL (pas "Non renseigné" forcé) | — | — | — |
| `EducationLevelId` → EducationLevels | idem | `education_level_label` | idem | Trim | Title Case | Dénormalisation | Conservé NULL, taux correct dans le profilage donc pas de traitement spécial | FK orpheline → "Non renseigné" | — | — | — |
| `StatusId` | Employees | `sk_status` (FK) | Int → Int (FK technique) | — | — | Résolution vers `dim_status` | **Ne doit jamais être NULL** (colonne structurante pour "effectif actif") — si NULL, ligne flaguée en contrôle qualité, pas d'invention de statut par défaut | FK orpheline → ligne exclue des KPI "effectif actif" et signalée | — | — | — |
| `CompanyId` | Employees | `sk_company` (FK) | Int → Int | — | — | Résolution vers `dim_company` | Ne doit jamais être NULL (axe multi-tenant obligatoire) | FK orpheline → rejet en contrôle qualité | — | — | — |
| `DepartementId` | Employees | `sk_department` (FK) | Int → Int | — | — | Résolution vers `dim_department` | **Conservé NULL** (90,52 % des cas) — surtout **ne pas** forcer un "département inconnu" qui laisserait croire que la donnée existe ; exposer plutôt un indicateur de couverture `pct_employees_with_department` dans la documentation du dashboard | FK orpheline → NULL | — | — | — |
| `CategoryId` | Employees | *(non chargé comme attribut actif dans dim_employee — reste en staging uniquement)* | — | — | — | — | 99,74 % NULL → décision Groupe C : ne pas exposer en BI | — | — | — | — |
| `ManagerId` | Employees | *(supprimé de dim_employee)* | — | — | — | — | 100 % NULL, aucune valeur exploitable | — | — | — | — |
| `CountryId` (sur Employees) | Employees | *(supprimé)* | — | — | — | — | 100 % NULL | — | — | — | — |
| `BankAccountId` | Employees | *(supprimé)* | — | — | — | — | 100 % NULL | — | — | — | — |
| `PersonalEmail` | Employees | *(supprimé)* | — | — | — | — | 100 % NULL | — | — | — | — |
| `AddressLine1`, `ZipCode`, `CityId` (← EmployeeAddresses, adresse principale sélectionnée, cf. D2) | EmployeeAddresses | `address_line1`, `zip_code`, `sk_city` (FK) | String/Int | Trim | Title Case adresse | Résolution `sk_city` | Conservé NULL si aucune adresse connue pour l'employé | FK ville orpheline → NULL | **Sélection de l'adresse la plus récente par `CreatedAt` par employé, avant tout autre traitement**, pour éviter la duplication de la ligne employé | — | — |

## 2.2 dim_company (← Companies)

| Colonne source | Colonne cible | Nettoyage |
|---|---|---|
| `Id` | `bk_company_id` | Jamais NULL, clé rejetée sinon |
| `CompanyName` | `company_name` | Trim, Title Case |
| `Currency` | `currency_code` | Trim, Uppercase (ISO 4217 attendu) ; valeur hors liste ISO connue → flag "à vérifier", pas de correction automatique |
| `PayrollPeriodicity` | `payroll_periodicity` | Trim, Title Case ; valeur hors énumération attendue (Mensuelle/Bimensuelle/...) → flag, conservé tel quel (pas d'invention) |
| `BusinessSector` | `business_sector` | Trim (texte libre, pas de normalisation forcée d'un référentiel qui n'existe pas côté source) |
| `IceNumber`, `CnssNumber` | `ice_number`, `cnss_number` | Trim ; format numérique national — conservé en texte (peut contenir des zéros non significatifs), pas de cast en Int |
| `ManagedByCompanyId` | `bk_managed_by_company_id` | Auto-référence — si NULL, société "racine" (pas une anomalie, valeur normale) |
| `CityId` | `sk_city` (FK) | Résolution vers dim_city ; FK orpheline → NULL |

## 2.3 dim_department (← Departements)

| Colonne source | Colonne cible | Nettoyage |
|---|---|---|
| `Id` | `bk_department_id` | Jamais NULL |
| `DepartementName` | `department_name` | Trim, Title Case ; **pas d'unicité garantie** (aucune clé métier fournie) → conserver `bk_department_id` comme seule clé fiable, ne jamais dédupliquer sur le nom |
| `HierarchyLevel` | `hierarchy_level` | Cast Int ; valeur négative → flag aberrant |
| `ParentDepartementId` | `bk_parent_department_id` | NULL = département racine (normal, pas une anomalie) |
| `ManagerId` | `bk_manager_employee_id` | Taux de remplissage propre à cette table à vérifier en staging avant décision finale (contrôle qualité Partie 5) |

## 2.4 dim_job_position / dim_contract_type

| Colonne source | Colonne cible | Nettoyage |
|---|---|---|
| `JobPositions.Name` | `job_position_name` | Trim, Title Case |
| `JobPositions.CompanyId` | `sk_company` (FK) | Résolution ; jamais NULL attendu |
| `ContractTypes.ContractTypeName` | `contract_type_name` | Trim, Title Case |
| `LegalContractTypes.Name` (dénormalisé) | `legal_contract_type_name` | Trim, Title Case ; FK orpheline vers ContractTypes → NULL, pas de valeur inventée |
| `StateEmploymentPrograms.Name` (dénormalisé) | `state_employment_program_name` | Trim, Title Case ; conservé NULL si aucun programme rattaché (cas normal, pas toutes les sociétés n'en ont) |

## 2.5 dim_employee_contract (← EmployeeContracts, SCD2)

| Colonne source | Colonne cible | Nettoyage |
|---|---|---|
| `Id` | `bk_contract_id` | Jamais NULL |
| `EmployeeId` | `sk_employee` (FK) | Jamais NULL attendu |
| `JobPositionId` | `sk_job_position` (FK) | FK orpheline → flag qualité, ligne conservée mais signalée |
| `ContractTypeId` | `sk_contract_type` (FK) | idem |
| `StartDate` | `start_date` | Cast Date strict ; jamais NULL (un contrat a toujours une date de début) — si NULL, ligne rejetée en contrôle qualité |
| `EndDate` | `end_date` | Cast Date strict ; **NULL conservé = contrat en cours, c'est l'état normal (80,78 % des cas)**, ne surtout pas remplacer par une date arbitraire | Aberrant si `EndDate < StartDate` → flag, ligne exclue de la résolution temporelle (D1) tant que non corrigée |

## 2.6 dim_employee_salary (← EmployeeSalaries, SCD2)

| Colonne source | Colonne cible | Nettoyage |
|---|---|---|
| `BaseSalary` | `base_salary` | Cast Decimal ; **NULL = problème potentiel** (contrairement à EndDate) — un salaire NULL n'a pas d'interprétation métier "normale", à signaler explicitement dans les contrôles qualité, jamais remplacé par 0 |
| `EffectiveDate` | `effective_date` | Cast Date strict, jamais NULL |
| `EndDate` | `end_date` | NULL conservé = salaire courant (87,10 % des cas), même logique que EmployeeContracts.EndDate | Aberrant si `EndDate < EffectiveDate` → flag |
| `BaseSalary` (valeur) | — | Aberrant si négatif ou si hors plage statistique (à définir via `statistics_tables.csv` min/max réels, pas une borne inventée) → flag, jamais correction silencieuse |

## 2.7 dim_status (← Statuses)

| Colonne source | Colonne cible | Nettoyage |
|---|---|---|
| `Code` | `status_code` | Trim, Uppercase |
| `NameFr/Ar/En` | `status_name_fr/ar/en` | Trim |
| `AffectsPayroll`, `AffectsAttendance`, `AffectsAccess` | `affects_payroll`, `affects_attendance`, `affects_access` | Cast Boolean strict ; jamais NULL attendu (5 lignes de référence, à vérifier en contrôle qualité — si NULL, flag explicite, pas de valeur par défaut True/False inventée) |

## 2.8 dim_leave_type / dim_leave_type_legal_rule

| Colonne source | Colonne cible | Nettoyage |
|---|---|---|
| `LeaveTypes.Nom` | `leave_type_name` | Trim, Title Case |
| `LeaveTypes.CompanyId` | `sk_company` (FK) | Résolution |
| `LeaveTypePolicies.*` (dénormalisé) | attributs politique | Trim/Cast selon type ; conservé NULL si aucune politique définie (cas normal) |
| `LeaveTypeLegalRules.*` | table satellite `dim_leave_type_legal_rule` | Trim texte (article de loi), Cast Int (jours accordés) ; NULL sur jours accordés = à signaler, pas 0 par défaut (0 aurait un sens métier différent : "aucun jour accordé") |

## 2.9 dim_holiday / dim_working_calendar

| Colonne source | Colonne cible | Nettoyage |
|---|---|---|
| `HolidayDate` | `holiday_date` | Cast Date strict, jamais NULL |
| `IsMandatory`, `IsPaid`, `AffectPayroll`, `AffectAttendance` | booléens | Cast Boolean ; NULL conservé et signalé, pas de défaut forcé (un jour férié "non qualifié" doit rester visible comme tel) |
| `DayOfWeek` | `day_of_week` | Cast Int (0–6), valeur hors plage → flag aberrant |
| `IsWorkingDay` | `is_working_day` | Cast Boolean strict |
| `StartTime`/`EndTime` | `start_time`/`end_time` | Cast Time ; `EndTime < StartTime` → flag aberrant (horaire traversant minuit à confirmer métier avant correction) |

## 2.10 dim_alert_definition (← AlertDefinitions + AlertCategories + AlertSeverities)

| Colonne source | Colonne cible | Nettoyage |
|---|---|---|
| `Code` | `alert_definition_code` | Trim, Uppercase |
| `Name` | `alert_definition_name` | Trim, Title Case |
| `CategoryId` → CategoryName | `alert_category_name` | Dénormalisation, Trim, Title Case |
| `SeverityId` → SeverityLabel | `alert_severity_label` | Dénormalisation, **conservé en clair** (Info/Warning/Critical), Trim |
| `IsActive` | `is_active` | Cast Boolean strict |
| `Description`, `SourceField` | *(supprimés)* | 100 % NULL |

## 2.11 dim_city / dim_country

| Colonne source | Colonne cible | Nettoyage |
|---|---|---|
| `CityName` | `city_name` | Trim, Title Case |
| `CountryId` (sur Cities) | `sk_country` (FK) | Résolution ; FK orpheline → NULL |
| `CountryName` | `country_name` | Trim, Title Case |
| `CountryCode` | `country_code` | Trim, Uppercase (ISO 3166 attendu) |

## 2.12 dim_date (construite)

Aucune colonne source à nettoyer — génération pure. Règles :
- Plage : 2019-01-01 → 2027-12-31 (couvre la profondeur historique observée sur `LeaveBalances` avec marge future).
- `AnnéeCivile`, `Mois`, `Trimestre`, `JourSemaine` calculés déterministiquement, aucune valeur source à valider.

## 2.13 fact_payroll (← PayrollResults)

| Colonne source | Colonne cible | Nettoyage |
|---|---|---|
| `EmployeeId`, `CompanyId`, `Month`, `Year` | `sk_employee`, `sk_company`, `sk_date` | FK jamais NULL attendu — ligne rejetée en staging si l'une manque (la ligne serait inexploitable pour toute analyse) |
| `SalaireBase`, `TotalBrut`, `NetAPayer`, etc. (mesures monétaires) | mêmes noms en snake_case | Cast Decimal(18,2) ; **NULL sur une mesure de montant = problème potentiel à signaler**, ne jamais remplacer par 0 (0 aurait un sens métier différent : "montant nul mais connu") ; valeur négative sur une mesure normalement positive (ex. `TotalBrut`) → flag aberrant, pas de correction automatique en valeur absolue |
| `ImpotRevenu`, `BrutImposable` | idem | idem ; cohérence `ImpotRevenu ≤ BrutImposable` vérifiée en contrôle qualité, pas en nettoyage silencieux |
| `Month`, `Year` | `sk_date` (résolu vers le 1er du mois) | Cast Int → Date ; `Month` hors 1–12 ou `Year` hors plage plausible (ex. < 2000) → ligne rejetée et loggée |
| `ClaudeModel`, `TokensUsed`, `ResultatJson`, `ErrorMessage` | *(supprimées)* | 100 % NULL, colonnes techniques |
| ~50 autres rubriques de paie non nommées | conservées en mesures | Même règle générique : Cast Decimal, NULL = à signaler (pas de remplacement par 0), négatif inattendu = flag |

## 2.14 fact_payroll_premium / fact_payroll_tax_snapshot

| Colonne source | Colonne cible | Nettoyage |
|---|---|---|
| `Montant` | `amount` | Cast Decimal ; NULL = problème potentiel (une ligne de prime sans montant n'a pas de sens), à signaler |
| `Label` | `premium_label` | Trim, Title Case ; conservé tel quel comme attribut dégénéré (pas de dictionnaire de correspondance disponible dans le profilage) |
| `IsTaxable` | `is_taxable` | Cast Boolean strict |
| `Ordre` | *(exclu des mesures, conservé en attribut de tri si besoin d'affichage)* | Cast Int |
| `CumulBrut`, `CumulCnss`, `CumulAmo`, `CumulIr`, `CumulNet`, `CumulFp` | mêmes noms | Cast Decimal ; NULL = à signaler, jamais 0 |
| `TauxEffectif` | `taux_effectif` | Cast Decimal ; hors plage [0,1] (si exprimé en ratio) → flag aberrant |

## 2.15 fact_absence_monthly / fact_leave_monthly / fact_worked_days_monthly / fact_overtime_monthly

| Colonne source | Colonne cible | Nettoyage |
|---|---|---|
| `EmployeeId`, `Year`, `Month` | `sk_employee`, `sk_date` | Jamais NULL, base du grain confirmé par contrainte applicative |
| `AbsenceDays` / `LeaveDays` / `WorkedDays` / `Hours25Pct`/`50`/`100` | mêmes noms en snake_case | Cast Decimal/Int ; **NULL = problème potentiel** sur un mois où l'employé est actif (à croiser avec `dim_status.AffectsPayroll`/`AffectsAttendance` en contrôle qualité) ; valeur négative → flag aberrant (impossible métier) ; valeur > nombre de jours du mois (pour AbsenceDays/LeaveDays/WorkedDays) → flag aberrant |

## 2.16 fact_leave_balance (← LeaveBalances)

| Colonne source | Colonne cible | Nettoyage |
|---|---|---|
| `OpeningDays`, `AccruedDays`, `UsedDays`, `CarryInDays`, `CarryOutDays`, `ClosingDays` | mêmes noms | Cast Decimal ; NULL = à signaler (pas 0, un solde inconnu n'est pas un solde nul) ; cohérence `ClosingDays ≈ OpeningDays + AccruedDays + CarryInDays − UsedDays` vérifiée en contrôle qualité, écart significatif → flag, pas de recalcul silencieux qui écraserait la donnée source |
| `CarryoverExpiresOn` | `carryover_expires_on` | Cast Date ; NULL conservé = pas de report avec expiration (cas normal) |
| `EmployeeId`, `LeaveTypeId`, `Year`, `Month` | FK | Jamais NULL, base du grain confirmé |

## 2.17 fact_salary_component (← EmployeeSalaryComponents)

| Colonne source | Colonne cible | Nettoyage |
|---|---|---|
| `Amount` | `amount` | Cast Decimal ; NULL = à signaler si `Percentage` est également NULL (aucune valorisation possible) |
| `Percentage` | `percentage` | Cast Decimal ; NULL conservé = composant en montant fixe (cohérent avec >99 % NULL, cas normal documenté) |
| `ComponentType`, `IsTaxable`, `IsSocial`, `IsCIMR` | mêmes noms | Trim/Cast Boolean ; conservés tels quels, attributs de filtre |
| `EndDate` | `end_date` | NULL conservé = composant actif (cohérent, >99 % NULL) |

## 2.18 fact_alert (← Alerts)

| Colonne source | Colonne cible | Nettoyage |
|---|---|---|
| `Status` | `status` | Trim, valeurs à cataloguer (échantillon de valeurs distinctes non fourni dans le profilage — à obtenir avant la phase de code, cf. audit) |
| `IsAcknowledged` | `is_acknowledged` | Cast Boolean strict |
| `SeverityId`, `CategoryId → CategoryName` | dénormalisés vers `sk_alert_definition` | Résolution FK |
| `EntityType` | `entity_type` | Trim ; conservé comme texte dégénéré, **jamais utilisé en jointure** (cf. D4) |
| `EntityId` | `entity_id` | Conservé comme texte informatif, **jamais utilisé en jointure** |
| `ResolvedAt` | `resolved_at` | Cast Date ; **NULL conservé à 92,19 %** — n'est pas transformé en date fictive ; le KPI "taux de résolution" reste documenté comme peu représentatif tant que ce taux ne baisse pas |
| `AcknowledgedAt`, `AcknowledgedByUserId` | *(conservés en attributs mais signalés inexploitables)* | 100 % NULL — aucun traitement possible, pas de KPI construit dessus (cf. catalogue KPI) |

## 2.19 fact_leave_request / fact_absence_event

| Colonne source | Colonne cible | Nettoyage |
|---|---|---|
| `CalendarDays`, `WorkingDaysDeducted` | mêmes noms | Cast Decimal ; NULL = à signaler, pas 0 |
| `Status` | `status` | Trim ; valeurs à cataloguer (échantillon non fourni) |
| `RequestedAt`, `DecisionAt` | `requested_at`, `decision_at` | Cast Date ; `DecisionAt < RequestedAt` → flag aberrant |
| `DurationType` | `duration_type` | Trim ; valeurs à cataloguer |

---
---

# PARTIE 3 — Plan des jointures ETL

## 3.1 Construction des dimensions (jointures de dénormalisation)

| # | Table gauche | Table droite | Col. gauche | Col. droite | Type | Cardinalité | Raison métier | Risque de duplication | Prévention |
|---|---|---|---|---|---|---|---|---|---|
| J1 | Employees | Genders | `GenderId` | `Id` | LEFT JOIN | N:1 | Dénormaliser le libellé genre dans dim_employee | Aucun (droite = référentiel 2 lignes, clé unique) | FK droite garantie unique par construction (référentiel) |
| J2 | Employees | Nationalities | `NationalityId` | `Id` | LEFT JOIN | N:1 | Dénormaliser le libellé nationalité | Aucun | idem |
| J3 | Employees | MaritalStatuses | `MaritalStatusId` | `Id` | LEFT JOIN | N:1 | Dénormaliser le statut marital | Aucun | idem |
| J4 | Employees | EducationLevels | `EducationLevelId` | `Id` | LEFT JOIN | N:1 | Dénormaliser le niveau d'éducation | Aucun | idem |
| J5 | Employees | EmployeeAddresses (déjà filtrée sur adresse principale, cf. D2) | `Id` | `EmployeeId` | LEFT JOIN | **1:1 après dédoublonnage** (source brute 1:N, 1213 adresses/1561 employés) | Rattacher l'adresse principale à l'employé | **Fort avant dédoublonnage** : un employé avec 2 adresses génère 2 lignes | Sélection de l'adresse la plus récente par `CreatedAt` **avant** la jointure (sous-requête `ROW_NUMBER() OVER (PARTITION BY EmployeeId ORDER BY CreatedAt DESC) = 1`), jamais une jointure brute |
| J6 | EmployeeAddresses (dédoublonnée) | Cities | `CityId` | `Id` | LEFT JOIN | N:1 | Résoudre `sk_city` | Aucun | Clé Cities unique |
| J7 | Cities | Countries | `CountryId` | `Id` | LEFT JOIN | N:1 | Résoudre `sk_country` (hiérarchie géographique) | Aucun | Clé Countries unique |
| J8 | Companies | Cities | `CityId` | `Id` | LEFT JOIN | N:1 | Résoudre `sk_city` de la société | Aucun | idem |
| J9 | ContractTypes | LegalContractTypes | `LegalContractTypeId` | `Id` | LEFT JOIN | N:1 | Dénormaliser le libellé légal dans dim_contract_type | Aucun | Référentiel 5 lignes |
| J10 | ContractTypes | StateEmploymentPrograms | `StateEmploymentProgramId` | `Id` | LEFT JOIN | N:1 | Dénormaliser le programme d'aide à l'emploi | Aucun | Référentiel 3 lignes |
| J11 | LeaveTypes | LeaveTypePolicies | `Id` | `LeaveTypeId` | LEFT JOIN | **1:1 ou 1:N à vérifier** (6 politiques pour ? types) | Dénormaliser la politique dans dim_leave_type | Si 1:N réel (plusieurs politiques par type), duplication de la ligne dim_leave_type | À vérifier avant code : si N:1 confirmé, JOIN direct ; si 1:N réel, ne prendre que la politique active/la plus récente (règle à définir avec l'équipe métier avant code) |
| J12 | LeaveTypes | LeaveTypeLegalRules | `Id` | `LeaveTypeId` | LEFT JOIN | 1:N (10 règles / 7 types, confirmé N:1 vers dim_leave_type) | Construire la table satellite `dim_leave_type_legal_rule` | Aucun risque sur dim_leave_type elle-même car conservée en table séparée (pas aplatie) | Ne pas aplatir dans dim_leave_type — garder la relation 1-N intacte dans une dimension satellite |
| J13 | AlertDefinitions | AlertCategories | `CategoryId` | `Id` | LEFT JOIN | N:1 | Dénormaliser la catégorie d'alerte | Aucun | Référentiel 6 lignes |
| J14 | AlertDefinitions | AlertSeverities | `SeverityId` | `Id` | LEFT JOIN | N:1 | Dénormaliser la sévérité en clair | Aucun | Référentiel 3 lignes |

## 3.2 Résolution temporelle (point de modélisation D1)

| # | Table gauche | Table droite | Col. gauche | Col. droite | Type | Cardinalité | Raison métier | Risque de duplication | Prévention |
|---|---|---|---|---|---|---|---|---|---|
| J15 | PayrollResults | EmployeeContracts | `EmployeeId` + date bulletin (1er du mois) | `EmployeeId` avec `StartDate ≤ date bulletin AND (EndDate IS NULL OR EndDate ≥ date bulletin)` | LEFT JOIN avec condition d'intervalle (temporal join) | **N:N potentiel si chevauchement de contrats** — attendu N:1 (un seul contrat actif par employé à une date donnée) | Résoudre le poste/type de contrat en vigueur au moment du bulletin (D1) | **Fort si deux contrats se chevauchent** (erreur de données côté source) → dupliquerait la ligne de `fact_payroll` | Après la jointure, sélectionner explicitement **un seul** contrat par (EmployeeId, date bulletin) — priorité au `StartDate` le plus récent en cas de chevauchement détecté — et **logger** chaque cas de chevauchement comme anomalie de données source (contrôle qualité Partie 5), jamais silencieusement ignoré |
| J16 | (résultat J15) | JobPositions | `JobPositionId` | `Id` | LEFT JOIN | N:1 | Résoudre `sk_job_position_at_payroll` | Aucun après J15 sécurisée | — |
| J17 | (résultat J15) | ContractTypes | `ContractTypeId` | `Id` | LEFT JOIN | N:1 | Résoudre `sk_contract_type_at_payroll` | Aucun après J15 sécurisée | — |

## 3.3 Construction des faits (rattachement aux dimensions)

| # | Table gauche | Table droite | Col. gauche | Col. droite | Type | Cardinalité | Raison métier | Risque de duplication | Prévention |
|---|---|---|---|---|---|---|---|---|---|
| J18 | PayrollResults | dim_employee | `EmployeeId` | `bk_employee_id` | INNER JOIN | N:1 | Résoudre `sk_employee` | Aucun (dim_employee a une ligne par employé) | Contrainte d'unicité vérifiée à la construction de dim_employee (Partie 5) |
| J19 | PayrollResults | dim_company | `CompanyId` | `bk_company_id` | INNER JOIN | N:1 | Résoudre `sk_company` | Aucun | idem |
| J20 | PayrollResults | dim_date | `Month`,`Year` (→ 1er du mois) | `sk_date` | INNER JOIN | N:1 | Résoudre `sk_date` | Aucun | dim_date construite avec exactement une ligne par jour calendaire |
| J21 | PayrollResultPrimes | fact_payroll (déjà construite) | `PayrollResultId` | `bk_payroll_id` (= ancien `Id` de PayrollResults conservé comme clé métier) | INNER JOIN | N:1 | Rattacher chaque prime à son bulletin, **et dénormaliser** `sk_employee`/`sk_company`/`sk_date` du parent (cf. modèle 1.1) | Aucun — chaque ligne de PayrollResultPrimes reste une ligne, la jointure ne fait qu'enrichir | — |
| J22 | PayrollTaxSnapshots | fact_payroll | `PayrollResultId` | `bk_payroll_id` | LEFT JOIN | **1:1 attendu** (confirmé par le schéma applicatif) | Rattacher le snapshot fiscal à son bulletin | Si la relation 1:1 est violée côté source (plusieurs snapshots pour un même bulletin), duplication de `fact_payroll_tax_snapshot` | Contrôle qualité explicite de l'unicité (PayrollResultId) sur PayrollTaxSnapshots avant chargement (Partie 5) |
| J23 | EmployeeMonthlyAbsenceSummaries | dim_employee, dim_company, dim_date | `EmployeeId`,`Year`,`Month` | `bk_employee_id`, (`sk_company` via employé), `sk_date` | INNER JOIN | N:1 chacune | Construire fact_absence_monthly | Aucun (grain confirmé unique par contrainte applicative) | Contrôle d'unicité (EmployeeId,Year,Month) en amont malgré tout (Partie 5), le profilage `Id` seul ne suffit pas |
| J24 | EmployeeMonthlyLeaveSummaries | dim_employee, dim_company, dim_date | idem | idem | INNER JOIN | N:1 | Construire fact_leave_monthly | Idem J23 | Idem J23 |
| J25 | EmployeeMonthlyWorkedDaysSummaries | dim_employee, dim_company, dim_date | idem | idem | INNER JOIN | N:1 | Construire fact_worked_days_monthly | Idem | Idem |
| J26 | EmployeeMonthlyOvertimeSummaries | dim_employee, dim_company, dim_date | idem | idem | INNER JOIN | N:1 | Construire fact_overtime_monthly | Idem | Idem |
| J27 | LeaveBalances | dim_employee, dim_leave_type, dim_company, dim_date | `EmployeeId`,`LeaveTypeId`,`Year`,`Month` | correspondants | INNER JOIN | N:1 chacune | Construire fact_leave_balance | Aucun (unicité confirmée EmployeeId×LeaveTypeId×Year×Month) | Contrôle d'unicité malgré tout en Partie 5 |
| J28 | EmployeeSalaryComponents | dim_employee_salary | `EmployeeSalaryId` | `bk_employee_salary_id` | INNER JOIN | N:1 | Rattacher le composant à sa version de salaire | Aucun | — |
| J29 | (résultat J28) | dim_employee | `EmployeeId` (via EmployeeSalaries) | `bk_employee_id` | INNER JOIN (dénormalisation) | N:1 | Ajouter `sk_employee` directement sur fact_salary_component pour usage BI direct | Aucun | — |
| J30 | Alerts | dim_company, dim_alert_definition, dim_date | `CompanyId`,`AlertDefinitionId`(ou équivalent),`CreatedAt` | correspondants | LEFT JOIN sur dim_alert_definition (une alerte peut ne pas avoir de définition rattachée si générée manuellement — à vérifier), INNER sur dim_company/dim_date | N:1 chacune | Construire fact_alert | Aucun si les clés sont bien N:1 | — |
| J31 | LeaveRequests | dim_employee, dim_company, dim_leave_type, dim_date | correspondants | correspondants | INNER JOIN | N:1 chacune | Construire fact_leave_request | Aucun (2 lignes seulement, mais grain fiable) | — |
| J32 | EmployeeAbsences | dim_employee, dim_company, dim_date | correspondants | correspondants | INNER JOIN | N:1 chacune | Construire fact_absence_event | Aucun | — |

## 3.4 Règle transverse sur toutes les jointures

Aucune jointure n'est faite sur la base d'une simple similarité de nom de colonne. Chaque jointure listée ci-dessus est justifiée par une FK déclarée dans `fk_tables.csv`, ou (pour J15, cas explicitement documenté sans FK technique) par une règle métier documentée dans l'audit structurel (jointure temporelle contrat/bulletin). Les références polymorphes (`Alerts.EntityType/EntityId`) sont **exclues de ce plan de jointures** : elles ne sont jamais jointes (cf. D4).

---
---

# PARTIE 4 — Plan logique de transformation

## 4.0 Principes transverses

- **Clés techniques des dimensions** : chaque dimension reçoit une clé surrogate entière auto-incrémentée (`sk_*`), générée à la construction du DW — jamais réutilisée directement depuis l'`Id` source (qui reste conservé en attribut `bk_*`, clé métier, pour la traçabilité et le rejeu des chargements).
- **Gestion des clés métier** : `bk_*` = `Id` de la table source. Utilisée pour les jointures de staging et pour la logique SCD2 (retrouver l'historique d'un même employé/contrat/salaire dans le temps), jamais exposée seule en Power BI (l'utilisateur voit les attributs métier, pas les ID techniques).
- **Valeurs inconnues** : une ligne dimension "Non renseigné" (`sk_* = -1` ou équivalent) est créée **uniquement** pour les FK utilisées comme axe de regroupement obligatoire dans un KPI catalogué comme calculable (ex. genre, nationalité) — jamais de manière systématique sur toutes les dimensions. Pour les colonnes à réserve forte (département, statut marital, catégorie), le NULL est conservé tel quel et documenté, conformément à la Partie 2.
- **Construction de dim_date** : génération procédurale (pas de source), une ligne par jour de 2019-01-01 à 2027-12-31, attributs calculés déterministiquement (année, mois, trimestre, jour de semaine).
- **Calcul des mesures** : les mesures sont chargées telles quelles depuis la source après cast de type et contrôle de plausibilité (Partie 2) — **aucun recalcul silencieux** qui masquerait un écart avec la source (ex. `ClosingDays` de `LeaveBalances` n'est jamais recalculé pour "corriger" un écart avec `Opening+Accrued-Used`, l'écart est seulement flagué).
- **Gestion des dates historiques** : les dimensions SCD2 (`dim_employee_contract`, `dim_employee_salary`) conservent l'intégralité de l'historique source (`StartDate`/`EffectiveDate` et `EndDate`) — aucune ligne historique n'est écrasée ou supprimée lors des chargements incrémentaux.
- **Gestion des doublons** : détection sur clé métier + combinaison métier (pas seulement `Id`) avant chargement, cf. contrôles Partie 5. En cas de doublon fonctionnel détecté (ex. deux bulletins pour le même employé/mois), la ligne est **isolée en table de rejet**, jamais fusionnée ou moyennée automatiquement (décision métier requise).
- **Enregistrements invalides** : toute ligne échouant à un contrôle bloquant (FK manquante sur colonne obligatoire, clé NULL, grain violé) est écartée du chargement en dimension/fait et déposée dans une table de rejet horodatée, avec le motif — jamais silencieusement ignorée.

## 4.1 Flux par table source

### Dimensions

| Table source | Transformation appliquée | Table cible | Colonnes créées | Colonnes supprimées | Colonnes renommées | Règles métier |
|---|---|---|---|---|---|---|
| Employees (+ Genders, Nationalities, MaritalStatuses, EducationLevels, EmployeeAddresses, Cities) | Dédoublonnage adresse (D2), dénormalisation J1–J8, calcul de l'âge/tranche d'âge à partir de `DateOfBirth`, résolution des FK Status/Company/Department | dim_employee | `sk_employee` (surrogate), `age`, `age_bracket`, libellés dénormalisés (genre, nationalité, statut marital, niveau d'éducation) | Colonnes techniques EF Core, `CategoryId`, `ManagerId`, `CountryId`, `BankAccountId`, `PersonalEmail` (cf. Partie 2) | `Id`→`bk_employee_id`, `Nom`→`last_name`, etc. | Sélection de l'adresse principale avant jointure (D2) ; libellés "Non renseigné" uniquement pour genre/nationalité (axes de KPI directs), NULL conservé ailleurs |
| Companies (+ Cities) | Dénormalisation J8 | dim_company | `sk_company` | Techniques EF Core, champs secondaires à très fort NULL non chiffré | `Id`→`bk_company_id` | Aucune valeur "Non renseigné" forcée — société toujours identifiée par son nom réel |
| Departements | Aucune transformation complexe, chargement direct | dim_department | `sk_department` | Techniques EF Core | `Id`→`bk_department_id` | Conservée malgré la réserve de couverture (D6) |
| JobPositions | Chargement direct | dim_job_position | `sk_job_position` | Techniques EF Core | `Id`→`bk_job_position_id` | — |
| ContractTypes (+ LegalContractTypes, StateEmploymentPrograms) | Dénormalisation J9–J10 | dim_contract_type | `sk_contract_type` | Techniques EF Core | `Id`→`bk_contract_type_id` | — |
| EmployeeContracts | Chargement SCD2 intégral (historique conservé) | dim_employee_contract | `sk_employee_contract` | Techniques EF Core | `Id`→`bk_contract_id` | `EndDate` NULL = contrat actif, jamais réécrit |
| EmployeeSalaries | Chargement SCD2 intégral | dim_employee_salary | `sk_employee_salary` | Techniques EF Core | `Id`→`bk_employee_salary_id` | `EndDate` NULL = salaire courant |
| Statuses | Chargement direct (5 lignes) | dim_status | `sk_status` | Techniques EF Core | `Id`→`bk_status_id` | Flags booléens jamais réinterprétés |
| LeaveTypes (+ LeaveTypePolicies) | Dénormalisation J11 (règle à trancher si 1:N réel) | dim_leave_type | `sk_leave_type` | Techniques EF Core | `Id`→`bk_leave_type_id` | — |
| LeaveTypeLegalRules | Chargement en table satellite (J12) | dim_leave_type_legal_rule | `sk_leave_legal_rule` | Techniques EF Core | `Id`→`bk_legal_rule_id` | Relation 1-N conservée, pas aplatie |
| Holidays | Chargement direct | dim_holiday | `sk_holiday` | Techniques EF Core | `Id`→`bk_holiday_id` | Non liée à dim_date (D3) |
| WorkingCalendars | Chargement direct | dim_working_calendar | `sk_working_calendar` | Techniques EF Core | `Id`→`bk_working_calendar_id` | Non liée à dim_date (D3) |
| AlertDefinitions (+ AlertCategories, AlertSeverities) | Dénormalisation J13–J14 | dim_alert_definition | `sk_alert_definition` | `Description`, `SourceField` (100 % NULL), techniques | `Id`→`bk_alert_definition_id` | Sévérité conservée en clair |
| Cities, Countries | Chargement direct + hiérarchie | dim_city, dim_country | `sk_city`, `sk_country` | Techniques EF Core | `Id`→`bk_city_id`/`bk_country_id` | Dimensions conformées partagées (D5) |
| — (générée) | Génération procédurale | dim_date | `sk_date`, tous les attributs calendaires | — | — | Plage 2019–2027 |

### Faits

| Table source | Transformation appliquée | Table cible | Colonnes créées | Colonnes supprimées | Colonnes renommées | Règles métier |
|---|---|---|---|---|---|---|
| PayrollResults | Résolution temporelle J15–J17 (poste/contrat au bulletin), résolution FK J18–J20, cast des ~60 mesures monétaires | fact_payroll | `sk_payroll`, `sk_job_position_at_payroll`, `sk_contract_type_at_payroll` | `ClaudeModel`, `TokensUsed`, `ResultatJson`, `ErrorMessage`, techniques EF Core | `Id`→`bk_payroll_id` | Contrôle d'unicité (EmployeeId,Month,Year) **obligatoire avant chargement** (grain non confirmé par le profilage) |
| PayrollResultPrimes | Rattachement + dénormalisation J21 | fact_payroll_premium | `sk_payroll_premium`, `sk_employee`/`sk_company`/`sk_date` dénormalisées | Techniques | `Id`→`bk_payroll_premium_id` | Ne jamais sommer avec `TotalPrimesImposables` de fact_payroll dans une même mesure Power BI |
| PayrollTaxSnapshots | Rattachement 1:1 J22 | fact_payroll_tax_snapshot | `sk_payroll_tax_snapshot` | Techniques | `Id`→`bk_tax_snapshot_id` | Contrôle d'unicité (PayrollResultId) obligatoire |
| EmployeeMonthlyAbsenceSummaries | Rattachement J23 | fact_absence_monthly | `sk_absence_monthly` | Techniques | `Id`→`bk_absence_monthly_id` | — |
| EmployeeMonthlyLeaveSummaries | Rattachement J24 | fact_leave_monthly | `sk_leave_monthly` | Techniques | `Id`→`bk_leave_monthly_id` | Pas de `sk_leave_type` (colonne non confirmée) |
| EmployeeMonthlyWorkedDaysSummaries | Rattachement J25 | fact_worked_days_monthly | `sk_worked_days_monthly` | Techniques | `Id`→`bk_worked_days_monthly_id` | — |
| EmployeeMonthlyOvertimeSummaries | Rattachement J26 | fact_overtime_monthly | `sk_overtime_monthly` | Techniques | `Id`→`bk_overtime_monthly_id` | Chargée mais non exposée au dashboard v1 |
| LeaveBalances | Rattachement J27, contrôle de cohérence Opening/Accrued/Used/Closing (flag, pas recalcul) | fact_leave_balance | `sk_leave_balance` | Techniques | `Id`→`bk_leave_balance_id` | — |
| EmployeeSalaryComponents | Rattachement J28–J29 | fact_salary_component | `sk_salary_component`, `sk_employee` dénormalisée | Techniques | `Id`→`bk_salary_component_id` | — |
| Alerts | Rattachement J30 | fact_alert | `sk_alert` | Techniques | `Id`→`bk_alert_id` | `EntityType`/`EntityId` jamais joints (D4) |
| LeaveRequests | Rattachement J31 | fact_leave_request | `sk_leave_request` | Techniques | `Id`→`bk_leave_request_id` | Non mis en avant tant que volumétrie faible |
| EmployeeAbsences | Rattachement J32 | fact_absence_event | `sk_absence_event` | Techniques | `Id`→`bk_absence_event_id` | Idem |

---
---

# PARTIE 5 — Système de contrôle qualité des données

## 5.0 Principe

Chaque contrôle produit une ligne de log structurée : `{table, contrôle, règle, seuil, résultat_observé, statut (OK/WARNING/FAIL), action}`. Les contrôles bloquants (FAIL) empêchent le chargement en production de la table concernée ; les contrôles d'alerte (WARNING) chargent la table mais loguent l'anomalie pour revue.

## 5.1 Contrôles génériques (appliqués à chaque table source ET chaque table transformée)

| Contrôle | Règle | Seuil acceptable | Résultat attendu | Action si échec |
|---|---|---|---|---|
| Nombre de lignes | `COUNT(*)` source vs cible | Écart 0 % pour les tables sans filtre (INNER JOIN sans rejet attendu) ; écart documenté et justifié pour les tables avec règle de filtrage (ex. dédoublonnage adresse) | Nombre de lignes cible = nombre de lignes source − rejets justifiés | WARNING si écart non expliqué < 1 % ; FAIL si écart > 1 % non justifié → chargement suspendu, investigation |
| Comparaison avant/après transformation | Somme de contrôle sur une mesure clé (ex. `SUM(TotalBrut)`) avant/après cast de type | Écart = 0 (aucune perte de précision attendue sur un cast Decimal correct) | Somme identique à la précision près | FAIL si écart > seuil de précision attendu (ex. 0,01 par ligne) → vérifier le cast |
| Doublons sur clé technique | `COUNT(Id) - COUNT(DISTINCT Id)` | 0 | 0 doublon | FAIL → rejet des doublons en table d'anomalie, chargement des lignes uniques uniquement |
| Doublons sur clé métier/grain | `COUNT(*) - COUNT(DISTINCT combinaison métier)` (ex. EmployeeId+Month+Year pour PayrollResults) | 0 pour les tables à unicité confirmée par le schéma applicatif ; **à mesurer sans a priori** pour PayrollResults (unicité non confirmée) | 0 | FAIL sur les tables à unicité confirmée → chargement bloqué, investigation obligatoire ; pour PayrollResults, tout doublon détecté est un résultat en soi à documenter avant de fixer le grain définitif du fait |
| Valeurs NULL sur colonnes obligatoires (clés, FK structurantes) | `COUNT(*) WHERE colonne IS NULL` | 0 sur `Id`, `EmployeeId`, `CompanyId` des tables de faits | 0 | FAIL → ligne rejetée en table d'anomalie |
| Valeurs NULL sur colonnes à réserve connue (ex. `DepartementId`) | `% NULL` mesuré | Documenté, pas de seuil bloquant (réserve déjà actée en conception) | Taux stable dans le temps (± 5 points par rapport au profilage initial) | WARNING si le taux dérive fortement (signe d'un problème d'extraction, pas de la donnée elle-même) |
| Clés étrangères orphelines | `LEFT JOIN ... WHERE dimension.sk IS NULL` sur une FK censée exister | 0 sur les FK obligatoires (`EmployeeId`, `CompanyId`) ; toléré et logué sur les FK à réserve (`DepartementId`, `JobPositionId` résolu) | 0 orpheline sur FK obligatoires | FAIL sur FK obligatoire → ligne rejetée ; WARNING sur FK à réserve → ligne conservée avec `sk = NULL`, loguée |
| Dates invalides | Date hors plage plausible (ex. `DateOfBirth` < 1900 ou > date du jour ; `EndDate < StartDate`) | 0 | 0 | WARNING → ligne conservée mais flaguée pour revue métier (pas de correction automatique) |
| Valeurs négatives incorrectes | Mesure normalement positive (montants de paie, jours) trouvée négative | 0 | 0 | FAIL sur les mesures monétaires structurantes (`TotalBrut`, `NetAPayer`) → ligne exclue des agrégats et loguée ; WARNING sur les mesures secondaires |
| Valeurs aberrantes (statistiques) | Comparaison à `statistics_tables.csv` (min/max/écart-type réels, pas de borne inventée) — valeur au-delà de ex. 3 écarts-types | Aucune valeur hors plage observée dans le profilage initial | Distribution stable | WARNING → conservée mais signalée, jamais tronquée/plafonnée automatiquement (une valeur extrême peut être réelle) |
| Incohérences entre tables | Ex. `SUM(AbsenceDays + WorkedDays)` d'un employé/mois > jours ouvrés théoriques du mois (`WorkingCalendars`) | 0 | 0 | WARNING → flaguée pour revue, ne bloque pas le chargement (donnée métier potentiellement légitime, ex. jours fériés comptés différemment) |
| Valeurs inconnues (FK résolue en "Non renseigné") | `% de lignes avec sk = -1` sur les dimensions où ce mécanisme est utilisé (genre, nationalité) | Doit rester cohérent avec le % NULL déjà documenté en conception | Stable | WARNING si le taux augmente fortement par rapport au profilage de référence |

## 5.2 Contrôles spécifiques par table (au-delà du générique)

| Table | Contrôle spécifique | Règle | Seuil | Action si échec |
|---|---|---|---|---|
| `PayrollResults` → `fact_payroll` | Unicité (EmployeeId, Month, Year) | `COUNT(*) GROUP BY EmployeeId,Month,Year HAVING COUNT(*) > 1` | 0 attendu, **non confirmé par le profilage initial** | **Bloquant tant que non vérifié une première fois** — si des doublons apparaissent, le grain du fait doit être revu avant tout chargement en production |
| `PayrollTaxSnapshots` → `fact_payroll_tax_snapshot` | Unicité (PayrollResultId) — relation 1:1 attendue | idem | 0 | FAIL → investigation, la relation 1:1 annoncée par le schéma applicatif doit être confirmée avant chargement |
| `EmployeeMonthlyAbsenceSummaries`, `LeaveSummaries`, `WorkedDaysSummaries`, `OvertimeSummaries` | Unicité (EmployeeId, Year, Month) | idem | 0 (confirmé par contrainte applicative) | FAIL → contrainte applicative visiblement violée dans l'extrait, à signaler à l'équipe source |
| `LeaveBalances` | Unicité (EmployeeId, LeaveTypeId, Year, Month) | idem | 0 (confirmé) | FAIL |
| `LeaveBalances` | Cohérence `ClosingDays ≈ OpeningDays + AccruedDays + CarryInDays − UsedDays` | Écart toléré (arrondi) | ± 0,5 jour | WARNING → flaguée, jamais recalculée automatiquement |
| `EmployeeContracts` résolution temporelle (J15) | Chevauchement de contrats actifs pour un même employé | `COUNT(contrats actifs simultanés) > 1` à une date donnée | 0 | FAIL sur la ligne de fait concernée → poste/contrat résolu à NULL et loguée, jamais choisi arbitrairement sans trace |
| `Employees` | Taux de remplissage `DepartementId`, `ManagerId`, `CategoryId`, `MaritalStatusId` | `% NULL` | Documenté (90,52 % / 100 % / 99,74 % / 85,27 % au profilage initial) | WARNING si le taux **s'améliore ou se dégrade fortement** par rapport à cette référence (signal à remonter au produit, pas une erreur ETL) |
| `Alerts` | Taux de remplissage `ResolvedAt`, `AcknowledgedAt` | `% NULL` | Documenté (92,19 % / 100 % au profilage initial) | WARNING si dérive significative |
| `EmployeeSalaries` | `BaseSalary` NULL | `COUNT(*) WHERE BaseSalary IS NULL` | 0 attendu (un salaire NULL est un problème, cf. Partie 2) | FAIL → ligne exclue des KPI de rémunération et loguée explicitement, jamais silencieusement acceptée |
| `LeaveRequests`, `EmployeeAbsences`, `EmployeeMonthlyOvertimeSummaries` | Volumétrie | `COUNT(*)` total | Documenté comme faible (2, 2, 4 lignes au profilage initial) | WARNING permanent tant que le volume reste sous un seuil de représentativité à définir avec l'équipe métier (ex. < 100 lignes) — dashboard marqué "échantillon limité" sur ces axes |

## 5.3 Format de log recommandé

Chaque exécution de contrôle produit une ligne structurée, lisible en revue manuelle et exploitable pour un tableau de bord de supervision ETL :

```
[TIMESTAMP] [TABLE] [CONTRÔLE] [RÈGLE] → OBSERVÉ=<valeur> / SEUIL=<valeur> / STATUT=<OK|WARNING|FAIL> / ACTION=<description>
```

Les logs `FAIL` doivent interrompre le chargement de la table concernée (pas de la totalité du pipeline sauf si la table est structurante — `Employees`, `Companies`) ; les `WARNING` sont chargés et consolidés dans un rapport de qualité hebdomadaire adressé à l'équipe data.

---

*Aucun code (PySpark, SQL, DDL) n'a été généré à ce stade, conformément à la demande. Ce document sert de spécification de référence pour la phase de code à venir.*
