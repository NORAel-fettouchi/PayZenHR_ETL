# Audit structurel de PayZenHR_Source — Analyse préparatoire au Data Warehouse

**Objectif :** identifier, table par table, sur la seule base des fichiers de profilage fournis, ce qui mérite d'entrer dans le modèle en étoile ClickHouse. Aucun code n'est produit à ce stade.

---

## 0. Ce que disent réellement les fichiers fournis

| Fichier | Contenu | Ce qu'il permet de dire |
|---|---|---|
| `rapport_tables.csv` | Nb lignes / colonnes par table | Volumétrie réelle → 103 tables profilées (pas 95) |
| `columns_tables.csv` | Colonnes, types Spark, nullable | Typologie technique (ID, date, texte, montant) |
| `nulls_tables.csv` | % NULL par colonne | Fiabilité réelle de chaque colonne |
| `duplicates_tables.csv` | Total / Distinct / Doublons | Qualité des clés |
| `fk_tables.csv` | 155 FK déclarées | Relations réelles entre tables |
| `statistics_tables.csv` | Min/Max/Moyenne/Écart-type | Plausibilité des mesures numériques |

**Ce qui manque et que je n'invente pas** : aucun fichier de profilage des **clés primaires** n'a été transmis (le point 4 annoncé dans le message n'est pas dans les 6 fichiers reçus). Je pars donc de l'observation — pas de la certitude — que chaque table a une colonne `Id` de type `IntegerType()` en première position, ce qui est la convention Entity Framework Core, mais ceci reste une hypothèse à confirmer par le fichier PK ou par le DBA. Toute mention de "PK probable = Id" dans ce document est signalée comme telle.

### Constats transverses (avant même de regarder table par table)

1. **103 tables au total**, pas 95 — à clarifier avec l'équipe source (tables supprimées/ajoutées depuis le comptage initial ?).
2. **Aucun doublon détecté nulle part** (`duplicates_tables.csv` : `Doublons = 0` sur les 103 tables). C'est cohérent avec des clés `Id` auto-incrémentées, mais signifie aussi que le profilage doublons n'a probablement testé que la colonne `Id`, pas des combinaisons métier (ex. un même employé avec deux bulletins pour le même mois). Ce point est à vérifier spécifiquement pour `PayrollResults` (Employee+Month+Year devrait être unique).
3. **44 des 103 tables ont 0 ligne** au moment du profilage — soit 43 % de la base. Ce n'est pas un défaut de profilage, c'est une donnée réelle et structurante : une bonne partie du moteur de règles de paie (`RuleCaps`, `RuleFormulas`, `RuleTiers`, `RuleVariants`, `RuleDualCaps`, `RulePercentages`, `ElementRules`, `ReferentielElements`), le pointage (`EmployeeAttendances`, `EmployeeAttendanceBreaks`), les heures supplémentaires (`EmployeeOvertimes`), les packages salariaux (`SalaryPackages` et ses tables liées) et une partie de la conformité CNSS (`CnssPreetabliImports/Lines`) ne contiennent **aucune donnée exploitable aujourd'hui**.
4. **Colonnes techniques omniprésentes** : `CreatedAt/By`, `UpdatedAt/By`, `DeletedAt/By` sur quasiment les 103 tables (soft delete + audit EF Core). Ce sont des colonnes techniques, jamais des colonnes métier, à exclure systématiquement du DW sauf besoin explicite de traçabilité.
5. **Tables hubs (les plus référencées par FK)** : `Companies` (29 FK entrantes), `Employees` (24), `Users` (12). Ce sont les vraies clés de voûte du modèle relationnel — cohérent avec une architecture multi-tenant (`Companies`) centrée sur l'employé.
6. `PayrollResults` contient deux colonnes surprenantes — `ClaudeModel` et `TokensUsed` (100 % NULL) — qui suggèrent qu'un moteur de calcul assisté par IA a été implémenté ou testé sur ce flux. Sans impact sur le DW (colonnes à exclure), mais à signaler car inattendu pour une table de paie.
7. `PayrollResults` ne contient des données que pour **Year = 2026** (aucune variance). Impossible aujourd'hui de construire des KPI d'évolution paie sur plusieurs exercices — à surveiller lors des prochains exports.

---

## 1. Grille de lecture utilisée pour classer chaque table

- **Dimension potentielle** : peu de lignes relatives, forte proportion de colonnes descriptives/textuelles, référencée par des FK entrantes.
- **Table de faits potentielle** : volumétrie qui croît avec l'activité (mensuelle/événementielle), colonnes numériques/mesures, clé étrangère vers Employee/Company/date.
- **Table de référence** : très peu de lignes (< 15), contenu figé de type code/libellé.
- **Table de liaison** : deux FK ou plus formant une association N-N, peu ou pas de colonnes métier propres.
- **Table historique** : contient `OldValue/NewValue`, `Action`, horodatage d'événement — journal d'audit applicatif.
- **Table technique** : infrastructure EF Core, moteur de règles non peuplé, préférences de notification/canal, config UI.
- **Table à exclure** : vide (0 ligne), doublon fonctionnel d'une autre table, ou hors périmètre RH/Paie (notifications génériques, sécurité applicative).

---

## 2. Analyse par domaine fonctionnel

### 2.1 Sécurité applicative / Système (hors périmètre BI RH-Paie)

| Table | Catégorie | Rôle métier | Utilité BI | Décision | Justification |
|---|---|---|---|---|---|
| Users | 3. Référence | Compte de connexion lié à un Employee (FK `Users.EmployeeId → Employees.Id`) | Faible pour KPI RH ; utile seulement comme dimension technique "auteur d'action" dans les logs | Exclure du DW métier | 28 lignes, 12 FK entrantes (le plus référencé après Companies/Employees) mais aucune colonne RH ; sert uniquement d'acteur applicatif |
| Roles | 3. Référence | Rôles applicatifs (RH, Manager, Admin...) | Nulle pour KPI RH/Paie | Exclure | 9 lignes, table de sécurité pure |
| Permissions | 3. Référence | Permissions unitaires | Nulle | Exclure | 473 lignes de contrôle d'accès, aucun rapport avec la paie/RH |
| RolesPermissions | 4. Liaison | Association Role↔Permission | Nulle | Exclure | Table pivot de sécurité |
| UsersRoles | 4. Liaison | Association User↔Role | Nulle | Exclure | Idem |
| Invitations | 6. Technique | Invitations à rejoindre une société | Nulle | Exclure | 0 ligne, feature d'onboarding compte, non peuplée |
| __EFMigrationsHistory | 6. Technique | Historique des migrations EF Core | Nulle | Exclure | Table d'infrastructure, jamais métier |
| sysdiagrams | 6. Technique | Diagrammes SQL Server Management Studio | Nulle | Exclure | 0 ligne, artefact d'outillage SSMS |
| SystemConstants | 3. Référence | Constantes système versionnées (`Key`, `Value`, `EffectiveDate`) | Faible, potentiellement utile pour documenter un paramètre de calcul (ex. SMIG) dans un tooltip Power BI | Analyse complémentaire | 32 lignes ; à vérifier si les clés stockées sont des paramètres légaux de paie (auquel cas dimension de référence utile) |

### 2.2 Référentiels géographiques / civils / légaux

| Table | Catégorie | Rôle métier | Utilité BI | Décision | Justification |
|---|---|---|---|---|---|
| Countries | 3. Référence | Pays | Faible directement | Inclure (secondaire) | 2 lignes seulement ; `Employees.CountryId` est à 100 % NULL → dimension quasi inutilisée côté Employee, mais utilisée par `Cities`, `Companies`, `Holidays` |
| Cities | 3. Référence | Villes | Moyenne (répartition géographique des sociétés/adresses) | Inclure (secondaire) | 37 lignes, FK entrante de `Companies`, `EmployeeAddresses` |
| Nationalities | 3. Référence | Nationalités | Faible-moyenne | Inclure (secondaire) | 5 lignes, référencée par `Employees.NationalityId` (taux de remplissage correct, non listé en fort taux de NULL) |
| Genders | 1. Dimension | Genre | Utile pour KPI diversité H/F | Inclure | 2 lignes, colonne bien remplie sur `Employees` |
| MaritalStatuses | 3. Référence | Statut marital | Faible | Analyse complémentaire | `Employees.MaritalStatusId` est à 85,27 % NULL → dimension présente mais quasi inexploitable en l'état |
| EducationLevels | 3. Référence | Niveau de diplôme | Faible-moyenne | Inclure (secondaire) | 11 lignes, à croiser avec le taux de remplissage réel sur `Employees.EducationLevelId` (non signalé en fort NULL, donc a priori correct) |
| Statuses | 1. Dimension | Statut de l'employé (actif/inactif...) avec impacts métier explicites (`AffectsPayroll`, `AffectsAttendance`, `AffectsAccess`) | **Forte** — axe central pour tout KPI d'effectif actif | Inclure (prioritaire) | 5 lignes, colonnes booléennes directement exploitables pour filtrer les KPI RH |
| BusinessSectors | 3. Référence | Secteur d'activité | Nulle actuellement | Exclure / à surveiller | 0 ligne malgré 1 FK entrante (`SalaryPackages`, elle-même vide) |
| StateEmploymentPrograms | 3. Référence | Programmes d'aide à l'emploi (exonérations) | Faible | Inclure (secondaire) | 3 lignes, liée à `ContractTypes`, utile pour un KPI d'exonérations légales si besoin réglementaire |
| LegalContractTypes | 3. Référence | Typologie légale des contrats (CDI/CDD...) | Moyenne | Inclure (secondaire) | 5 lignes, référencée par `ContractTypes` |
| IrTaxBrackets | 3. Référence | Barème de l'impôt sur le revenu | **Forte** pour la paie | Inclure | 6 lignes, alimente directement le calcul IR visible dans `PayrollResults.ImpotRevenu/IrTaux` |
| LegalParameters | 6. Technique | Paramètres légaux généraux | Nulle actuellement | Exclure / à surveiller | 0 ligne — table prévue mais non peuplée |

### 2.3 Entreprise

| Table | Catégorie | Rôle métier | Utilité BI | Décision | Justification |
|---|---|---|---|---|---|
| Companies | 1. Dimension | Société cliente (multi-tenant), avec auto-référence `ManagedByCompanyId` (cabinet comptable gérant plusieurs sociétés) | **Forte** — axe d'analyse transverse à tout le dashboard | Inclure (prioritaire) | 43 lignes, table la plus référencée (29 FK entrantes), nombreuses colonnes légales (ICE, RC, CNSS) à forte volumétrie de NULL sur champs secondaires (signatoire, site web) mais champs clés (nom, devise, périodicité paie) exploitables |
| CompanyDocuments | 6. Technique | Documents administratifs liés à la société | Nulle | Exclure | 3 lignes, gestion documentaire, pas un KPI RH |
| CompanyEventLogs | 5. Historique | Journal des changements sur la société | Faible | Exclure du DW étoile | 46 lignes, log d'audit (`oldValue/newValue`), utile en support mais pas en fait/dimension |
| BankAccounts | 1. Dimension (à surveiller) | Comptes bancaires (société/employé) | Faible actuellement | Analyse complémentaire | 7 lignes ; les tables qui l'exploitent (`EmployeeBankAccountAllocations`, `PayrollBankPayments`) sont vides et `Employees.BankAccountId` est à 100 % NULL → le paiement bancaire n'est pas encore tracé dans les données |
| WorkingCalendars | 1. Dimension | Calendrier de travail par société (jour de semaine travaillé, horaires) | Moyenne-forte | Inclure | 325 lignes, nécessaire pour tout calcul de jours ouvrés (congés, absences, heures) |
| Holidays | 1. Dimension | Jours fériés/chômés | Moyenne-forte | Inclure | 34 lignes, alimente les calculs de congés/paie (`AffectPayroll`, `AffectAttendance`) |

### 2.4 RH — Employés, organisation, contrats

| Table | Catégorie | Rôle métier | Utilité BI | Décision | Justification |
|---|---|---|---|---|---|
| Employees | 1. Dimension | Fiche employé complète | **Très forte** — dimension centrale du dashboard | Inclure (prioritaire) | 1561 lignes, 44 colonnes, 24 FK entrantes. Attention qualité : `ManagerId`, `CountryId`, `BankAccountId`, `PersonalEmail` à 100 % NULL ; `DepartementId` à 90,52 % NULL (!) ; `CategoryId` à 99,74 % NULL ; `MaritalStatusId` à 85,27 % NULL. **La hiérarchie managériale et le rattachement département sont donc quasiment inexploitables tels quels** — à fiabiliser avant de construire les KPI "effectif par département" |
| Departements | 1. Dimension | Département/service, hiérarchique (`ParentDepartementId`) et avec un manager (`ManagerId`) | Forte en théorie, limitée en pratique | Inclure, avec réserve | 585 lignes mais `Employees.DepartementId` NULL à 90,52 % → très peu d'employés sont effectivement rattachés à un département dans les données actuelles |
| JobPositions | 1. Dimension | Intitulé de poste, rattaché à la société | Moyenne-forte | Inclure | 387 lignes ; **attention** : `JobPositionId` n'existe pas dans `Employees`, il n'existe que dans `EmployeeContracts` — le poste d'un employé se déduit donc de son contrat (actif ou historique), pas d'un lien direct |
| EmployeeCategories | 3. Référence | Catégorie de paie de l'employé (mode, périodicité) | Faible actuellement | Analyse complémentaire | 86 lignes, mais `Employees.CategoryId` NULL à 99,74 % → quasi inexploitable |
| EmployeeAddresses | 1. Dimension (attribut) | Adresse postale de l'employé | Moyenne | Inclure (secondaire) | 1213 lignes, bon taux de remplissage sur `AddressLine1`/`ZipCode`/`CityId`, utile pour une répartition géographique des effectifs |
| EmployeeChildren | 3. Référence | Enfants à charge | Faible | Exclure / à surveiller | 2 lignes seulement — volumétrie insuffisante pour un KPI famille/allocations |
| EmployeeSpouses | 3. Référence | Conjoint | Nulle actuellement | Exclure | 0 ligne |
| EmployeeDocuments | 6. Technique | Pièces jointes employé | Nulle | Exclure | 0 ligne, gestion documentaire |
| EmployeeEventLogs | 5. Historique | Journal des changements sur la fiche employé | Faible pour un fait, utile en source d'événements RH | Exclure du DW étoile / analyse complémentaire | 921 lignes, log générique (`eventName`, `oldValue/newValue`) ; pourrait alimenter un futur "fait événement RH" (promotion, mutation) si le contenu de `eventName` est exploitable — à vérifier avec un échantillon de valeurs |
| ContractTypes | 3. Référence | Types de contrat propres à la société (CDI, CDD, Intérim...) | Moyenne | Inclure (secondaire) | 174 lignes, relié à `LegalContractTypes` et `StateEmploymentPrograms` |
| EmployeeContracts | 1. Dimension historisée (SCD2) | Historique des contrats (poste, type de contrat, dates) | **Forte** | Inclure (prioritaire) | 1337 lignes ; `EndDate` NULL à 80,78 % (contrats toujours actifs = cohérent) ; c'est la seule table donnant accès au poste réel de l'employé dans le temps |

### 2.5 Paie / Rémunération

| Table | Catégorie | Rôle métier | Utilité BI | Décision | Justification |
|---|---|---|---|---|---|
| EmployeeSalaries | 1. Dimension historisée (SCD2) | Historique du salaire de base | **Forte** | Inclure (prioritaire) | 1225 lignes, `EndDate` NULL à 87,10 % (= salaire courant) ; alimente l'évolution salariale dans le temps |
| EmployeeSalaryComponents | 2. Fait (détail) | Composants de rémunération (primes, indemnités) rattachés à un salaire | Moyenne-forte | Inclure (secondaire) | 1112 lignes ; `Percentage`/`EndDate` très souvent NULL (>99 %, cohérent avec des composants en montant fixe et actifs) |
| SalaryPackages | 1. Dimension | Modèle de package salarial (template) | Nulle actuellement | Exclure / à surveiller | 0 ligne |
| SalaryPackageItems | 4. Liaison | Détail des éléments d'un package | Nulle | Exclure | 0 ligne |
| SalaryPackageAssignments | 4. Liaison | Affectation d'un package à un employé/contrat | Nulle | Exclure | 0 ligne |
| PayComponents | 3. Référence | Catalogue des éléments de paie (code, type, imposabilité) | Nulle actuellement malgré son rôle structurant | Exclure / à surveiller | 0 ligne — catalogue non peuplé, alors que `PayrollResults` contient déjà les montants calculés à plat |
| **PayrollResults** | **2. Fait principal** | Bulletin de paie calculé par employé/mois/année (77 colonnes de montants : brut, cotisations, IR, net) | **Très forte — fait central du dashboard Paie** | Inclure (prioritaire) | 870 lignes, granularité Employee × Month × Year, uniquement Year=2026 actuellement ; colonnes techniques (`ClaudeModel`, `TokensUsed`, `ResultatJson`, `ErrorMessage`) à exclure du modèle en étoile |
| PayrollResultPrimes | 2. Fait (détail) | Détail des primes composant un bulletin | Moyenne | Inclure (secondaire) | 399 lignes, bridge label/montant/ordre rattaché à `PayrollResults` |
| PayrollTaxSnapshots | 2. Fait (cumulatif) | Cumuls fiscaux/sociaux (CNSS, AMO, IR) par bulletin | Moyenne-forte | Inclure (secondaire) | 581 lignes, utile pour un KPI de cumuls annuels (taux d'imposition effectif) |
| PayrollBankPayments | 2. Fait | Détail des virements bancaires du bulletin | Nulle actuellement | Exclure / à surveiller | 0 ligne |
| PayrollCalculationAuditSteps | 5. Historique/Technique | Journal détaillé (pas à pas, JSON) du moteur de calcul de paie | Nulle pour le BI | Exclure | 9412 lignes — volume important mais 100 % technique (JSON d'entrée/sortie de formules), destiné au debug du moteur, pas à l'analyse RH |
| PayrollCustomRules | 6. Technique | Règles de paie personnalisées (DSL) | Nulle actuellement | Exclure | 0 ligne |
| EmployeeBankAccountAllocations | 4. Liaison | Répartition du salaire entre comptes bancaires | Nulle actuellement | Exclure / à surveiller | 0 ligne |
| AncienneteRateSets / AncienneteRates | 3. Référence | Barème de prime d'ancienneté | Nulle actuellement | Exclure / à surveiller | 0 ligne chacune, alors que `PayrollResults` contient déjà `PrimeAnciennete` calculée — le barème source n'est pas peuplé mais le résultat l'est |
| ElementRules, RuleCaps, RuleDualCaps, RuleFormulas, RulePercentages, RuleTiers, RuleVariants | 6. Technique | Moteur de règles de calcul paramétrable (plafonds, formules, paliers) | Nulle | Exclure | 0 ligne pour les 7 tables — moteur de règles non peuplé au moment du profilage |
| ReferentielElements, ElementCategories, EligibilityCriteria, Authorities | 3. Référence (technique) | Référentiel légal des éléments de paie et autorités de tutelle | Nulle | Exclure / à surveiller | 0 ligne pour les 4 tables |
| CnssPreetabliImports / CnssPreetabliLines | 2. Fait (déclaratif) | Import des fichiers préétablis CNSS | Nulle actuellement | Exclure / à surveiller | 0 ligne chacune — fonctionnalité de conformité sociale non encore utilisée |

### 2.6 Congés / Absences

| Table | Catégorie | Rôle métier | Utilité BI | Décision | Justification |
|---|---|---|---|---|---|
| LeaveTypes | 1. Dimension | Types de congé (par société) | Moyenne | Inclure | 7 lignes |
| LeaveTypePolicies | 3. Référence | Politique associée à un type de congé (solde négatif autorisé, etc.) | Faible-moyenne | Inclure (secondaire) | 6 lignes |
| LeaveTypeLegalRules | 3. Référence | Règles légales par cas (jours accordés, article de loi) | Faible-moyenne | Inclure (secondaire) | 10 lignes |
| **LeaveRequests** | **2. Fait** | Demande de congé (statut, dates, jours décomptés) | Forte en théorie | Inclure, à surveiller | **Seulement 2 lignes** — table structurellement centrale mais quasiment vide ; ne permet aujourd'hui aucun KPI fiable de "taux de congés/refus/délai d'approbation" |
| LeaveRequestApprovalHistories | 5. Historique | Historique des décisions sur une demande | Faible | Exclure du DW étoile | 2 lignes, log d'audit |
| LeaveRequestAttachments | 6. Technique | Pièces jointes de la demande | Nulle | Exclure | 0 ligne |
| LeaveRequestExemptions | 3. Référence | Exemptions de jours dans le décompte (jours fériés inclus dans le congé) | Nulle actuellement | Exclure / à surveiller | 0 ligne |
| **LeaveBalances** | **2. Fait (snapshot périodique)** | Solde de congés par employé/type/mois/année | Forte | Inclure (prioritaire) | 109 lignes, période 2019–2026, bon candidat de fait pour le KPI "solde de congés" |
| LeaveCarryOverAgreements | 3. Référence | Accords de report de solde entre années | Nulle actuellement | Exclure / à surveiller | 0 ligne |
| LeaveAuditLogs | 5. Historique | Journal des changements sur les congés | Faible | Exclure | 7 lignes, log d'audit |
| **EmployeeAbsences** | **2. Fait** | Absence individuelle (date, type, décision) | Forte en théorie | Inclure, à surveiller | **Seulement 2 lignes** — même constat que `LeaveRequests` : structure prête, données quasi absentes |
| **EmployeeMonthlyAbsenceSummaries** | **2. Fait pré-agrégé** | Cumul mensuel de jours d'absence par employé | **Forte** | Inclure (prioritaire) | 150 lignes, période 2026 (jan-juil), directement exploitable pour un KPI de taux d'absentéisme sans recalcul |
| **EmployeeMonthlyLeaveSummaries** | **2. Fait pré-agrégé** | Cumul mensuel de jours de congé par employé | **Forte** | Inclure (prioritaire) | 150 lignes, même logique, exploitable directement |

### 2.7 Présence / Temps de travail

| Table | Catégorie | Rôle métier | Utilité BI | Décision | Justification |
|---|---|---|---|---|---|
| EmployeeAttendances | 2. Fait | Pointage journalier (check-in/out, heures travaillées) | Nulle actuellement malgré son rôle central annoncé dans le besoin métier | Exclure / à surveiller en priorité | **0 ligne** — la fonctionnalité de pointage n'est pas utilisée dans cet extrait, alors que le besoin métier mentionne explicitement "Absences" ; à reconfirmer avec l'équipe produit avant d'investir dans ce flux |
| EmployeeAttendanceBreaks | 2. Fait (détail) | Pauses dans un pointage | Nulle | Exclure | 0 ligne, dépend d'`EmployeeAttendances` (vide) |
| EmployeeOvertimes | 2. Fait | Heures supplémentaires déclarées | Nulle actuellement | Exclure / à surveiller | 0 ligne malgré une structure très riche (34 colonnes, workflow d'approbation complet) |
| OvertimeRateRules | 3. Référence | Barème de majoration des heures sup | Faible actuellement | Inclure (secondaire) / à surveiller | 6 lignes — le barème existe mais rien ne l'utilise encore (`EmployeeOvertimes` vide) |
| EmployeeMonthlyWorkedDaysSummaries | 2. Fait pré-agrégé | Cumul mensuel de jours travaillés | Faible-moyenne | Inclure (secondaire) | 26 lignes seulement, volumétrie faible mais structure directement exploitable |
| EmployeeMonthlyOvertimeSummaries | 2. Fait pré-agrégé | Cumul mensuel d'heures sup | Très faible | Exclure / à surveiller | 4 lignes — volumétrie insuffisante pour tout KPI fiable |

### 2.8 Alertes

| Table | Catégorie | Rôle métier | Utilité BI | Décision | Justification |
|---|---|---|---|---|---|
| AlertCategories | 3. Référence | Catégories d'alerte | Moyenne | Inclure | 6 lignes |
| AlertSeverities | 3. Référence | Niveaux de sévérité | Moyenne | Inclure | 3 lignes |
| AlertDefinitions | 1. Dimension | Catalogue des règles d'alerte (seuils, canaux) | Moyenne-forte | Inclure | 12 lignes ; plusieurs champs (Description, SourceField) à 100 % NULL — catalogue partiellement documenté |
| **Alerts** | **2. Fait** | Instance d'alerte déclenchée, liée à une entité polymorphe (`EntityType`/`EntityId`) | **Forte pour le module "Alertes" demandé** | Inclure (prioritaire) | 64 lignes ; exploitable pour un dashboard d'alertes RH/Paie si le volume augmente en production |
| AlertDeliveries | 5. Historique/Technique | Envoi effectif d'une alerte (canal, statut) | Nulle | Exclure | 0 ligne |
| AlertAuditLogs | 5. Historique | Journal des modifications d'alertes | Faible | Exclure | 234 lignes, mais colonnes métier (`OldValueJson`, `IpAddress`...) à 100 % NULL |
| AlertRecipientRoles | 4. Liaison | Rôles destinataires d'une alerte | Nulle pour le BI | Exclure | 24 lignes, config technique |
| AlertRuleExecutionLogs | 6. Technique | Journal d'exécution du moteur d'alertes (monitoring) | Nulle | Exclure | 4719 lignes — volumétrie élevée mais 100 % opérationnel/technique |
| AlertUserPreferences / AlertUserPreferenceAudits / CompanyAlertPreferences | 6. Technique | Préférences de notification d'alerte | Nulle | Exclure | 0 ligne chacune |

### 2.9 Notifications (hors périmètre RH/Paie)

| Table | Catégorie | Rôle métier | Utilité BI | Décision | Justification |
|---|---|---|---|---|---|
| NotificationCategories, NotificationDefinitions, Notifications, NotificationDeliveries, NotificationAuditLogs, NotificationUserPreferences, CompanyNotificationPreferences | 6. Technique | Système de notification applicatif générique (in-app/email/SMS) | Nulle pour un dashboard RH/Paie | Exclure toutes | Fonctionnalité plateforme transverse, aucune colonne métier RH/Paie ; `Notifications` a 67 lignes mais son contenu est un message applicatif, pas une donnée d'analyse |

---

## 3. Synthèse finale

### 3.1 Tables les plus importantes pour le Data Warehouse (à charger en priorité)

**Dimensions**
- `Employees` (avec réserve forte sur `DepartementId`/`ManagerId`/`CategoryId` très incomplets)
- `Companies`
- `Departements`
- `JobPositions` (via `EmployeeContracts`, pas de lien direct sur `Employees`)
- `EmployeeContracts` (dimension historisée SCD2 — clé pour poste/type de contrat dans le temps)
- `EmployeeSalaries` (dimension historisée SCD2 — salaire de base dans le temps)
- `ContractTypes`, `LegalContractTypes`
- `Statuses` (axe filtrant essentiel : actif, impact paie/pointage)
- `LeaveTypes`
- `Holidays`, `WorkingCalendars`
- `IrTaxBrackets`
- `AlertDefinitions`, `AlertCategories`, `AlertSeverities`

**Faits**
- `PayrollResults` — fait central Paie (870 lignes, 2026)
- `PayrollResultPrimes`, `PayrollTaxSnapshots` — détail/cumuls paie
- `EmployeeMonthlyAbsenceSummaries` — fait Absentéisme (déjà agrégé, prêt à l'emploi)
- `EmployeeMonthlyLeaveSummaries` — fait Congés (déjà agrégé, prêt à l'emploi)
- `LeaveBalances` — fait Solde de congés (2019–2026)
- `EmployeeSalaryComponents` — détail rémunération
- `Alerts` — fait Alertes

### 3.2 Tables probablement inutiles pour ce Data Warehouse

- Toute la sécurité applicative : `Users`, `Roles`, `Permissions`, `RolesPermissions`, `UsersRoles`, `Invitations`, `__EFMigrationsHistory`, `sysdiagrams`.
- Tout le module Notifications (7 tables) : hors périmètre RH/Paie, contenu applicatif pur.
- Tous les journaux d'audit techniques : `*AuditLogs`, `*EventLogs`, `PayrollCalculationAuditSteps`, `AlertRuleExecutionLogs` — volumineux mais sans valeur analytique RH directe (utiles seulement pour le support technique).
- Toutes les tables à **0 ligne** (44 tables) : à exclure du premier chargement par construction — il n'y a rien à charger. Cela concerne notamment tout le moteur de règles de paie paramétrable (`ElementRules`, `RuleCaps`, `RuleFormulas`, `RuleTiers`, `RuleVariants`, `RuleDualCaps`, `RulePercentages`), les packages salariaux, le pointage (`EmployeeAttendances*`), les heures supplémentaires (`EmployeeOvertimes`), et la conformité CNSS déclarative.

### 3.3 Tables nécessitant une analyse supplémentaire avant décision

| Table | Pourquoi une décision définitive n'est pas possible aujourd'hui |
|---|---|
| `LeaveRequests`, `EmployeeAbsences` | Structurellement centrales pour le besoin "Congés/Absences", mais seulement 2 lignes chacune : impossible de juger de la qualité réelle des données en production. À revalider sur un extrait plus large ou en environnement de production. |
| `EmployeeAttendances`, `EmployeeOvertimes` et leurs tables filles | 0 ligne alors que le besoin métier mentionne "Absences" et potentiellement le temps de travail. À confirmer avec l'équipe produit : la fonctionnalité de pointage est-elle utilisée en production, ou est-ce un module non activé sur ce tenant ? |
| `Employees.DepartementId` / `.ManagerId` / `.CategoryId` | Taux de NULL de 85 à 100 % sur des colonnes qui seraient normalement structurantes pour un dashboard RH (organigramme, catégorie de paie). À vérifier si ces informations existent ailleurs (peut-être portées par `EmployeeContracts` plutôt que par `Employees`) avant de les exclure définitivement. |
| `EmployeeEventLogs` / `CompanyEventLogs` | Logs génériques (`eventName`, `oldValue`/`newValue`) qui pourraient, si le contenu de `eventName` est structuré (ex. "PROMOTION", "MUTATION", "SALARY_CHANGE"), constituer une source intéressante pour un futur fait "événements de carrière". Nécessite un échantillon des valeurs distinctes de `eventName`, non fourni dans le profilage actuel. |
| `SystemConstants` | À vérifier si les `Key`/`Value` stockés sont des paramètres légaux de paie (SMIG, plafond CNSS...) auquel cas ils mériteraient une réutilisation en dimension de référence, sinon pure config technique. |
| `PayrollResults` — unicité Employee×Month×Year | Le fichier doublons ne teste probablement que la clé technique `Id`. À vérifier explicitement qu'il n'existe pas deux bulletins pour le même employé/mois/année (grain attendu du fait). |

### 3.4 Informations manquantes pour fiabiliser la décision

1. **Fichier de profilage des clés primaires** annoncé dans votre message mais non transmis (seuls les FK sont disponibles) — nécessaire pour confirmer que `Id` est bien PK partout et repérer d'éventuelles clés composites.
2. **Profilage des doublons sur des combinaisons de colonnes métier** (pas seulement sur `Id`) — en particulier pour `PayrollResults` (Employee+Month+Year), `LeaveBalances` (Employee+LeaveType+Year+Month), `EmployeeMonthlyAbsenceSummaries`/`LeaveSummaries` (Employee+Year+Month).
3. **Échantillon de valeurs distinctes** pour les colonnes `Status`/`Action`/`eventName`/`AbsenceType`/`DurationType` (actuellement typées `IntegerType()` ou `StringType()` sans dictionnaire de valeurs) — indispensable pour savoir si ces codes sont directement lisibles en BI ou nécessitent une table de correspondance.
4. **Confirmation métier** sur le périmètre réellement actif en production pour `EmployeeAttendances`, `EmployeeOvertimes`, `SalaryPackages` et le moteur de règles (`Rule*`) : s'agit-il de fonctionnalités non lancées, de modules optionnels non activés sur ce tenant, ou d'un environnement de test peu représentatif de la production réelle ?
5. **Fenêtre temporelle réelle des données** : `PayrollResults` ne couvre que 2026 dans cet extrait — à confirmer si d'autres exercices existent en base pour permettre des KPI d'évolution pluriannuelle.

---

*Aucun code (PySpark, SQL, DDL) n'a été généré à ce stade, conformément à la demande. Cette analyse sert de base à la définition du modèle en étoile (prochaine étape).*
