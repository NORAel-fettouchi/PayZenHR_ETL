# PayZenHR — Liste finale des tables retenues pour le Data Warehouse

Cette classification s'appuie sur deux sources : le profilage réel des données (volumétrie, NULL, FK, doublons) et le schéma applicatif `DATABASE_SCHEMA_PAR_MODULE.md` (cascades, contraintes d'unicité, colonnes shadow, relations polymorphes). Le schéma applicatif confirme et complète trois points utilisés ci-dessous :

- **Contraintes d'unicité confirmées** (absentes du profilage mais présentes dans le code) : `LeaveBalances` unique sur (EmployeeId, LeaveTypeId, Year, Month) ; `EmployeeMonthlyAbsenceSummaries`/`LeaveSummaries`/`OvertimeSummaries`/`WorkedDaysSummaries` uniques sur (EmployeeId, Year, Month) ; `EmployeeBankAccountAllocations` unique sur (EmployeeId, BankAccountId). Cela **confirme le grain** des tables de faits pré-agrégées du Groupe A.
- **Comportements de suppression** (`SetNull` sur la majorité des FK démographiques d'`Employees`) : explique techniquement pourquoi `DepartementId`, `ManagerId`, `CategoryId`, `MaritalStatusId` peuvent être NULL même sur un employé actif (suppression d'une valeur de référence → mise à NULL automatique), ce qui nuance le diagnostic qualité posé précédemment sans l'annuler.
- **Références polymorphes sans FK réelle** : `Alerts.EntityType/EntityId` et `Notifications.SourceType/SourceId` ne sont pas des FK SQL, ce qui limite la possibilité de les rattacher proprement à des dimensions dans un modèle en étoile classique.

Le classement ci-dessous vise exclusivement le **dashboard Power BI RH/Paie** (Effectifs, Départements, Postes, Congés, Absences, Paie, Alertes). Aucune table n'est conservée par défaut : chaque présence est justifiée par un besoin d'analyse réel et une donnée réellement exploitable.

---

## GROUPE A — Tables de faits potentielles

| # | Table | Événement métier | Grain potentiel | Mesures disponibles | Dimensions liées | Période disponible | KPI possibles |
|---|---|---|---|---|---|---|---|
| 1 | **PayrollResults** | Bulletin de paie calculé | 1 ligne = 1 employé × 1 mois × 1 année (à vérifier : pas de contrainte d'unicité confirmée dans le schéma applicatif contrairement aux tables de synthèse) | `SalaireBase`, `TotalBrut`, `TotalNet`/`NetAPayer`, `ImpotRevenu`, `CnssPartSalariale/Patronale`, `CimrPartSalariale/Patronale`, `AmoPartSalariale/Patronale`, `HeuresSupp25/50/100`, `Conges`, `JoursFeries`, `PrimeAnciennete`, et ~50 autres rubriques de paie | DimEmployee, DimCompany, DimDate (Month/Year) | 2026 uniquement (12 mois observés dans l'extrait) | Masse salariale totale, coût employeur (charges patronales), net moyen par employé/département, poids de l'IR, évolution mensuelle du brut/net, taux de charges sociales |
| 2 | **PayrollResultPrimes** | Ligne de prime versée dans un bulletin | 1 ligne = 1 prime × 1 bulletin | `Montant`, `Ordre`, `IsTaxable` | PayrollResults (parent), DimEmployee (via parent) | Idem PayrollResults | Répartition des primes par type, part des primes imposables vs non imposables dans la rémunération |
| 3 | **PayrollTaxSnapshots** | Cumuls fiscaux/sociaux à date d'un bulletin | 1 ligne = 1 bulletin (relation 1:1 avec PayrollResults, confirmée par le schéma applicatif) | `CumulBrut`, `CumulCnss`, `CumulAmo`, `CumulIr`, `CumulNet`, `TauxEffectif`, `CumulFp` | PayrollResults (1:1), DimEmployee, DimDate | Idem PayrollResults | Taux d'imposition effectif cumulé, suivi des plafonds CNSS/AMO sur l'année |
| 4 | **EmployeeMonthlyAbsenceSummaries** | Cumul mensuel des jours d'absence | 1 ligne = 1 employé × 1 mois × 1 année (**unicité confirmée** par le schéma applicatif) | `AbsenceDays` | DimEmployee, DimDate | 2026, jan.–juil. (150 lignes) | Taux d'absentéisme par employé/département/mois, tendance mensuelle, employés à absentéisme élevé |
| 5 | **EmployeeMonthlyLeaveSummaries** | Cumul mensuel des jours de congé pris | 1 ligne = 1 employé × 1 mois × 1 année (**unicité confirmée**) | jours de congé cumulés | DimEmployee, DimDate, DimLeaveType (si le détail par type existe dans les colonnes réelles, à vérifier) | 2026, jan.–juil. (150 lignes) | Taux de prise de congés, saisonnalité des départs en congé, comparaison quota vs consommé |
| 6 | **LeaveBalances** | Solde de congés d'un employé pour une période | 1 ligne = 1 employé × 1 type de congé × 1 mois × 1 année (**unicité confirmée**) | `OpeningDays`, `AccruedDays`, `UsedDays`, `CarryInDays`, `CarryOutDays`, `ClosingDays` | DimEmployee, DimLeaveType, DimCompany, DimDate | 2019–2026 (109 lignes, la plus longue profondeur historique disponible) | Solde de congé restant par employé/type, risque de perte de solde (`CarryoverExpiresOn`), taux d'utilisation du quota annuel |
| 7 | **EmployeeSalaryComponents** | Composant de rémunération (prime, indemnité) rattaché à un salaire | 1 ligne = 1 composant × 1 salaire | `Amount`, `Percentage`, `IsTaxable`, `IsSocial`, `IsCIMR` | EmployeeSalaries (parent), DimEmployee (via parent) | Historique complet des salaires (voir EmployeeSalaries) | Structure de la rémunération (fixe vs variable), part des composants imposables/sociaux |
| 8 | **Alerts** | Alerte générée par le moteur de règles (seuil dépassé, anomalie, échéance) | 1 ligne = 1 occurrence d'alerte | `Status`, `IsAcknowledged`, dates de résolution | DimCompany, DimAlertDefinition, référence polymorphe `EntityType/EntityId` (non-FK, à traiter en attribut texte, pas en jointure garantie) | Non précisée dans le profilage (34 lignes) | Nombre d'alertes actives par sévérité/catégorie, délai moyen de traitement, taux de résolution |
| 9 | **LeaveRequests** | Demande de congé individuelle | 1 ligne = 1 demande | `CalendarDays`, `WorkingDaysDeducted`, `Status` | DimEmployee, DimCompany, DimLeaveType, DimDate | Volumétrie actuelle insuffisante (2 lignes) — **à réévaluer sur un extrait de production** | Taux d'approbation/refus, délai moyen de traitement, volume de demandes par type/période |
| 10 | **EmployeeAbsences** | Absence individuelle constatée | 1 ligne = 1 absence | `DurationType`, `Status` | DimEmployee, DimDate | Volumétrie actuelle insuffisante (2 lignes) — **à réévaluer sur un extrait de production** | Fréquence d'absence par employé/motif, taux d'absentéisme non planifié |
| 11 | **EmployeeMonthlyWorkedDaysSummaries** | Cumul mensuel de jours travaillés | 1 ligne = 1 employé × 1 mois × 1 année (**unicité confirmée**) | `WorkedDays` | DimEmployee, DimDate | 26 lignes | Taux de présence, jours travaillés par employé/mois |
| 12 | **EmployeeMonthlyOvertimeSummaries** | Cumul mensuel d'heures supplémentaires | 1 ligne = 1 employé × 1 mois × 1 année (**unicité confirmée**) | heures sup cumulées | DimEmployee, DimDate | 4 lignes seulement | Volume d'heures sup par employé/mois — **volumétrie trop faible pour un KPI fiable aujourd'hui** |

> **Note de conception** : les tables 9, 10 et 12 sont structurellement des faits mais leur volumétrie actuelle (2 à 4 lignes) ne permet aucune validation de qualité ni de restitution BI crédible. Elles sont conservées dans le Groupe A parce que leur grain et leurs mesures sont clairs et qu'elles répondent à un besoin explicite du dashboard (Congés/Absences), mais leur chargement doit être conditionné à une nouvelle extraction sur un périmètre de production plus représentatif.

---

## GROUPE B — Dimensions potentielles

| Nom | Clé primaire | Clé métier éventuelle | Attributs utiles pour l'analyse | Tables de faits liées |
|---|---|---|---|---|
| **DimEmployee** (← Employees) | `Id` (PK supposée — non confirmée par un fichier PK dédié) | `Matricule` | Nom/Prénom, DateOfBirth (→ âge/tranche d'âge), StatusId, GenderId, CompanyId, DepartementId (qualité faible, cf. Groupe D notes), CategoryId (qualité faible) | PayrollResults, PayrollTaxSnapshots, LeaveBalances, LeaveRequests, EmployeeAbsences, EmployeeMonthly*Summaries, Alerts (indirect) |
| **DimCompany** (← Companies) | `Id` | `IceNumber` (identifiant légal marocain), `CnssNumber` | CompanyName, Currency, PayrollPeriodicity, BusinessSector (texte libre), auto-référence `ManagedByCompanyId` (structure cabinet/multi-société) | Toutes les tables de faits (axe transverse multi-tenant) |
| **DimDepartement** (← Departements) | `Id` | Aucune (nom non garanti unique) | DepartementName, HierarchyLevel, ParentDepartementId (hiérarchie), ManagerId | PayrollResults (via Employee), EmployeeMonthly*Summaries (via Employee) — **jointure indirecte uniquement, cf. réserve qualité** |
| **DimJobPosition** (← JobPositions) | `Id` | Aucune | Name, CompanyId | Aucun fait direct — accessible uniquement via `EmployeeContracts` (pas de FK JobPosition sur Employees ni sur PayrollResults) |
| **DimContractType** (← ContractTypes) | `Id` | Aucune | ContractTypeName, rattachements `LegalContractTypeId`/`StateEmploymentProgramId` (à dénormaliser, cf. Groupe C) | EmployeeContracts |
| **DimEmployeeContract** (← EmployeeContracts) | `Id` | Aucune | JobPositionId, ContractTypeId, StartDate/EndDate (SCD2 — contrat en cours si EndDate NULL) | Aucun fait direct ; sert de dimension historisée pour retrouver le poste/type de contrat *à la date* d'un bulletin de paie |
| **DimEmployeeSalary** (← EmployeeSalaries) | `Id` | Aucune | BaseSalary, ContractId, EffectiveDate/EndDate (SCD2) | Aucun fait direct ; sert à retracer l'évolution du salaire de base dans le temps |
| **DimStatus** (← Statuses) | `Id` | `Code` | NameFr/Ar/En, `AffectsPayroll`, `AffectsAttendance`, `AffectsAccess` (flags directement filtrables) | Employees → tous les faits liés à Employee |
| **DimLeaveType** (← LeaveTypes) | `Id` | Aucune | Nom, CompanyId (type de congé propre à une société) | LeaveBalances, LeaveRequests, EmployeeMonthlyLeaveSummaries (si détaillé par type) |
| **DimHoliday** (← Holidays) | `Id` | Aucune | HolidayDate, IsMandatory, IsPaid, AffectPayroll, AffectAttendance | Utilisée en filtre calendaire (pas de FK entrante directe depuis un fait, mais indispensable pour calculer des jours ouvrés) |
| **DimWorkingCalendar** (← WorkingCalendars) | `Id` | Aucune | DayOfWeek, IsWorkingDay, StartTime/EndTime, CompanyId | Idem Holidays — dimension calendaire de calcul, pas jointe directement à un fait |
| **DimAlertDefinition** (← AlertDefinitions) | `Id` | `Code` | Name, CategoryId/SeverityId (à dénormaliser, cf. Groupe C), IsActive | Alerts |
| **DimCity** (← Cities) | `Id` | Aucune | CityName, CountryId | Utilisée via DimCompany et DimEmployee (adresse) — axe géographique transverse |
| **DimCountry** (← Countries) | `Id` | `CountryCode` | CountryName | DimCity, DimCompany, DimHoliday — dimension géographique racine partagée |
| **DimDate** *(à construire, non issue de la base source)* | Date (YYYYMMDD) | Date calendaire | Année, Mois, Trimestre, Nom du mois, Jour de semaine, indicateur jour férié (croisement avec DimHoliday) | Toutes les tables de faits ayant `Month`/`Year` ou une date : PayrollResults, LeaveBalances, EmployeeMonthly*Summaries |

> **Réserve importante sur DimEmployee/DimDepartement** : le schéma applicatif confirme que `DepartementId`, `ManagerId`, `CategoryId` et `MaritalStatusId` sur `Employees` sont en `SetNull` — c'est-à-dire que la valeur passe à NULL automatiquement dès que la ligne de référence est supprimée. Cela **explique** techniquement le taux de NULL élevé observé dans le profilage, mais ne le résout pas : tant que ces colonnes restent NULL sur 85 à 100 % des employés, l'axe "par département" ou "par manager" du dashboard restera peu fiable. Décision : les dimensions concernées (`DimDepartement`) sont conservées car structurellement nécessaires au besoin métier, mais leur usage réel doit être validé sur un extrait de production avant mise en avant dans le dashboard.

> **Point de modélisation à trancher avec l'équipe** : `EmployeeContracts` et `EmployeeSalaries` sont des historiques SCD2 sans lien direct vers `PayrollResults` (qui ne référence que `EmployeeId`/`CompanyId`). Pour afficher "le poste au moment du bulletin", il faudra soit (a) une jointure temporelle (date du bulletin comprise entre StartDate et EndDate du contrat), soit (b) dénormaliser le poste/salaire courant directement sur une dimension employé "snapshot". Ce choix impacte directement le modèle en étoile et doit être acté avant la phase de code.

---

## GROUPE C — Tables de référence

Ces tables sont trop petites ou trop peu structurantes pour justifier une dimension à part entière dans tous les cas — la décision (intégrer / dimension indépendante / rester en staging) est prise au cas par cas, en fonction de la cardinalité, du nombre de tables consommatrices, et de la fiabilité du lien observé dans le profilage.

| Table | Lignes | Décision | Justification |
|---|---|---|---|
| **Genders** | 2 | Intégrer directement dans DimEmployee (dénormaliser en attribut texte) | Cardinalité minimale (2 valeurs), un seul consommateur significatif (Employees) ; une dimension séparée pour 2 valeurs n'apporte rien en Power BI |
| **Nationalities** | 5 | Intégrer directement dans DimEmployee | Faible cardinalité, seul consommateur = Employees, taux de remplissage correct |
| **MaritalStatuses** | 4 | Intégrer directement dans DimEmployee, avec réserve | Faible cardinalité ; mais `Employees.MaritalStatusId` NULL à 85 % → l'attribut sera très souvent vide dans le dashboard, à signaler aux utilisateurs finaux |
| **EducationLevels** | 11 | Intégrer directement dans DimEmployee | Cardinalité modérée mais un seul consommateur, pas de valeur ajoutée à isoler en dimension séparée |
| **EmployeeCategories** | 86 | Rester en staging (ne pas exposer en BI pour l'instant) | `Employees.CategoryId` NULL à 99,74 % — la donnée existe côté référentiel mais n'est quasiment jamais rattachée à un employé ; exposer cette dimension créerait un axe d'analyse vide |
| **EmployeeAddresses** | 1213 | Intégrer directement dans DimEmployee (attributs Ville/CodePostal) | Relation quasi 1:1 avec l'employé, bon taux de remplissage sur `AddressLine1`/`CityId`, pas de justification à en faire une table de faits ou une dimension séparée |
| **LegalContractTypes** | 5 | Intégrer directement dans DimContractType | Un seul niveau d'indirection, très faible cardinalité, simplifie le modèle sans perte d'information |
| **StateEmploymentPrograms** | 3 | Intégrer directement dans DimContractType | Idem — cardinalité minimale, un seul consommateur (ContractTypes) |
| **LeaveTypePolicies** | 6 | Intégrer directement dans DimLeaveType | Configuration de politique par type de congé, faible volumétrie, pas d'intérêt à séparer |
| **LeaveTypeLegalRules** | 10 | Intégrer directement dans DimLeaveType | Règles légales rattachées 1-N à un type de congé ; à plat, exploitable comme attribut multi-valué ou table de correspondance simple, pas comme dimension indépendante |
| **AlertCategories** | 6 | Intégrer directement dans DimAlertDefinition | Cardinalité minimale, un seul consommateur (AlertDefinitions) |
| **AlertSeverities** | 3 | Intégrer directement dans DimAlertDefinition | Idem — mais **conserver le libellé de sévérité en clair** (Info/Warning/Critical) car c'est un axe de filtre probable du dashboard Alertes |
| **IrTaxBrackets** | 6 | Rester en staging | Barème utilisé en amont par le moteur de calcul de paie (déjà répercuté dans `PayrollResults.ImpotRevenu`) ; utile pour documenter/valider un calcul, pas comme axe d'analyse du dashboard |
| **OvertimeRateRules** | 6 | Rester en staging | Le fait associé (`EmployeeOvertimes`) est vide — pas d'intérêt à construire une dimension sans fait à alimenter ; à réactiver si le module heures supplémentaires est mis en production |
| **SystemConstants** | 32 | Rester en staging | Paramétrage technique (à confirmer s'il s'agit de paramètres légaux de paie) ; pas un axe d'analyse RH/Paie en tant que tel |

---

## GROUPE D — Tables à exclure

| Table | Raison technique/métier | Justification issue du profilage |
|---|---|---|
| Users | Hors périmètre RH/Paie (compte applicatif) | 28 lignes, aucune colonne métier RH ; sert uniquement d'acteur technique dans les logs |
| Roles, Permissions, RolesPermissions, UsersRoles | Sécurité applicative | Tables de contrôle d'accès, sans rapport avec les KPI RH/Paie |
| Invitations | Fonctionnalité d'onboarding compte | 0 ligne — non peuplée |
| __EFMigrationsHistory | Infrastructure EF Core | Table technique de migration de schéma, jamais métier |
| sysdiagrams | Artefact SQL Server Management Studio | 0 ligne, outillage DBA |
| LegalParameters | Référentiel de paramétrage légal | 0 ligne au profilage |
| BusinessSectors | Référentiel secteur d'activité | 0 ligne malgré une FK entrante (SalaryPackages, elle-même vide) |
| CompanyDocuments | Gestion documentaire | 3 lignes, aucune mesure ni dimension d'analyse |
| CompanyEventLogs | Journal d'audit société | Log technique (`oldValue/newValue`), et le schéma applicatif confirme l'absence de FK contrainte sur ses colonnes `companyId`/`employeeId` — non fiable pour une jointure de modèle en étoile |
| BankAccounts | Dimension bancaire non exploitée | `Employees.BankAccountId` NULL à 100 %, tables filles (`EmployeeBankAccountAllocations`, `PayrollBankPayments`) vides — aucune donnée de paiement bancaire tracée actuellement |
| EmployeeChildren | Volumétrie insuffisante | 2 lignes — pas de KPI famille/allocations fiable |
| EmployeeSpouses | Table vide | 0 ligne |
| EmployeeDocuments | Gestion documentaire | 0 ligne |
| EmployeeEventLogs | Journal d'audit RH générique | 921 lignes mais log technique (`oldValue/newValue`) sans FK contrainte confirmée par le schéma applicatif ; contenu de `eventName` non documenté dans le profilage |
| SalaryPackages, SalaryPackageItems, SalaryPackageAssignments | Modèle de package salarial non utilisé | 0 ligne chacune |
| PayComponents | Catalogue d'éléments de paie | 0 ligne — les montants calculés sont déjà à plat dans `PayrollResults`, ce catalogue source n'apporte rien tant qu'il est vide |
| PayrollBankPayments | Détail de virement bancaire | 0 ligne |
| PayrollCalculationAuditSteps | Journal technique du moteur de calcul | 9412 lignes mais 100 % technique (JSON d'entrée/sortie de formules), destiné au debug, pas à l'analyse |
| PayrollCustomRules | Règles de paie personnalisées (DSL) | 0 ligne ; le schéma applicatif confirme en outre l'absence de FK réelle sur `CompanyId` |
| EmployeeBankAccountAllocations | Répartition bancaire du salaire | 0 ligne |
| AncienneteRateSets, AncienneteRates | Barème de prime d'ancienneté | 0 ligne chacune — la prime calculée existe déjà dans `PayrollResults.PrimeAnciennete`, le barème source n'est pas peuplé |
| ElementRules, RuleCaps, RuleDualCaps, RuleFormulas, RulePercentages, RuleTiers, RuleVariants | Moteur de règles de calcul paramétrable | 0 ligne pour les 7 tables |
| ReferentielElements, ElementCategories, EligibilityCriteria, Authorities | Référentiel légal du moteur de règles | 0 ligne pour les 4 tables |
| CnssPreetabliImports, CnssPreetabliLines | Import de conformité CNSS | 0 ligne chacune |
| LeaveRequestApprovalHistories | Historique d'audit des décisions de congé | 2 lignes, log d'audit |
| LeaveRequestAttachments | Pièces jointes | 0 ligne |
| LeaveRequestExemptions | Exemptions de décompte de jours | 0 ligne |
| LeaveCarryOverAgreements | Accords de report de solde | 0 ligne |
| LeaveAuditLogs | Journal d'audit des congés | 7 lignes, log technique |
| EmployeeAttendances | Pointage journalier | **0 ligne** — fonctionnalité non utilisée dans cet extrait malgré son rôle attendu pour le module "Absences/Présence" ; à reconfirmer avec l'équipe produit |
| EmployeeAttendanceBreaks | Détail des pauses de pointage | 0 ligne, dépend d'EmployeeAttendances (vide) |
| EmployeeOvertimes | Heures supplémentaires déclarées | 0 ligne malgré une structure très riche (workflow d'approbation complet) |
| AlertDeliveries | Envoi effectif d'une alerte | 0 ligne |
| AlertAuditLogs | Journal d'audit des alertes | 234 lignes mais colonnes métier à 100 % NULL |
| AlertRecipientRoles | Configuration des destinataires d'alerte | Table de config technique (rôle↔définition), pas un axe d'analyse |
| AlertRuleExecutionLogs | Journal d'exécution du moteur d'alertes | 4719 lignes mais 100 % opérationnel/monitoring, pas RH |
| AlertUserPreferences, AlertUserPreferenceAudits, CompanyAlertPreferences | Préférences de notification d'alerte | 0 ligne chacune |
| NotificationCategories, NotificationDefinitions, Notifications, NotificationDeliveries, NotificationAuditLogs, NotificationUserPreferences, CompanyNotificationPreferences | Système de notification applicatif générique | Hors périmètre RH/Paie ; le schéma applicatif confirme que le lien `Notifications.SourceType/SourceId` vers une alerte est une référence logicielle "soft", sans FK — inexploitable pour une jointure de modèle en étoile |

---

## Synthèse chiffrée

| Groupe | Nb tables source | 
|---|---|
| A — Faits potentiels | 12 |
| B — Dimensions potentielles | 14 (+ 1 dimension calendaire à construire : DimDate) |
| C — Référence (à absorber dans B ou en staging) | 15 |
| D — Exclues | 62 |
| **Total** | **103** |

Sur 103 tables sources, **26 tables** (Groupes A + B) constituent le cœur du modèle en étoile, **15 tables** (Groupe C) viennent enrichir ces dimensions par dénormalisation plutôt que d'exister en tant que telles dans le DW, et **62 tables** (60 %) sont exclues — pour l'essentiel parce qu'elles sont vides à ce jour ou hors périmètre du dashboard RH/Paie.

**Réserve à ne pas perdre de vue avant la phase de code** : plusieurs tables du Groupe A (`LeaveRequests`, `EmployeeAbsences`, `EmployeeMonthlyOvertimeSummaries`) ont une volumétrie actuelle trop faible pour valider leur fiabilité. Leur présence dans le modèle est justifiée par le besoin fonctionnel exprimé, mais leur chargement effectif devrait être revalidé sur un extrait de production avant d'être mis en avant dans le dashboard final.
