# PayZenHR — Catalogue des KPI calculables

Règle appliquée : un KPI n'est retenu que si **toutes** les colonnes nécessaires à son calcul existent dans les tables du Groupe A/B/C (voir `PayZenHR_Modele_DW_Groupes_ABCD.md`) et présentent un taux de remplissage compatible avec un usage analytique. Quand une colonne existe mais que son taux de NULL rend le KPI non fiable, le KPI est classé « impossible » et la raison précise (colonne, % NULL) est donnée — pas de KPI "à moitié" présenté comme fiable.

---

## RH

| KPI | Définition métier | Formule | Table source | Colonnes utilisées | Fréquence | Dimensions d'analyse disponibles |
|---|---|---|---|---|---|---|
| Effectif actif | Nombre d'employés dont le statut a un impact "actif" sur la paie/présence | `COUNT(DISTINCT EmployeeId)` filtré sur `Statuses.AffectsPayroll = true` | Employees, Statuses | `Employees.StatusId`, `Statuses.AffectsPayroll` | Mensuelle / à la demande | Société, Genre, Nationalité, Niveau d'éducation |
| Répartition par genre | Part Hommes/Femmes dans l'effectif | `COUNT(EmployeeId) GROUP BY GenderId` | Employees, Genders | `Employees.GenderId` | Mensuelle | Société, Statut |
| Répartition par nationalité | Part de chaque nationalité dans l'effectif | `COUNT(EmployeeId) GROUP BY NationalityId` | Employees, Nationalities | `Employees.NationalityId` | Trimestrielle | Société |
| Répartition par niveau d'éducation | Part de chaque niveau de diplôme | `COUNT(EmployeeId) GROUP BY EducationLevelId` | Employees, EducationLevels | `Employees.EducationLevelId` | Trimestrielle | Société |
| Pyramide des âges | Distribution de l'effectif par tranche d'âge | `DATEDIFF(year, DateOfBirth, date_du_jour)` puis regroupement en tranches | Employees | `Employees.DateOfBirth` | Trimestrielle | Genre, Société |
| Effectif par société | Nombre d'employés par entité juridique | `COUNT(EmployeeId) GROUP BY CompanyId` | Employees | `Employees.CompanyId` | Mensuelle | — |
| Répartition par type de contrat | Part CDI/CDD/autre dans l'effectif sous contrat actif | `COUNT(EmployeeId) GROUP BY ContractTypeId` sur contrat actif (`EndDate IS NULL` ou `EndDate > date du jour`) | EmployeeContracts, ContractTypes | `EmployeeContracts.ContractTypeId`, `EmployeeContracts.EndDate` | Mensuelle | Société |
| Répartition par poste | Effectif par intitulé de poste, déduit du contrat actif | `COUNT(EmployeeId) GROUP BY JobPositionId` sur contrat actif | EmployeeContracts, JobPositions | `EmployeeContracts.JobPositionId`, `EmployeeContracts.EndDate` | Mensuelle | Société |
| Ancienneté moyenne | Durée moyenne depuis le premier contrat connu | `AVG(date_du_jour − MIN(StartDate))` par employé | EmployeeContracts | `EmployeeContracts.EmployeeId`, `EmployeeContracts.StartDate` | Trimestrielle | Société, Type de contrat |
| Répartition géographique des employés | Effectif par ville de résidence | `COUNT(EmployeeId) GROUP BY CityId` | EmployeeAddresses | `EmployeeAddresses.EmployeeId`, `EmployeeAddresses.CityId` | Trimestrielle | Société |

### KPI RH impossibles à calculer avec les données disponibles

| KPI envisagé | Colonnes nécessaires manquantes ou inexploitables |
|---|---|
| Taux de rotation / turnover | Aucune date de départ fiable n'existe : `Employees` n'a pas de colonne "date de fin de collaboration", et `EmployeeContracts.EndDate` marque la fin d'un *contrat* (souvent renouvelé), pas nécessairement un départ. Sans un statut horodaté ("inactif depuis le…"), le taux de rotation ne peut pas être calculé de façon fiable. |
| Répartition par département | `Employees.DepartementId` est NULL à 90,52 % — la donnée existe techniquement mais sur moins de 10 % des employés, ce qui rendrait le KPI trompeur. |
| Répartition par catégorie de paie | `Employees.CategoryId` est NULL à 99,74 % — quasiment aucun employé n'a de catégorie renseignée. |
| Taux d'encadrement / ratio manager-équipe | `Employees.ManagerId` est NULL à 100 % — aucune hiérarchie managériale n'est renseignée dans cet extrait. |
| Répartition par statut marital | `Employees.MaritalStatusId` est NULL à 85,27 % — trop incomplet pour un KPI agrégé fiable. |

---

## CONGÉS / ABSENCES

| KPI | Définition métier | Formule | Table source | Colonnes utilisées | Fréquence | Dimensions d'analyse disponibles |
|---|---|---|---|---|---|---|
| Jours d'absence cumulés par employé | Nombre de jours d'absence sur une période | `SUM(AbsenceDays)` | EmployeeMonthlyAbsenceSummaries | `EmployeeId`, `Year`, `Month`, `AbsenceDays` | Mensuelle | Employé, Société (via Employee) |
| Taux d'absentéisme | Part des jours d'absence rapportée aux jours ouvrés théoriques du mois | `SUM(AbsenceDays) / SUM(jours ouvrés du mois selon WorkingCalendars)` | EmployeeMonthlyAbsenceSummaries, WorkingCalendars | `AbsenceDays`, `WorkingCalendars.IsWorkingDay/DayOfWeek` | Mensuelle | Société, Employé |
| Jours de congé pris cumulés | Nombre de jours de congé posés et pris sur une période | `SUM(LeaveDays)` | EmployeeMonthlyLeaveSummaries | `EmployeeId`, `Year`, `Month`, `LeaveDays` | Mensuelle | Employé, Société |
| Solde de congés restant | Solde disponible à une date donnée | `ClosingDays` (dernière valeur connue de la période) | LeaveBalances | `EmployeeId`, `LeaveTypeId`, `Year`, `Month`, `ClosingDays` | Mensuelle | Employé, Type de congé |
| Taux d'utilisation du quota de congés | Part du solde annuel effectivement consommée | `UsedDays / (OpeningDays + AccruedDays)` | LeaveBalances | `UsedDays`, `OpeningDays`, `AccruedDays` | Mensuelle / annuelle | Employé, Type de congé |
| Risque de perte de solde | Nombre d'employés avec un solde reportable non nul proche de la date d'expiration | `COUNT(EmployeeId)` où `ClosingDays > 0` et `CarryoverExpiresOn` proche de la date du jour | LeaveBalances | `ClosingDays`, `CarryoverExpiresOn` | Mensuelle | Employé, Type de congé |
| Répartition congés vs absences | Comparaison du volume de jours "planifiés" (congés) vs "non planifiés" (absences) | `SUM(LeaveDays)` vs `SUM(AbsenceDays)` sur la même période | EmployeeMonthlyLeaveSummaries, EmployeeMonthlyAbsenceSummaries | `LeaveDays`, `AbsenceDays` | Mensuelle | Employé, Société |
| Jours travaillés cumulés | Nombre de jours effectivement travaillés sur une période | `SUM(WorkedDays)` | EmployeeMonthlyWorkedDaysSummaries | `EmployeeId`, `Year`, `Month`, `WorkedDays` | Mensuelle | Employé, Société |

### KPI Congés/Absences nécessitant une transformation (calculables mais avec réserve de fiabilité)

| KPI | Formule | Réserve |
|---|---|---|
| Taux d'approbation des demandes de congé | `COUNT(Status = Approuvé) / COUNT(*)` sur `LeaveRequests` | Techniquement calculable (`LeaveRequests.Status` existe), mais **seulement 2 lignes** dans l'extrait actuel — le résultat ne serait pas représentatif ; à revalider sur un jeu de données de production |
| Délai moyen de traitement d'une demande de congé | `AVG(DecisionAt − RequestedAt)` sur `LeaveRequests` | Même réserve — colonnes présentes (`RequestedAt`, `DecisionAt`) mais volumétrie insuffisante (2 lignes) pour un résultat fiable |
| Répartition des absences par type/motif | `COUNT(*) GROUP BY AbsenceType` sur `EmployeeAbsences` | Colonnes présentes mais seulement 2 lignes dans l'extrait — à revalider en production |
| Heures supplémentaires par employé/mois | `SUM(Hours25Pct + Hours50Pct + Hours100Pct)` sur `EmployeeMonthlyOvertimeSummaries` | Colonnes présentes et bien structurées, mais seulement **4 lignes** dans l'extrait — volumétrie insuffisante pour un KPI agrégé représentatif |

### KPI Congés/Absences impossibles à calculer

| KPI envisagé | Raison |
|---|---|
| Taux d'absentéisme non planifié vs planifié au niveau événement (jour précis) | `EmployeeAttendances` (pointage journalier, qui permettrait de croiser présence réelle et absence déclarée) est vide (0 ligne) — seul le niveau mensuel pré-agrégé (`EmployeeMonthlyAbsenceSummaries`) est exploitable, pas le détail jour par jour |

---

## PAIE

| KPI | Définition métier | Formule | Table source | Colonnes utilisées | Fréquence | Dimensions d'analyse disponibles |
|---|---|---|---|---|---|---|
| Masse salariale brute | Somme des rémunérations brutes versées | `SUM(TotalBrut)` | PayrollResults | `EmployeeId`, `CompanyId`, `Month`, `Year`, `TotalBrut` | Mensuelle | Société, Employé |
| Masse salariale nette | Somme des montants nets versés aux employés | `SUM(NetAPayer)` | PayrollResults | `NetAPayer` | Mensuelle | Société, Employé |
| Coût employeur total | Brut + charges patronales | `SUM(TotalBrut + TotalCotisationsPatronales)` | PayrollResults | `TotalBrut`, `TotalCotisationsPatronales` | Mensuelle | Société |
| Salaire net moyen | Rémunération nette moyenne par employé sur la période | `AVG(NetAPayer)` | PayrollResults | `NetAPayer` | Mensuelle | Société, Employé |
| Salaire brut moyen | Rémunération brute moyenne par employé | `AVG(TotalBrut)` ou `AVG(SalaireBase)` | PayrollResults | `TotalBrut`, `SalaireBase` | Mensuelle | Société |
| Taux de charges salariales | Poids des cotisations salariales dans le brut | `SUM(TotalCotisationsSalariales) / SUM(TotalBrut)` | PayrollResults | `TotalCotisationsSalariales`, `TotalBrut` | Mensuelle | Société |
| Taux de charges patronales | Poids des cotisations patronales dans le brut | `SUM(TotalCotisationsPatronales) / SUM(TotalBrut)` | PayrollResults | `TotalCotisationsPatronales`, `TotalBrut` | Mensuelle | Société |
| Poids de l'impôt sur le revenu | Part de l'IR dans le brut imposable | `SUM(ImpotRevenu) / SUM(BrutImposable)` | PayrollResults | `ImpotRevenu`, `BrutImposable` | Mensuelle | Société, Employé |
| Part des heures supplémentaires dans la masse salariale | Poids financier des heures sup dans le brut (effet déjà valorisé en paie) | `SUM(HeuresSupp25 + HeuresSupp50 + HeuresSupp100) / SUM(TotalBrut)` | PayrollResults | `HeuresSupp25/50/100`, `TotalBrut` | Mensuelle | Société |
| Structure primes imposables / non imposables | Répartition de la rémunération variable selon son régime fiscal | `SUM(TotalPrimesImposables)` vs `SUM(AutresPrimesNonImposable)` | PayrollResults | `TotalPrimesImposables`, `AutresPrimesNonImposable` | Mensuelle | Société |
| Détail des primes versées | Montant par type de prime réellement versé | `SUM(Montant) GROUP BY Label` | PayrollResultPrimes | `PayrollResultId`, `Label`, `Montant`, `IsTaxable` | Mensuelle | Bulletin de paie (Employé, Société, Mois via jointure au bulletin parent) |
| Évolution mensuelle du brut/net | Tendance de la masse salariale au sein de l'année 2026 | `SUM(TotalBrut)/SUM(NetAPayer) GROUP BY Month` | PayrollResults | `Month`, `TotalBrut`, `NetAPayer` | Mensuelle | Société — **limité à l'année 2026 dans l'extrait actuel, pas d'analyse pluriannuelle possible** |
| Cumul annuel brut/net/IR par employé | Cumuls fiscaux à date (YTD) | Lecture directe de `CumulBrut`, `CumulNet`, `CumulIr` | PayrollTaxSnapshots | `EmployeeId`, `Month`, `Year`, `CumulBrut`, `CumulNet`, `CumulIr`, `CumulCnss`, `CumulAmo` | Mensuelle | Société, Employé |
| Taux d'imposition effectif cumulé | Taux d'imposition réel calculé sur le cumul annuel | Lecture directe de `TauxEffectif` | PayrollTaxSnapshots | `TauxEffectif` | Mensuelle | Société, Employé |
| Structure de la rémunération (composants) | Répartition fixe/variable, imposable/social/CIMR des composants de salaire | `SUM(Amount) GROUP BY ComponentType, IsTaxable, IsSocial, IsCIMR` | EmployeeSalaryComponents | `EmployeeSalaryId`, `ComponentType`, `Amount`, `IsTaxable`, `IsSocial`, `IsCIMR` | Mensuelle | Employé (via EmployeeSalaries) |

### KPI Paie nécessitant une transformation

| KPI | Formule | Transformation nécessaire |
|---|---|---|
| Évolution du salaire de base dans le temps | Comparaison de `BaseSalary` entre deux `EffectiveDate` successives par employé | Nécessite de reconstruire la chronologie SCD2 (`EmployeeSalaries.EffectiveDate`/`EndDate`) pour identifier "le salaire en vigueur au mois M" — pas un simple agrégat direct |
| Coût moyen par département / poste | `AVG(TotalBrut) GROUP BY DepartementId` (via Employee) | Calculable techniquement (jointure PayrollResults → Employees → Departements), **mais fiabilité limitée** car `Employees.DepartementId` est NULL à 90,52 % — le résultat ne couvrirait qu'une minorité d'employés |

### KPI Paie impossibles à calculer

| KPI envisagé | Raison |
|---|---|
| Comparaison de la masse salariale entre exercices (YoY) | `PayrollResults.Year` ne contient que la valeur 2026 dans l'extrait fourni — aucune donnée d'un autre exercice pour comparer |
| Coût réel des heures supplémentaires déclarées (avant validation paie) | `EmployeeOvertimes` (déclaration/validation des heures sup) est vide — seul l'effet déjà intégré dans `PayrollResults` est visible, pas le processus de déclaration en amont |
| Répartition des paiements par compte bancaire | `PayrollBankPayments` et `EmployeeBankAccountAllocations` sont vides, et `Employees.BankAccountId` est NULL à 100 % |

---

## ALERTES

| KPI | Définition métier | Formule | Table source | Colonnes utilisées | Fréquence | Dimensions d'analyse disponibles |
|---|---|---|---|---|---|---|
| Nombre d'alertes par sévérité | Volume d'alertes actives ou historiques par niveau de gravité | `COUNT(Id) GROUP BY SeverityLabel` | Alerts | `SeverityId`, `SeverityLabel` | Hebdomadaire / à la demande | Société, Catégorie |
| Nombre d'alertes par catégorie | Volume d'alertes par thématique (RH, Paie, Congés...) | `COUNT(Id) GROUP BY CategoryName` | Alerts | `CategoryName` | Hebdomadaire | Société, Sévérité |
| Nombre d'alertes par société | Volume d'alertes par tenant | `COUNT(Id) GROUP BY CompanyId` | Alerts | `CompanyId` | Hebdomadaire | Sévérité, Catégorie |
| Volume d'alertes par type d'entité concernée | Répartition des alertes selon l'objet source (employé, contrat, bulletin...) | `COUNT(Id) GROUP BY EntityType` | Alerts | `EntityType` | Hebdomadaire | Société — **`EntityId` n'étant pas une FK réelle (référence polymorphe non contrainte), on ne peut pas descendre au détail de l'objet exact, seulement compter par type** |
| Taux d'alertes acquittées | Part des alertes marquées comme prises en compte | `COUNT(IsAcknowledged = true) / COUNT(*)` | Alerts | `IsAcknowledged` | Hebdomadaire | Société, Sévérité |

### KPI Alertes nécessitant une transformation (avec réserve forte)

| KPI | Formule | Réserve |
|---|---|---|
| Taux de résolution des alertes | `COUNT(Status = Résolue) / COUNT(*)` | Calculable via `Alerts.Status`, mais `ResolvedAt` est NULL à 92,19 % — la majorité des alertes n'a pas de trace de résolution effective, donc le résultat sera dominé par les alertes non closes |

### KPI Alertes impossibles à calculer

| KPI envisagé | Raison |
|---|---|
| Délai moyen de prise en charge (accusé de réception) | `Alerts.AcknowledgedAt` et `AcknowledgedByUserId` sont NULL à **100 %** — aucune alerte n'a de date d'accusé de réception enregistrée dans l'extrait |
| Délai moyen de résolution | `Alerts.ResolvedAt` est NULL à 92,19 % — seules ~5 alertes sur 64 ont une date de résolution ; volumétrie insuffisante pour une moyenne représentative |
| Alertes assignées par utilisateur | `AssignedToUserId` est NULL à 100 % — aucune assignation nominative enregistrée |
| Taux d'alertes expirées sans traitement | `ExpiresAt` est NULL à 100 % — aucune échéance n'est renseignée dans l'extrait |

---

## Synthèse : classification globale des KPI

| Statut | Nombre de KPI | Domaines concernés |
|---|---|---|
| **Directement calculables** | 24 | RH (10), Congés/Absences (8), Paie (14 — répartis ci-dessus), Alertes (5) |
| **Calculables avec transformation** (jointure temporelle, agrégation, ou réserve de volumétrie/fiabilité explicite) | 8 | RH (via contrat pour poste/type de contrat, déjà comptés ci-dessus comme "directs" car la transformation est simple), Congés/Absences (4), Paie (2), Alertes (1) |
| **Impossibles avec les données actuelles** | 12 | RH (5), Congés/Absences (1), Paie (3), Alertes (4) — cause systématique : colonne requise NULL à 85–100 %, table vide, ou absence de FK fiable |

**Point d'attention transverse** : plusieurs KPI classés "calculables" reposent sur des tables à très faible volumétrie dans l'extrait fourni (`LeaveRequests`, `EmployeeAbsences` : 2 lignes ; `EmployeeMonthlyOvertimeSummaries` : 4 lignes). La formule est correcte et les colonnes existent, mais le résultat affiché sur un dashboard de production ne sera représentatif qu'une fois un volume de données réel chargé — ce n'est pas un défaut de conception, c'est une limite de l'échantillon profilé.
