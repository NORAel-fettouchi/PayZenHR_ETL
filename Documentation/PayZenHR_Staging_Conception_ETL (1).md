# PayZenHR — Conception de la couche STAGING (`payzenhr_staging`)
## Rapport d'architecture de données — Transformations ETL PySpark (SQL Server local `PayZenHR_Source` → Staging)

**Auteur :** Senior Data Architect / Senior Data Engineer
**Périmètre :** Conception exhaustive de la couche Staging, table par table, colonne par colonne
**Sources analysées :** `DATABASE_SCHEMA_PAR_MODULE.md`, `rapport_tables.csv`, `columns_tables.csv`, `nulls_tables.csv`,
`statistics_tables.csv`, `duplicates_tables.csv`, `fk_tables.csv`
**Non fourni / non exploité :** `rapport_dates.csv` n'a pas été joint au dossier transmis → toute analyse fine
des formats/anomalies de dates individuelles (hors le constat structurel ci-dessous) reste
**« Information insuffisante pour décider »** et devra être complétée avant l'implémentation PySpark.
**Code PySpark :** volontairement absent de ce document, conformément à la demande — conception uniquement.

> **⚠️ Mise à jour d'environnement (consigne du tuteur de stage) — travail en local.**
> L'architecture cible a évolué par rapport à la version initiale : le développement et les tests s'effectuent
> désormais **en local**, avant tout déploiement Azure. Cela ne change **rien aux décisions de conception**
> ci-dessous (le profiling a été réalisé sur les mêmes données), mais modifie les paramètres de connexion et
> le nom du système source à utiliser dans les colonnes techniques (`SourceSystem`, cf. règle G10).
>
> | Élément | Version initiale (cible Azure) | Version actuelle (locale) |
> |---|---|---|
> | Moteur base source | Azure SQL Database | **SQL Server local** (ex. `localhost:1433`, instance par défaut ou nommée) |
> | Nom de la base source | `test_db` | **`PayZenHR_Source`** |
> | Driver JDBC | `mssql-jdbc` (identique) | `mssql-jdbc-13.4.0.jre8.jar` chargé via `spark.jars` |
> | Connexion | Chaîne Azure (`*.database.windows.net`, `encrypt=true`) | `jdbc:sqlserver://localhost:1433;databaseName=PayZenHR_Source;encrypt=false;trustServerCertificate=true;` |
> | Base de staging `payzenhr_staging` | Azure SQL Database | **À confirmer** — probablement SQL Server local également (une seconde base locale, ou un même serveur avec un schéma/catalogue distinct) → *Information insuffisante pour trancher tant que la cible de staging locale n'est pas précisée par le tuteur* |
> | ClickHouse / Power BI | Inchangé | Inchangé (hors périmètre de ce document) |
>
> Toutes les occurrences de `SourceSystem` dans les colonnes techniques (G10) ont été mises à jour de
> `'PayZenHR.test_db'` vers **`'PayZenHR.PayZenHR_Source'`**, et les mentions de la base source dans les
> justifications ont été alignées sur `PayZenHR_Source`. Le champ `SourceSystem` est conçu pour rester
> stable même après un futur passage en Azure — seule sa valeur littérale change selon l'environnement
> (variable de configuration ETL, pas une valeur codée en dur dans le pipeline PySpark).

---

## 0. Résumé exécutif

Le profiling porte sur **103 tables** (⚠️ le schéma `.md` annonce ~95 tables ; l'écart de +8 s'explique par des
tables techniques/EF non documentées dans le `.md` — `__EFMigrationsHistory`, `sysdiagrams` — et quelques tables
de résumés mensuels ajoutées après la rédaction du document de référence). Constats structurants pour la
conception de la Staging :

| Constat | Donnée | Impact conception |
|---|---|---|
| **Base majoritairement « à froid »** | 39 tables sur 103 ont **0 ligne** dans `PayZenHR_Source` (SQL Server local) | Schéma préparé pour toutes, chargement effectif différé pour ces 39 (cf. §1.6) |
| **Aucun doublon détecté** | `duplicates_tables.csv` : 0 doublon sur les 64 tables non-vides testées | La règle de dédoublonnage reste implémentée par défaut (robustesse production), mais n'est déclenchée par aucune donnée actuelle |
| **Dates stockées en texte** | **323 colonnes** sur **101 tables** portent une sémantique de date/heure (`CreatedAt`, `UpdatedAt`, `DeletedAt`, `DecisionAt`…) mais sont typées `StringType()` côté source | **Transformation cross-cutting n°1** : standardisation systématique des dates (T1) |
| **Soft-delete quasi inutilisé** | `DeletedAt`/`DeletedBy` sont NULL à 92–100 % sur la totalité des tables où ils existent | Staging conserve tout l'historique, aucun filtre de suppression appliqué en amont |
| **Pas de colonnes d'authentification sensibles** | Aucune colonne `PasswordHash` / `SecurityStamp` dans les 1608 lignes de `columns_tables.csv` | L'authentification est externalisée (`Users.ExternalId`, `Users.Source`) — transformation « suppression du hash de mot de passe » **non applicable** à ce schéma |
| **156 relations FK déclarées** | `fk_tables.csv` | Base de la validation d'intégrité référentielle en Staging (T4) |
| **77 colonnes sur `PayrollResults`** | dont des colonnes de traçabilité IA (`ClaudeModel`, `TokensUsed`) | Séparation faits métier / traçabilité technique (voir fiche dédiée) |

---

## 1. Cadre de gouvernance global (s'applique à TOUTES les tables sauf mention contraire)

### 1.1 Principe directeur de la couche Staging
La Staging n'est **pas** le modèle en étoile final. Elle reste **isomorphe à la source** (une table Staging par
table source, sauf fusions justifiées au cas par cas), mais **nettoyée, typée correctement, contrôlée en
intégrité, dédupliquée et enrichie de métadonnées techniques**. Le remodelage en `Dim_*` / `Fact_*` (schéma en
étoile, clés de substitution numériques, SCD géré explicitement) est repoussé à la couche ClickHouse, qui est le
véritable Data Warehouse. Ce choix limite les pertes d'information et facilite le débogage (traçabilité 1:1 avec
la source) — écart volontaire par rapport à un Staging « brut » (bronze) : les 10 transformations demandées par
le cahier des charges (dates, NULL, doublons, FK, etc.) placent cette couche plutôt en zone **« silver »**.

### 1.2 Convention de nommage
- **Tables** : nom identique à la source (PascalCase), sans préfixe (`Stg_` volontairement évité pour ne pas
  alourdir le mapping avec ClickHouse en aval — à documenter dans le data dictionary si l'équipe préfère un préfixe).
- **Colonnes** : PascalCase conservé (cohérent avec le modèle EF Core existant et avec Power BI, qui affiche les
  noms de colonnes tels quels). Seules les colonnes en casse incohérente avec le reste du schéma sont harmonisées
  (`employeeId` → `EmployeeId`, `companyId` → `CompanyId` sur `EmployeeEventLogs`/`CompanyEventLogs`, cf. T6).
- **Colonnes calculées** : suffixées explicitement par leur nature (`…Years`, `…Days`, `Is…` pour les booléens).

### 1.3 Catalogue des règles globales de colonnes (référencées `[G1]`…`[G10]` dans chaque fiche table)

| Code | Règle |
|---|---|
| **G1** | `Id` (clé primaire source) : conservée telle quelle, castée en `Int64`, `NOT NULL`. Sert de clé de correspondance avec `PayZenHR_Source` (SQL Server local) — aucune nouvelle clé de substitut n'est générée en Staging (le `Id` source en tient lieu ; les clés de substitution ClickHouse, si nécessaires au modèle en étoile, seront générées en aval). |
| **G2** | Colonnes `*Id` (clé étrangère) : conservées, castées en `Nullable(Int64)` (la majorité des FK du schéma sont configurées `SetNull`, donc nullable par nature), validées par rapprochement avec la table référencée (T4). |
| **G3** | Colonnes d'audit EF Core (`CreatedAt`, `CreatedBy`, `UpdatedAt`, `UpdatedBy`) : conservées pour traçabilité. `CreatedAt`/`UpdatedAt`, typées `StringType()` côté source, sont converties en `Nullable(DateTime64(3))` (T1). `CreatedBy`/`UpdatedBy` castées en `Nullable(Int64)`, **non validées par FK** (non contraintes côté EF). |
| **G4** | Colonnes de suppression logique (`DeletedAt`, `DeletedBy`) : quasi totalement NULL sur toutes les tables observées (92–100 %). Conservées (mêmes règles de type que G3) **+ colonne dérivée `IsDeleted` (Bool)** = `DeletedAt IS NOT NULL`. Le Staging **ne filtre pas** les lignes soft-deleted : l'historique complet est chargé, le filtrage applicatif se fait en aval (DWH/BI). |
| **G5** | Champs texte libres : conservés, `TRIM`, suppression des caractères de contrôle, normalisation des espaces multiples, encodage forcé UTF-8. |
| **G6** | Champs booléens : conservés, castés `UInt8` (Bool ClickHouse). Un NULL n'est remplacé par `False` que si une justification métier explicite est documentée dans la fiche table — jamais par défaut. |
| **G7** | Champs numériques / montants (`Decimal`) : conservés avec la précision source. **Un NULL n'est jamais remplacé par 0** (particulièrement critique en paie : NULL = « composante non applicable », 0 = « applicable mais nulle » — deux sémantiques différentes). |
| **G8** | Tables/colonnes sans donnée exploitable (0 ligne) : profiling impossible, décision de conception basée uniquement sur le schéma déclaratif. Toute affirmation de qualité reste **« Information insuffisante pour décider »** tant qu'aucune donnée réelle n'est chargée. |
| **G9** | Colonnes à 100 % NULL sur le jeu de données actuel mais dont la présence est cohérente avec une fonctionnalité applicative existante : **conservées** (pas de suppression réflexe), signalées « 🔎 à ressurveiller après un chargement de production » plutôt que supprimées, pour éviter une perte d'information si `PayZenHR_Source` (SQL Server local) n'est pas représentatif du volume/de la variété de production. |
| **G10** | Colonnes techniques **ajoutées systématiquement** à chaque table Staging : `LoadDate` (`DateTime64(3)`, horodatage d'écriture), `SourceSystem` (`String`, constante `'PayZenHR.PayZenHR_Source'`), `SourceTable` (`String`, nom de la table source), `ETLExecutionDate` (`Date`, date du batch). |

### 1.4 Catalogue des transformations cross-cutting (référencées `T1`…`T10`)

| Code | Transformation | Portée |
|---|---|---|
| **T1** | **Standardisation des dates.** 323 colonnes / 101 tables typées `StringType()` mais sémantiquement temporelles. PySpark : `to_timestamp` multi-format (ISO8601 avec/sans fuseau, `yyyy-MM-dd HH:mm:ss`, etc.) ; échec de parsing → NULL **+ ligne journalisée** dans `Staging.audit_dates_rejected` (jamais de date fabriquée par défaut). | 101 tables |
| **T2** | Suppression des colonnes techniques sans valeur analytique (binaires `RowVersion`, artefacts d'outils comme `sysdiagrams.definition`). | Ponctuel |
| **T3** | Dédoublonnage : `ROW_NUMBER() OVER (PARTITION BY clé_métier ORDER BY UpdatedAt DESC, CreatedAt DESC) = 1`. Non déclenché aujourd'hui (0 doublon constaté) mais implémenté par défaut pour la robustesse en production. | Toutes |
| **T4** | Validation des FK / lignes orphelines : `LEFT ANTI JOIN` contre la table référencée pour chacune des 156 relations déclarées. FK nullable (`SetNull`) → valeur mise à `NULL` + ligne conservée et journalisée ; FK obligatoire au sens métier (ex. `PayrollResults.EmployeeId`, `Employees.CompanyId`) → ligne rejetée en quarantaine (`Staging.audit_orphans`), jamais supprimée silencieusement. | Toutes les tables avec FK |
| **T5** | Ajout des colonnes techniques (= G10). | Toutes |
| **T6** | Normalisation des noms de colonnes (casse), harmonisation des suffixes `Id`. | `EmployeeEventLogs`, `CompanyEventLogs` |
| **T7** | Suppression de colonnes 100 % vides : **jamais appliquée par défaut** (cf. G9), décidée au cas par cas avec justification métier explicite dans la fiche table. | — |
| **T8** | Calculs RH dérivés : `AgeYears` (`Employees.DateOfBirth`), `SeniorityYears`/`SeniorityMonths` (`EmployeeContracts.StartDate`), `IsActiveContract`, `IsActiveEmployee`. **Snapshot à `ETLExecutionDate`** — pour un usage BI « temps réel », recalculer dynamiquement en vue ClickHouse plutôt que se fier à la valeur figée en Staging. | `Employees`, `EmployeeContracts` |
| **T9** | Blobs JSON (`ResultatJson`, `AutoRulesJson`, `CimrConfigJson`, `ImportMetadataJson`, `OldValueJson`/`NewValueJson`) : isolés dans des tables techniques `*_TechnicalAudit` séparées des faits métier, pour ne pas alourdir les scans analytiques ClickHouse. Parsing structuré différé (pas de valeur ajoutée sans besoin analytique identifié). | `PayrollResults`, `SalaryPackages`, `*_AuditLogs` |
| **T10** | Confidentialité (Loi 09-08 marocaine — CNDP / esprit RGPD) : `CinNumber`, `DateOfBirth`, `Phone`, `Email`, `PersonalEmail`, `CnssNumber`, `CimrNumber`, `RibNumber`, `PrivateInsuranceNumber` sont des données personnelles sensibles. **Non supprimées** (nécessaires légalement à la paie) mais accès Staging restreint à l'équipe Data Engineering ; masquage/pseudonymisation recommandé pour les vues Power BI non-RH. | `Employees` et dérivées |

### 1.5 Légende des décisions de classification (table)
🟢 **CONSERVÉE TELLE QUELLE** — 🟡 **TRANSFORMÉE** — 🟣 **FUSIONNÉE** — 🔵 **CONSERVÉE (chargement différé, 0 ligne)** — 🔴 **SUPPRIMÉE / IGNORÉE**

### 1.6 Traitement des 39 tables à 0 ligne
Ces tables ne sont **ni ignorées ni supprimées** : elles correspondent à des modules fonctionnels de
PayZenHR non encore utilisés par le tenant de test (pointage, GED, moteur de règles de paie avancé,
notifications multi-canal…), et leur absence de données ne signifie pas absence de valeur future. Décision
retenue : **le schéma Staging est conçu pour toutes**, mais leur **chargement effectif est différé** jusqu'à
disponibilité d'un volume de données suffisant pour valider le profiling (NULL%, doublons, formats). Elles sont
repérées 🔵 dans les fiches ci-après, avec la mention explicite *« Information insuffisante pour décider »*
partout où une affirmation de qualité serait une hypothèse non vérifiable.

### 1.7 Deux tables exclues définitivement de la couche décisionnelle
`__EFMigrationsHistory` et `sysdiagrams` sont des tables **techniques internes** (Entity Framework Core / SQL
Server Management Studio) sans aucune portée métier RH/Paie. Décision : **SUPPRIMÉES**, non mappées vers
`payzenhr_staging`.

### 1.8 Table candidate à la fusion (avec données réelles)
`PayrollTaxSnapshots` (581 lignes) est en relation **1:1 stricte** avec `PayrollResults` (FK `PayrollResultId`,
suppression en cascade, confirmé par `DATABASE_SCHEMA_PAR_MODULE.md`) et duplique `EmployeeId`/`CompanyId`/
`Month`/`Year`. Un JOIN 1:1 systématique n'apportant pas de cardinalité différente, elle est **fusionnée** dans
un module élargi `Staging.PayrollResults_Fiscal` plutôt que maintenue comme table séparée (détail en fiche
dédiée, module Payroll).

---


## Module 1 — Auth (Utilisateurs, Rôles, Permissions)

### `Users`

**Lignes source :** 28 | **Colonnes source :** 14 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 12

**Décision table : 🟡 TRANSFORMÉE**


**Pourquoi :** Table pivot d'authentification. Aucune colonne PasswordHash/SecurityStamp trouvée dans le profiling (columns_tables.csv) : l'authentification est déléguée à un IdP externe (ExternalId + Source, 3.57% NULL). Donc l'exemple de transformation « suppression des PasswordHash/SecurityStamp » cité en introduction ne s'applique pas à ce schéma — Information insuffisante pour confirmer un fournisseur d'identité précis (Azure AD B2C, Auth0…) : à valider avec l'équipe applicative.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| EmployeeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). (NULL=10.7%) [G2] |
| Username | `StringType()` | Conserver + nettoyer | String | Normaliser (lower-case) pour dédoublonnage logique ; Email est la colonne pivot fonctionnelle. [G5] |
| Email | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| EmailPersonal | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| IsActive | `BooleanType()` | Conserver | UInt8 (Bool) | Conserver — sert de filtre d'utilisateurs actifs côté DWH. [G6] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=14.3%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |
| ExternalId | `StringType()` | Conserver + nettoyer | String | Conserver — clé de rapprochement avec l'IdP externe. (NULL=3.6%) [G5] |
| Source | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=3.6%) [G5] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Transformations ETL spécifiques :**

- Validation FK EmployeeId → Employees.Id (10.71% NULL = comptes non liés à un employé, ex. comptes admin plateforme — cas métier légitime, ne pas rejeter).

**Relations (validation FK à l'ETL) :**
- `Users.EmployeeId` → `Employees.Id` (sortante)
- `Alerts.AssignedToUserId` → `Users.Id` (entrante)
- `Alerts.AcknowledgedByUserId` → `Users.Id` (entrante)
- `Alerts.ResolvedByUserId` → `Users.Id` (entrante)
- `AlertUserPreferences.UserId` → `Users.Id` (entrante)
- `CompanyAlertPreferences.UpdatedByUserId` → `Users.Id` (entrante)
- `Notifications.UserId` → `Users.Id` (entrante)
- `NotificationUserPreferences.UserId` → `Users.Id` (entrante)
- `UsersRoles.UserId` → `Users.Id` (entrante)
- `AlertAuditLogs.UserId` → `Users.Id` (entrante)
- `AlertDeliveries.UserId` → `Users.Id` (entrante)
- `NotificationAuditLogs.UserId` → `Users.Id` (entrante)
- `NotificationDeliveries.UserId` → `Users.Id` (entrante)

**Schéma final `payzenhr_staging.Users`** (18 colonnes) :

```
Id                               Int64
EmployeeId                       Nullable(Int64)
Username                         String
Email                            String
EmailPersonal                    String
IsActive                         UInt8 (Bool)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
ExternalId                       String
Source                           String
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `Roles`

**Lignes source :** 9 | **Colonnes source :** 9 | **Doublons détectés :** 0 | **FK sortantes :** 0 | **FK entrantes (référencée par) :** 4

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** Table de référence (lookup) des rôles applicatifs, 9 lignes, structure stable.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Name | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Description | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=88.9%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=88.9%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=88.9%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `AlertRecipientRoles.RoleId` → `Roles.Id` (entrante)
- `RolesPermissions.RoleId` → `Roles.Id` (entrante)
- `UsersRoles.RoleId` → `Roles.Id` (entrante)
- `Invitations.RoleId` → `Roles.Id` (entrante)

**Schéma final `payzenhr_staging.Roles`** (13 colonnes) :

```
Id                               Int64
Name                             String
Description                      String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `Permissions`

**Lignes source :** 473 | **Colonnes source :** 11 | **Doublons détectés :** 0 | **FK sortantes :** 0 | **FK entrantes (référencée par) :** 1

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** Catalogue de permissions (473 lignes) — dimension de référence statique.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Name | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Description | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Resource | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Action | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=94.1%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=94.1%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `RolesPermissions.PermissionId` → `Permissions.Id` (entrante)

**Schéma final `payzenhr_staging.Permissions`** (15 colonnes) :

```
Id                               Int64
Name                             String
Description                      String
Resource                         String
Action                           String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `RolesPermissions`

**Lignes source :** 1790 | **Colonnes source :** 9 | **Doublons détectés :** 0 | **FK sortantes :** 2 | **FK entrantes (référencée par) :** 0

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** Table de jonction N:N Roles↔Permissions (1790 lignes). Aucune transformation métier requise au-delà des règles globales.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| RoleId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| PermissionId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=98.7%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=98.7%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=98.7%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `RolesPermissions.RoleId` → `Roles.Id` (sortante)
- `RolesPermissions.PermissionId` → `Permissions.Id` (sortante)

**Schéma final `payzenhr_staging.RolesPermissions`** (13 colonnes) :

```
Id                               Int64
RoleId                           Nullable(Int64)
PermissionId                     Nullable(Int64)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `UsersRoles`

**Lignes source :** 92 | **Colonnes source :** 9 | **Doublons détectés :** 0 | **FK sortantes :** 2 | **FK entrantes (référencée par) :** 0

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** Table de jonction N:N Users↔Roles (92 lignes).


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| UserId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| RoleId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=95.7%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=95.7%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=98.9%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `UsersRoles.UserId` → `Users.Id` (sortante)
- `UsersRoles.RoleId` → `Roles.Id` (sortante)

**Schéma final `payzenhr_staging.UsersRoles`** (13 colonnes) :

```
Id                               Int64
UserId                           Nullable(Int64)
RoleId                           Nullable(Int64)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `Invitations`

**Lignes source :** 0 | **Colonnes source :** 14 | **Doublons détectés :** 0 | **FK sortantes :** 2 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne dans PayZenHR_Source. Fonctionnalité d'invitation par email — structure prête, non alimentée en Phase 1. Information insuffisante pour juger de la qualité des données (Token notamment).


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Token | `StringType()` | Conserver + nettoyer | String | Donnée sensible (jeton d'invitation à usage unique) — à exclure de tout export Power BI, conserver uniquement pour audit technique restreint. [G5] |
| Email | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| RoleId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| EmployeeId | `IntegerType()` | Conserver (FK implicite non contrainte) | Nullable(Int64) | Suffixe Id sans contrainte FK EF déclarée — conservée en l'état, validée si référence identifiée. [G2] |
| Status | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| ExpiresAt | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `Invitations.CompanyId` → `Companies.Id` (sortante)
- `Invitations.RoleId` → `Roles.Id` (sortante)

**Schéma final `payzenhr_staging.Invitations`** (18 colonnes) :

```
Id                               Int64
Token                            String
Email                            String
CompanyId                        Nullable(Int64)
RoleId                           Nullable(Int64)
EmployeeId                       Nullable(Int64)
Status                           String
ExpiresAt                        DateTime64(3)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---


## Module 2 — Company (Sociétés, Organisation)

### `Companies`

**Lignes source :** 43 | **Colonnes source :** 38 | **Doublons détectés :** 0 | **FK sortantes :** 3 | **FK entrantes (référencée par) :** 29

**Décision table : 🟡 TRANSFORMÉE**


**Pourquoi :** Table centrale multi-tenant (43 lignes). Fort taux de NULL sur les champs légaux marocains (PatenteNumber 97.67%, SignatoryName/Title 100%, WebsiteUrl 100%, MatriculeTemplate 100%) : cohérent avec un jeu de test partiellement renseigné plutôt qu'une anomalie — à confirmer en production (G9).


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| CompanyName | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Email | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| PhoneNumber | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CountryPhoneCode | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CompanyAddress | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CityId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| CountryId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| CnssNumber | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| IsCabinetExpert | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| IceNumber | `StringType()` | Conserver + nettoyer | String | Identifiant légal marocain (ICE) — nettoyer espaces/tirets, conserver format source, ne pas recalculer. (NULL=46.5%) [G5] |
| IfNumber | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=48.8%) [G5] |
| RcNumber | `StringType()` | Conserver + nettoyer | String | Registre de commerce — nettoyage espace uniquement, pas de validation de checksum (info. insuffisante sur le format officiel). (NULL=51.2%) [G5] |
| PatenteNumber | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=97.7%) [G5] |
| RibNumber | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=60.5%) [G5] |
| LegalForm | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=60.5%) [G5] |
| SignatoryName | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| SignatoryTitle | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| FoundingDate | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). (NULL=90.7%) [G3] |
| WebsiteUrl | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| Currency | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| PayrollPeriodicity | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| FiscalYearStartMonth | `IntegerType()` | Conserver | Int32 |  [G-] |
| BusinessSector | `StringType()` | Conserver + nettoyer | String | Colonne texte libre 100% NULL alors qu'une table BusinessSectors existe dans le module Payroll Référentiel — incohérence de modélisation à signaler à l'équipe applicative (doublon fonctionnel probable), conservée telle quelle en staging sans fusion arbitraire. (NULL=100.0%) [G5] |
| PaymentMethod | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| ManagedByCompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | FK self-référence (holding). Valider absence de cycle (A gère B gère A) lors du contrôle FK. (NULL=53.5%) [G2] |
| isActive | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=39.5%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=74.4%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |
| AuthType | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| MatriculeTemplate | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| MatriculeNextValue | `IntegerType()` | Conserver | Int32 |  [G-] |
| CimrNumber | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=72.1%) [G5] |
| PayrollEngineMode | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Transformations ETL spécifiques :**

- Autoréférence ManagedByCompanyId : détection de cycles avant chargement DWH (graph check).

**Relations (validation FK à l'ETL) :**
- `Companies.CityId` → `Cities.Id` (sortante)
- `Companies.CountryId` → `Countries.Id` (sortante)
- `Companies.ManagedByCompanyId` → `Companies.Id` (sortante)
- `EmployeeContracts.CompanyId` → `Companies.Id` (entrante)
- `LeaveBalances.CompanyId` → `Companies.Id` (entrante)
- `AlertRuleExecutionLogs.CompanyId` → `Companies.Id` (entrante)
- `LeaveCarryOverAgreements.CompanyId` → `Companies.Id` (entrante)
- `Companies.ManagedByCompanyId` → `Companies.Id` (entrante)
- `Alerts.CompanyId` → `Companies.Id` (entrante)
- `PayrollResults.CompanyId` → `Companies.Id` (entrante)
- `AlertUserPreferences.CompanyId` → `Companies.Id` (entrante)
- `AncienneteRateSets.CompanyId` → `Companies.Id` (entrante)
- `LeaveRequests.CompanyId` → `Companies.Id` (entrante)
- `CompanyAlertPreferences.CompanyId` → `Companies.Id` (entrante)
- `CompanyDocuments.CompanyId` → `Companies.Id` (entrante)
- `ContractTypes.CompanyId` → `Companies.Id` (entrante)
- `CompanyNotificationPreferences.CompanyId` → `Companies.Id` (entrante)
- `Departements.CompanyId` → `Companies.Id` (entrante)
- `Notifications.CompanyId` → `Companies.Id` (entrante)
- `EmployeeCategories.CompanyId` → `Companies.Id` (entrante)
- `NotificationUserPreferences.CompanyId` → `Companies.Id` (entrante)
- `Holidays.CompanyId` → `Companies.Id` (entrante)
- `JobPositions.CompanyId` → `Companies.Id` (entrante)
- `LeaveAuditLogs.CompanyId` → `Companies.Id` (entrante)
- `LeaveTypes.CompanyId` → `Companies.Id` (entrante)
- `SalaryPackages.CompanyId` → `Companies.Id` (entrante)
- `WorkingCalendars.CompanyId` → `Companies.Id` (entrante)
- `BankAccounts.CompanyId` → `Companies.Id` (entrante)
- `Invitations.CompanyId` → `Companies.Id` (entrante)
- `Employees.CompanyId` → `Companies.Id` (entrante)
- `CnssPreetabliImports.CompanyId` → `Companies.Id` (entrante)
- `LeaveTypePolicies.CompanyId` → `Companies.Id` (entrante)

**Schéma final `payzenhr_staging.Companies`** (42 colonnes) :

```
Id                               Int64
CompanyName                      String
Email                            String
PhoneNumber                      String
CountryPhoneCode                 String
CompanyAddress                   String
CityId                           Nullable(Int64)
CountryId                        Nullable(Int64)
CnssNumber                       String
IsCabinetExpert                  UInt8 (Bool)
IceNumber                        String
IfNumber                         String
RcNumber                         String
PatenteNumber                    String
RibNumber                        String
LegalForm                        String
SignatoryName                    String
SignatoryTitle                   String
FoundingDate                     DateTime64(3)
WebsiteUrl                       String
Currency                         String
PayrollPeriodicity               String
FiscalYearStartMonth             Int32
BusinessSector                   String
PaymentMethod                    String
ManagedByCompanyId               Nullable(Int64)
isActive                         UInt8 (Bool)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
AuthType                         String
MatriculeTemplate                String
MatriculeNextValue               Int32
CimrNumber                       String
PayrollEngineMode                String
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `Departements`

**Lignes source :** 585 | **Colonnes source :** 14 | **Doublons détectés :** 0 | **FK sortantes :** 3 | **FK entrantes (référencée par) :** 2

**Décision table : 🟡 TRANSFORMÉE**


**Pourquoi :** 585 lignes, structure hiérarchique auto-référencée (ParentDepartementId, 28.21% NULL = départements racine, cohérent). ManagerId 100% NULL sur le jeu de test actuel : fonctionnalité non encore utilisée (G9), conservée.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| DepartementName | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |
| DisplayOrder | `IntegerType()` | Conserver | Int32 |  [G-] |
| HierarchyLevel | `IntegerType()` | Conserver | Int32 |  [G-] |
| Description | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=24.6%) [G5] |
| ParentDepartementId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Auto-FK — construire un niveau hiérarchique (Depth) et un chemin (Path) en staging pour faciliter les rollups Power BI. (NULL=28.2%) [G2] |
| ManagerId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). (NULL=100.0%) [G2] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Transformations ETL spécifiques :**

- Calcul de la profondeur hiérarchique (DepartementLevel) par parcours récursif du ParentDepartementId (arbre).

**Relations (validation FK à l'ETL) :**
- `Departements.CompanyId` → `Companies.Id` (sortante)
- `Departements.ParentDepartementId` → `Departements.Id` (sortante)
- `Departements.ManagerId` → `Employees.Id` (sortante)
- `Departements.ParentDepartementId` → `Departements.Id` (entrante)
- `Employees.DepartementId` → `Departements.Id` (entrante)

**Schéma final `payzenhr_staging.Departements`** (18 colonnes) :

```
Id                               Int64
DepartementName                  String
CompanyId                        Nullable(Int64)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
DisplayOrder                     Int32
HierarchyLevel                   Int32
Description                      String
ParentDepartementId              Nullable(Int64)
ManagerId                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `ContractTypes`

**Lignes source :** 174 | **Colonnes source :** 11 | **Doublons détectés :** 0 | **FK sortantes :** 3 | **FK entrantes (référencée par) :** 1

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 174 lignes, dimension de configuration par société. LegalContractTypeId/StateEmploymentProgramId 100% NULL sur le jeu actuel : options légales non toutes utilisées (G9).


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| ContractTypeName | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| LegalContractTypeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). (NULL=100.0%) [G2] |
| StateEmploymentProgramId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). (NULL=100.0%) [G2] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `ContractTypes.CompanyId` → `Companies.Id` (sortante)
- `ContractTypes.LegalContractTypeId` → `LegalContractTypes.Id` (sortante)
- `ContractTypes.StateEmploymentProgramId` → `StateEmploymentPrograms.Id` (sortante)
- `EmployeeContracts.ContractTypeId` → `ContractTypes.Id` (entrante)

**Schéma final `payzenhr_staging.ContractTypes`** (15 colonnes) :

```
Id                               Int64
ContractTypeName                 String
CompanyId                        Nullable(Int64)
LegalContractTypeId              Nullable(Int64)
StateEmploymentProgramId         Nullable(Int64)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `JobPositions`

**Lignes source :** 387 | **Colonnes source :** 9 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 1

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 387 lignes, dimension de poste par société.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Name | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `JobPositions.CompanyId` → `Companies.Id` (sortante)
- `EmployeeContracts.JobPositionId` → `JobPositions.Id` (entrante)

**Schéma final `payzenhr_staging.JobPositions`** (13 colonnes) :

```
Id                               Int64
Name                             String
CompanyId                        Nullable(Int64)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `Holidays`

**Lignes source :** 34 | **Colonnes source :** 24 | **Doublons détectés :** 0 | **FK sortantes :** 2 | **FK entrantes (référencée par) :** 2

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 34 lignes. CompanyId 100% NULL = jours fériés nationaux génériques (non spécifiques à une société), cohérent avec le modèle (CompanyId nullable). RecurrenceRule 100% NULL : fonctionnalité de récurrence non utilisée (G9).


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| NameFr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameAr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameEn | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| HolidayDate | `DateType()` | Conserver + convertir type | Date | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| Description | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). (NULL=100.0%) [G2] |
| CountryId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| Scope | `IntegerType()` | Conserver | Int32 |  [G-] |
| HolidayType | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| IsMandatory | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| IsPaid | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| IsRecurring | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| RecurrenceRule | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| Year | `IntegerType()` | Conserver | Int32 |  [G-] |
| AffectPayroll | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| AffectAttendance | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| IsActive | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `Holidays.CompanyId` → `Companies.Id` (sortante)
- `Holidays.CountryId` → `Countries.Id` (sortante)
- `EmployeeOvertimes.HolidayId` → `Holidays.Id` (entrante)
- `LeaveRequestExemptions.HolidayId` → `Holidays.Id` (entrante)

**Schéma final `payzenhr_staging.Holidays`** (28 colonnes) :

```
Id                               Int64
NameFr                           String
NameAr                           String
NameEn                           String
HolidayDate                      Date
Description                      String
CompanyId                        Nullable(Int64)
CountryId                        Nullable(Int64)
Scope                            Int32
HolidayType                      String
IsMandatory                      UInt8 (Bool)
IsPaid                           UInt8 (Bool)
IsRecurring                      UInt8 (Bool)
RecurrenceRule                   String
Year                             Int32
AffectPayroll                    UInt8 (Bool)
AffectAttendance                 UInt8 (Bool)
IsActive                         UInt8 (Bool)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `WorkingCalendars`

**Lignes source :** 325 | **Colonnes source :** 12 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 0

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 325 lignes. StartTime/EndTime 25.85% NULL : calendriers à horaires variables/flexibles, cohérent métier.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| DayOfWeek | `IntegerType()` | Conserver | Int32 |  [G-] |
| IsWorkingDay | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| StartTime | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). (NULL=25.9%) [G3] |
| EndTime | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). (NULL=25.9%) [G3] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=95.7%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=95.7%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `WorkingCalendars.CompanyId` → `Companies.Id` (sortante)

**Schéma final `payzenhr_staging.WorkingCalendars`** (16 colonnes) :

```
Id                               Int64
CompanyId                        Nullable(Int64)
DayOfWeek                        Int32
IsWorkingDay                     UInt8 (Bool)
StartTime                        DateTime64(3)
EndTime                          DateTime64(3)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `CompanyDocuments`

**Lignes source :** 3 | **Colonnes source :** 11 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 0

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 3 lignes, GED société, structure simple.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| Name | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| FilePath | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| DocumentType | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `CompanyDocuments.CompanyId` → `Companies.Id` (sortante)

**Schéma final `payzenhr_staging.CompanyDocuments`** (15 colonnes) :

```
Id                               Int64
CompanyId                        Nullable(Int64)
Name                             String
FilePath                         String
DocumentType                     String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---


## Module 3 — Référentiel (Lookups partagés)

### `Countries`

**Lignes source :** 2 | **Colonnes source :** 11 | **Doublons détectés :** 0 | **FK sortantes :** 0 | **FK entrantes (référencée par) :** 5

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 2 lignes — dimension géographique de base.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| CountryName | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CountryNameAr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CountryCode | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CountryPhoneCode | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `Cities.CountryId` → `Countries.Id` (entrante)
- `Companies.CountryId` → `Countries.Id` (entrante)
- `Holidays.CountryId` → `Countries.Id` (entrante)
- `Employees.CountryId` → `Countries.Id` (entrante)
- `EmployeeAddresses.CountryId` → `Countries.Id` (entrante)

**Schéma final `payzenhr_staging.Countries`** (15 colonnes) :

```
Id                               Int64
CountryName                      String
CountryNameAr                    String
CountryCode                      String
CountryPhoneCode                 String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `Cities`

**Lignes source :** 37 | **Colonnes source :** 9 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 2

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 37 lignes — dimension géographique liée à Countries.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| CityName | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CountryId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `Cities.CountryId` → `Countries.Id` (sortante)
- `Companies.CityId` → `Cities.Id` (entrante)
- `EmployeeAddresses.CityId` → `Cities.Id` (entrante)

**Schéma final `payzenhr_staging.Cities`** (13 colonnes) :

```
Id                               Int64
CityName                         String
CountryId                        Nullable(Int64)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `Genders`

**Lignes source :** 2 | **Colonnes source :** 12 | **Doublons détectés :** 0 | **FK sortantes :** 0 | **FK entrantes (référencée par) :** 3

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 2 lignes — lookup démographique.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Code | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameFr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameAr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameEn | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| IsActive | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `EmployeeChildren.GenderId` → `Genders.Id` (entrante)
- `EmployeeSpouses.GenderId` → `Genders.Id` (entrante)
- `Employees.GenderId` → `Genders.Id` (entrante)

**Schéma final `payzenhr_staging.Genders`** (16 colonnes) :

```
Id                               Int64
Code                             String
NameFr                           String
NameAr                           String
NameEn                           String
IsActive                         UInt8 (Bool)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `Statuses`

**Lignes source :** 5 | **Colonnes source :** 15 | **Doublons détectés :** 0 | **FK sortantes :** 0 | **FK entrantes (référencée par) :** 1

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 5 lignes — statuts employé (actif/inactif/…).


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Code | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameFr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameAr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameEn | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| IsActive | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| AffectsAccess | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| AffectsPayroll | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| AffectsAttendance | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `Employees.StatusId` → `Statuses.Id` (entrante)

**Schéma final `payzenhr_staging.Statuses`** (19 colonnes) :

```
Id                               Int64
Code                             String
NameFr                           String
NameAr                           String
NameEn                           String
IsActive                         UInt8 (Bool)
AffectsAccess                    UInt8 (Bool)
AffectsPayroll                   UInt8 (Bool)
AffectsAttendance                UInt8 (Bool)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `EducationLevels`

**Lignes source :** 11 | **Colonnes source :** 13 | **Doublons détectés :** 0 | **FK sortantes :** 0 | **FK entrantes (référencée par) :** 1

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 11 lignes — lookup démographique.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Code | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameFr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameAr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameEn | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| LevelOrder | `IntegerType()` | Conserver | Int32 |  [G-] |
| IsActive | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `Employees.EducationLevelId` → `EducationLevels.Id` (entrante)

**Schéma final `payzenhr_staging.EducationLevels`** (17 colonnes) :

```
Id                               Int64
Code                             String
NameFr                           String
NameAr                           String
NameEn                           String
LevelOrder                       Int32
IsActive                         UInt8 (Bool)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `MaritalStatuses`

**Lignes source :** 4 | **Colonnes source :** 11 | **Doublons détectés :** 0 | **FK sortantes :** 0 | **FK entrantes (référencée par) :** 1

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 4 lignes — lookup démographique.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Code | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameFr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameAr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameEn | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `Employees.MaritalStatusId` → `MaritalStatuses.Id` (entrante)

**Schéma final `payzenhr_staging.MaritalStatuses`** (15 colonnes) :

```
Id                               Int64
Code                             String
NameFr                           String
NameAr                           String
NameEn                           String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `Nationalities`

**Lignes source :** 5 | **Colonnes source :** 8 | **Doublons détectés :** 0 | **FK sortantes :** 0 | **FK entrantes (référencée par) :** 1

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 5 lignes — lookup démographique.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Name | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `Employees.NationalityId` → `Nationalities.Id` (entrante)

**Schéma final `payzenhr_staging.Nationalities`** (12 colonnes) :

```
Id                               Int64
Name                             String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `LegalContractTypes`

**Lignes source :** 5 | **Colonnes source :** 9 | **Doublons détectés :** 0 | **FK sortantes :** 0 | **FK entrantes (référencée par) :** 1

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 5 lignes — référentiel légal (types de contrat marocains : CDI, CDD, ANAPEC…).


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Code | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Name | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `ContractTypes.LegalContractTypeId` → `LegalContractTypes.Id` (entrante)

**Schéma final `payzenhr_staging.LegalContractTypes`** (13 colonnes) :

```
Id                               Int64
Code                             String
Name                             String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `StateEmploymentPrograms`

**Lignes source :** 3 | **Colonnes source :** 14 | **Doublons détectés :** 0 | **FK sortantes :** 0 | **FK entrantes (référencée par) :** 1

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 3 lignes. MaxDurationMonths/SalaryCeiling 33% NULL — programmes d'aide à l'emploi sans plafond défini (cohérent, tous les programmes n'ont pas de plafond légal).


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Code | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Name | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| IsIrExempt | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| IsCnssEmployeeExempt | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| IsCnssEmployerExempt | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| MaxDurationMonths | `IntegerType()` | Conserver | Int32 |  (NULL=33.3%) [G-] |
| SalaryCeiling | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=33.3%) [G7] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `ContractTypes.StateEmploymentProgramId` → `StateEmploymentPrograms.Id` (entrante)

**Schéma final `payzenhr_staging.StateEmploymentPrograms`** (18 colonnes) :

```
Id                               Int64
Code                             String
Name                             String
IsIrExempt                       UInt8 (Bool)
IsCnssEmployeeExempt             UInt8 (Bool)
IsCnssEmployerExempt             UInt8 (Bool)
MaxDurationMonths                Int32
SalaryCeiling                    Decimal(18,2)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `OvertimeRateRules`

**Lignes source :** 6 | **Colonnes source :** 30 | **Doublons détectés :** 0 | **FK sortantes :** 0 | **FK entrantes (référencée par) :** 1

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 6 lignes. Nombreuses colonnes 100% NULL (StartTime, EndTime, ApplicableDaysOfWeek, EffectiveFrom/To, MinimumDurationHours, MaximumDurationHours, LegalReference, DocumentationUrl) : règles de majoration simples sans restriction horaire/légale documentée sur ce jeu de données — Information insuffisante pour juger si ces champs seront utilisés en production, conservés tels quels.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Code | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameAr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameFr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameEn | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Description | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| AppliesTo | `IntegerType()` | Conserver | Int32 |  [G-] |
| TimeRangeType | `IntegerType()` | Conserver | Int32 |  [G-] |
| StartTime | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). (NULL=100.0%) [G3] |
| EndTime | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). (NULL=100.0%) [G3] |
| ApplicableDaysOfWeek | `IntegerType()` | Conserver | Int32 |  (NULL=100.0%) [G-] |
| Multiplier | `DecimalType(5,2)` | Conserver | Decimal(5,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| CumulationStrategy | `IntegerType()` | Conserver | Int32 |  [G-] |
| Priority | `IntegerType()` | Conserver | Int32 |  [G-] |
| Category | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| IsActive | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| EffectiveFrom | `DateType()` | Conserver + convertir type | Date | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). (NULL=100.0%) [G3] |
| EffectiveTo | `DateType()` | Conserver + convertir type | Date | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). (NULL=100.0%) [G3] |
| MinimumDurationHours | `DecimalType(5,2)` | Conserver | Decimal(5,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=100.0%) [G7] |
| MaximumDurationHours | `DecimalType(5,2)` | Conserver | Decimal(5,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=100.0%) [G7] |
| RequiresSuperiorApproval | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| LegalReference | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| DocumentationUrl | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| RowVersion | `BinaryType()` | Exclure | — | Colonne binaire technique (ex. RowVersion) sans valeur analytique — exclue du staging. [T2] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `EmployeeOvertimes.RateRuleId` → `OvertimeRateRules.Id` (entrante)

**Schéma final `payzenhr_staging.OvertimeRateRules`** (33 colonnes) :

```
Id                               Int64
Code                             String
NameAr                           String
NameFr                           String
NameEn                           String
Description                      String
AppliesTo                        Int32
TimeRangeType                    Int32
StartTime                        DateTime64(3)
EndTime                          DateTime64(3)
ApplicableDaysOfWeek             Int32
Multiplier                       Decimal(5,2)
CumulationStrategy               Int32
Priority                         Int32
Category                         String
IsActive                         UInt8 (Bool)
EffectiveFrom                    Date
EffectiveTo                      Date
MinimumDurationHours             Decimal(5,2)
MaximumDurationHours             Decimal(5,2)
RequiresSuperiorApproval         UInt8 (Bool)
LegalReference                   String
DocumentationUrl                 String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---


## Module 4 — Employee (RH, Contrats, Salaires, Pointage)

### `Employees`

**Lignes source :** 1561 | **Colonnes source :** 44 | **Doublons détectés :** 0 | **FK sortantes :** 11 | **FK entrantes (référencée par) :** 24

**Décision table : 🟡 TRANSFORMÉE**


**Pourquoi :** Table maîtresse RH (1561 lignes, 44 colonnes) — cœur du modèle. Plusieurs colonnes à NULL très élevé documentées ci-dessous ; aucune n'est supprimée sans certitude métier (risque fort en table pivot).


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Matricule | `IntegerType()` | Conserver | Int32 | 16.08% NULL — normaliser en String (matricule peut comporter des zéros non significatifs), ne pas caster en Int. (NULL=16.1%) [G-] |
| FirstName | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| LastName | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CinNumber | `StringType()` | Conserver + nettoyer | String | Donnée personnelle sensible (CIN marocaine) — nettoyage espace/majuscule, PAS de suppression (obligatoire légalement pour la paie/CNSS). Accès restreint en aval (T10). [G5] |
| DateOfBirth | `DateType()` | Conserver + convertir type | Date | Donnée personnelle sensible — conservée + calcul dérivé AgeYears (T8, snapshot à ETLExecutionDate). [G3] |
| Phone | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Email | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| PersonalEmail | `StringType()` | Conserver + nettoyer | String | 100% NULL — conservée (G9), champ potentiellement utilisé en production hors du jeu de test actuel. (NULL=100.0%) [G5] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| ManagerId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | 100% NULL sur le jeu de test — la hiérarchie managériale n'est pas encore renseignée. Conservée en Nullable(Int64), auto-FK vers Employees.Id, validée par détection de cycles. (NULL=100.0%) [G2] |
| DepartementId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | 90.52% NULL — la majorité des employés ne sont pas rattachés à un département dans ce jeu de test. Conservée (SetNull côté source), signalé comme point de vigilance qualité amont (saisie applicative incomplète). (NULL=90.5%) [G2] |
| StatusId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| GenderId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). (NULL=6.7%) [G2] |
| NationalityId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). (NULL=7.9%) [G2] |
| EducationLevelId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). (NULL=17.0%) [G2] |
| MaritalStatusId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). (NULL=85.3%) [G2] |
| CnssNumber | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=84.9%) [G5] |
| CimrNumber | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=99.0%) [G5] |
| CimrEmployeeRate | `DecimalType(5,4)` | Conserver | Decimal(5,4) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=99.2%) [G7] |
| CimrCompanyRate | `DecimalType(5,4)` | Conserver | Decimal(5,4) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=99.6%) [G7] |
| HasPrivateInsurance | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| PrivateInsuranceNumber | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| PrivateInsuranceRate | `DecimalType(5,4)` | Conserver | Decimal(5,4) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=100.0%) [G7] |
| DisableAmo | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen — conserver tel quel, NULL non remplacé sans confirmation métier. [G6] |
| PaymentMethod | `StringType()` | Conserver + nettoyer | String | 100% NULL sur ce jeu — conservée (G9). (NULL=100.0%) [G5] |
| CategoryId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). (NULL=99.7%) [G2] |
| CountryId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | 100% NULL — colonne shadow EF (mentionnée dans le schéma comme génération shadow property liée à Country.Employees). Confirmé par la documentation technique fournie. Conservée mais signalée comme candidate à la suppression si confirmé définitivement inutilisée (Information insuffisante pour trancher définitivement — nécessite validation avec l'équipe applicative). (NULL=100.0%) [G2] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=28.3%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=96.7%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |
| AnnualLeaveOpeningDays | `DecimalType(10,2)` | Conserver | Decimal(10,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| AnnualLeaveOpeningEffectiveFrom | `DateType()` | Conserver + convertir type | Date | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). (NULL=90.6%) [G3] |
| CategoryChangeDate | `DateType()` | Conserver + convertir type | Date | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). (NULL=99.9%) [G3] |
| CimrRatesChangeDate | `DateType()` | Conserver + convertir type | Date | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). (NULL=90.4%) [G3] |
| ManagerChangeDate | `DateType()` | Conserver + convertir type | Date | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). (NULL=100.0%) [G3] |
| MaritalStatusChangeDate | `DateType()` | Conserver + convertir type | Date | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). (NULL=99.0%) [G3] |
| PrivateInsuranceChangeDate | `DateType()` | Conserver + convertir type | Date | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). (NULL=100.0%) [G3] |
| RibNumber | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=99.1%) [G5] |
| BankAccountId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). (NULL=100.0%) [G2] |
| HousingDeduction | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant — conserver, NULL ≠ 0 (G7). [G7] |

**Colonnes calculées ajoutées :**

| Colonne | Type | Calcul |
|---|---|---|
| AgeYears | Int32 (Nullable) | DATEDIFF(year, DateOfBirth, ETLExecutionDate) ajusté au jour anniversaire. ⚠️ Snapshot à la date d'exécution ETL — pour un âge « à la seconde », recalculer en vue DWH/BI avec today(), ne pas se fier à une valeur figée en staging pour des besoins temps réel. |
| IsActiveEmployee | UInt8 (Bool) | Dérivé de DeletedAt IS NULL (soft-delete) — à croiser avec le futur statut de contrat actif (EmployeeContracts) plutôt qu'utilisé seul. |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Transformations ETL spécifiques :**

- Validation des 11 FK sortantes (CompanyId, ManagerId, DepartementId, StatusId, GenderId, NationalityId, EducationLevelId, MaritalStatusId, CategoryId, BankAccountId + shadow CountryId).
- CompanyId doit être NOT NULL en pratique (rattachement obligatoire) — toute ligne orpheline ou CompanyId NULL est rejetée en quarantaine, pas de valeur par défaut.
- Nettoyage des espaces/casse sur FirstName, LastName (Proper Case), CinNumber (Upper Case).

**Relations (validation FK à l'ETL) :**
- `Employees.CompanyId` → `Companies.Id` (sortante)
- `Employees.ManagerId` → `Employees.Id` (sortante)
- `Employees.DepartementId` → `Departements.Id` (sortante)
- `Employees.StatusId` → `Statuses.Id` (sortante)
- `Employees.GenderId` → `Genders.Id` (sortante)
- `Employees.NationalityId` → `Nationalities.Id` (sortante)
- `Employees.EducationLevelId` → `EducationLevels.Id` (sortante)
- `Employees.MaritalStatusId` → `MaritalStatuses.Id` (sortante)
- `Employees.CategoryId` → `EmployeeCategories.Id` (sortante)
- `Employees.CountryId` → `Countries.Id` (sortante)
- `Employees.BankAccountId` → `BankAccounts.Id` (sortante)
- `EmployeeAttendances.EmployeeId` → `Employees.Id` (entrante)
- `EmployeeBankAccountAllocations.EmployeeId` → `Employees.Id` (entrante)
- `EmployeeChildren.EmployeeId` → `Employees.Id` (entrante)
- `EmployeeContracts.EmployeeId` → `Employees.Id` (entrante)
- `EmployeeDocuments.EmployeeId` → `Employees.Id` (entrante)
- `EmployeeOvertimes.EmployeeId` → `Employees.Id` (entrante)
- `EmployeeSpouses.EmployeeId` → `Employees.Id` (entrante)
- `LeaveBalances.EmployeeId` → `Employees.Id` (entrante)
- `LeaveCarryOverAgreements.EmployeeId` → `Employees.Id` (entrante)
- `PayrollResults.EmployeeId` → `Employees.Id` (entrante)
- `Users.EmployeeId` → `Employees.Id` (entrante)
- `LeaveRequests.EmployeeId` → `Employees.Id` (entrante)
- `EmployeeSalaries.EmployeeId` → `Employees.Id` (entrante)
- `Departements.ManagerId` → `Employees.Id` (entrante)
- `LeaveAuditLogs.EmployeeId` → `Employees.Id` (entrante)
- `EmployeeMonthlyAbsenceSummaries.EmployeeId` → `Employees.Id` (entrante)
- `SalaryPackageAssignments.EmployeeId` → `Employees.Id` (entrante)
- `EmployeeMonthlyLeaveSummaries.EmployeeId` → `Employees.Id` (entrante)
- `BankAccounts.EmployeeId` → `Employees.Id` (entrante)
- `Employees.ManagerId` → `Employees.Id` (entrante)
- `EmployeeMonthlyOvertimeSummaries.EmployeeId` → `Employees.Id` (entrante)
- `EmployeeMonthlyWorkedDaysSummaries.EmployeeId` → `Employees.Id` (entrante)
- `EmployeeAbsences.EmployeeId` → `Employees.Id` (entrante)
- `EmployeeAddresses.EmployeeId` → `Employees.Id` (entrante)

**Schéma final `payzenhr_staging.Employees`** (50 colonnes) :

```
Id                               Int64
Matricule                        Int32
FirstName                        String
LastName                         String
CinNumber                        String
DateOfBirth                      Date
Phone                            String
Email                            String
PersonalEmail                    String
CompanyId                        Nullable(Int64)
ManagerId                        Nullable(Int64)
DepartementId                    Nullable(Int64)
StatusId                         Nullable(Int64)
GenderId                         Nullable(Int64)
NationalityId                    Nullable(Int64)
EducationLevelId                 Nullable(Int64)
MaritalStatusId                  Nullable(Int64)
CnssNumber                       String
CimrNumber                       String
CimrEmployeeRate                 Decimal(5,4)
CimrCompanyRate                  Decimal(5,4)
HasPrivateInsurance              UInt8 (Bool)
PrivateInsuranceNumber           String
PrivateInsuranceRate             Decimal(5,4)
DisableAmo                       UInt8 (Bool)
PaymentMethod                    String
CategoryId                       Nullable(Int64)
CountryId                        Nullable(Int64)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
AnnualLeaveOpeningDays           Decimal(10,2)
AnnualLeaveOpeningEffectiveFrom  Date
CategoryChangeDate               Date
CimrRatesChangeDate              Date
ManagerChangeDate                Date
MaritalStatusChangeDate          Date
PrivateInsuranceChangeDate       Date
RibNumber                        String
BankAccountId                    Nullable(Int64)
HousingDeduction                 Decimal(18,2)
AgeYears                         Int32 (Nullable)
IsActiveEmployee                 UInt8 (Bool)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `EmployeeCategories`

**Lignes source :** 86 | **Colonnes source :** 11 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 1

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 86 lignes — dimension de catégorie employé par société.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| Name | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Mode | `IntegerType()` | Conserver | Int32 |  [G-] |
| PayrollPeriodicity | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=97.7%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=97.7%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `EmployeeCategories.CompanyId` → `Companies.Id` (sortante)
- `Employees.CategoryId` → `EmployeeCategories.Id` (entrante)

**Schéma final `payzenhr_staging.EmployeeCategories`** (15 colonnes) :

```
Id                               Int64
CompanyId                        Nullable(Int64)
Name                             String
Mode                             Int32
PayrollPeriodicity               String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `EmployeeContracts`

**Lignes source :** 1337 | **Colonnes source :** 14 | **Doublons détectés :** 0 | **FK sortantes :** 4 | **FK entrantes (référencée par) :** 2

**Décision table : 🟡 TRANSFORMÉE**


**Pourquoi :** 1337 lignes. EndDate 80.78% NULL = contrats en cours (CDI actifs), cohérent métier — NE PAS remplacer par une date fictive. ExonerationEndDate 100% NULL (exonération fiscale non utilisée sur ce jeu, G9).


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| EmployeeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| JobPositionId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| ContractTypeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| StartDate | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| EndDate | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). (NULL=80.8%) [G3] |
| ExonerationEndDate | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). (NULL=100.0%) [G3] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=98.4%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=98.4%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes calculées ajoutées :**

| Colonne | Type | Calcul |
|---|---|---|
| IsActiveContract | UInt8 (Bool) | EndDate IS NULL OR EndDate > ETLExecutionDate — contrat actif à la date de chargement. |
| SeniorityYears / SeniorityMonths | Decimal(5,2) / Int32 (Nullable) | DATEDIFF(StartDate, COALESCE(EndDate, ETLExecutionDate)) — ancienneté calculée en snapshot ETL, mêmes réserves que AgeYears (à recalculer dynamiquement pour un usage temps réel en BI). |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Transformations ETL spécifiques :**

- Contrôle de cohérence StartDate ≤ EndDate quand EndDate renseignée (ligne en anomalie sinon, loguée mais pas rejetée automatiquement sans confirmation métier).

**Relations (validation FK à l'ETL) :**
- `EmployeeContracts.EmployeeId` → `Employees.Id` (sortante)
- `EmployeeContracts.CompanyId` → `Companies.Id` (sortante)
- `EmployeeContracts.JobPositionId` → `JobPositions.Id` (sortante)
- `EmployeeContracts.ContractTypeId` → `ContractTypes.Id` (sortante)
- `EmployeeSalaries.ContractId` → `EmployeeContracts.Id` (entrante)
- `SalaryPackageAssignments.ContractId` → `EmployeeContracts.Id` (entrante)

**Schéma final `payzenhr_staging.EmployeeContracts`** (20 colonnes) :

```
Id                               Int64
EmployeeId                       Nullable(Int64)
CompanyId                        Nullable(Int64)
JobPositionId                    Nullable(Int64)
ContractTypeId                   Nullable(Int64)
StartDate                        DateTime64(3)
EndDate                          DateTime64(3)
ExonerationEndDate               DateTime64(3)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
IsActiveContract                 UInt8 (Bool)
SeniorityYears / SeniorityMonths Decimal(5,2) / Int32 (Nullable)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `EmployeeSalaries`

**Lignes source :** 1225 | **Colonnes source :** 13 | **Doublons détectés :** 0 | **FK sortantes :** 2 | **FK entrantes (référencée par) :** 2

**Décision table : 🟡 TRANSFORMÉE**


**Pourquoi :** 1225 lignes. BaseSalaryHourly 93.31% NULL = la majorité des salaires sont mensuels et non horaires (cohérent). EndDate 87.10% NULL = ligne de salaire courante (pas de fin), cohérent avec un historique de type SCD (Slowly Changing Dimension) sur le salaire.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| EmployeeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| ContractId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| BaseSalary | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| BaseSalaryHourly | `DecimalType(18,4)` | Conserver | Decimal(18,4) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=93.3%) [G7] |
| EffectiveDate | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| EndDate | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). (NULL=87.1%) [G3] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=98.3%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=98.3%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes calculées ajoutées :**

| Colonne | Type | Calcul |
|---|---|---|
| IsCurrentSalary | UInt8 (Bool) | EndDate IS NULL — dernière ligne de salaire active par employé. |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Transformations ETL spécifiques :**

- Table de type SCD2 implicite (historisation par EffectiveDate/EndDate) — conserver l'historique complet en staging, ne jamais écraser une ligne, l'agrégation « salaire courant » se fait via IsCurrentSalary.

**Relations (validation FK à l'ETL) :**
- `EmployeeSalaries.EmployeeId` → `Employees.Id` (sortante)
- `EmployeeSalaries.ContractId` → `EmployeeContracts.Id` (sortante)
- `EmployeeSalaryComponents.EmployeeSalaryId` → `EmployeeSalaries.Id` (entrante)
- `SalaryPackageAssignments.EmployeeSalaryId` → `EmployeeSalaries.Id` (entrante)

**Schéma final `payzenhr_staging.EmployeeSalaries`** (18 colonnes) :

```
Id                               Int64
EmployeeId                       Nullable(Int64)
ContractId                       Nullable(Int64)
BaseSalary                       Decimal(18,2)
BaseSalaryHourly                 Decimal(18,4)
EffectiveDate                    DateTime64(3)
EndDate                          DateTime64(3)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
IsCurrentSalary                  UInt8 (Bool)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `EmployeeSalaryComponents`

**Lignes source :** 1112 | **Colonnes source :** 16 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 0

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 1112 lignes. EndDate 99.37% NULL, Percentage 99.37% NULL (composants à montant fixe majoritaires vs. composants en %, cohérent), UpdatedAt/By et DeletedAt/By suivent le pattern global G3/G4.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| EmployeeSalaryId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| ComponentType | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| IsTaxable | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| IsSocial | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| IsCIMR | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| Amount | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| EffectiveDate | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| EndDate | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). (NULL=99.4%) [G3] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=98.7%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=99.4%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=99.1%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=99.1%) [G4] |
| Percentage | `DecimalType(5,2)` | Conserver | Decimal(5,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=99.4%) [G7] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `EmployeeSalaryComponents.EmployeeSalaryId` → `EmployeeSalaries.Id` (sortante)

**Schéma final `payzenhr_staging.EmployeeSalaryComponents`** (20 colonnes) :

```
Id                               Int64
EmployeeSalaryId                 Nullable(Int64)
ComponentType                    String
IsTaxable                        UInt8 (Bool)
IsSocial                         UInt8 (Bool)
IsCIMR                           UInt8 (Bool)
Amount                           Decimal(18,2)
EffectiveDate                    DateTime64(3)
EndDate                          DateTime64(3)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
Percentage                       Decimal(5,2)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `EmployeeAddresses`

**Lignes source :** 1213 | **Colonnes source :** 13 | **Doublons détectés :** 0 | **FK sortantes :** 3 | **FK entrantes (référencée par) :** 0

**Décision table : 🟡 TRANSFORMÉE**


**Pourquoi :** 1213 lignes. AddressLine2 100% NULL (complément d'adresse rarement utilisé, cohérent), CountryId 100% NULL — même remarque que Employees.CountryId (colonne shadow EF potentiellement redondante avec la relation City → Country), signalée mais non supprimée (Information insuffisante pour confirmer).


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| EmployeeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| AddressLine1 | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| AddressLine2 | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| ZipCode | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CityId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| CountryId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). (NULL=100.0%) [G2] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=99.2%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=99.2%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `EmployeeAddresses.EmployeeId` → `Employees.Id` (sortante)
- `EmployeeAddresses.CityId` → `Cities.Id` (sortante)
- `EmployeeAddresses.CountryId` → `Countries.Id` (sortante)

**Schéma final `payzenhr_staging.EmployeeAddresses`** (17 colonnes) :

```
Id                               Int64
EmployeeId                       Nullable(Int64)
AddressLine1                     String
AddressLine2                     String
ZipCode                          String
CityId                           Nullable(Int64)
CountryId                        Nullable(Int64)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `EmployeeDocuments`

**Lignes source :** 0 | **Colonnes source :** 12 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne — GED employé non utilisée sur ce jeu de test. Structure prête (FilePath, DocumentType, ExpirationDate). Information insuffisante pour profiler la qualité.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| EmployeeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| Name | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| FilePath | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| ExpirationDate | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| DocumentType | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `EmployeeDocuments.EmployeeId` → `Employees.Id` (sortante)

**Schéma final `payzenhr_staging.EmployeeDocuments`** (16 colonnes) :

```
Id                               Int64
EmployeeId                       Nullable(Int64)
Name                             String
FilePath                         String
ExpirationDate                   DateTime64(3)
DocumentType                     String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `EmployeeAttendances`

**Lignes source :** 0 | **Colonnes source :** 15 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 1

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne — module de pointage non activé sur ce tenant de test. Information insuffisante pour juger qualité/formats de CheckIn/CheckOut.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| EmployeeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| WorkDate | `DateType()` | Conserver + convertir type | Date | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| CheckIn | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| CheckOut | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| BreakMinutesApplied | `IntegerType()` | Conserver | Int32 |  [G-] |
| Status | `IntegerType()` | Conserver | Int32 |  [G-] |
| Source | `IntegerType()` | Conserver | Int32 |  [G-] |
| WorkedHours | `DecimalType(5,2)` | Conserver | Decimal(5,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `EmployeeAttendances.EmployeeId` → `Employees.Id` (sortante)
- `EmployeeAttendanceBreaks.EmployeeAttendanceId` → `EmployeeAttendances.Id` (entrante)

**Schéma final `payzenhr_staging.EmployeeAttendances`** (19 colonnes) :

```
Id                               Int64
EmployeeId                       Nullable(Int64)
WorkDate                         Date
CheckIn                          DateTime64(3)
CheckOut                         DateTime64(3)
BreakMinutesApplied              Int32
Status                           Int32
Source                           Int32
WorkedHours                      Decimal(5,2)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `EmployeeAttendanceBreaks`

**Lignes source :** 0 | **Colonnes source :** 11 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne, dépendante de EmployeeAttendances (également vide).


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| EmployeeAttendanceId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| BreakStart | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| BreakEnd | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| BreakType | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `EmployeeAttendanceBreaks.EmployeeAttendanceId` → `EmployeeAttendances.Id` (sortante)

**Schéma final `payzenhr_staging.EmployeeAttendanceBreaks`** (15 colonnes) :

```
Id                               Int64
EmployeeAttendanceId             Nullable(Int64)
BreakStart                       DateTime64(3)
BreakEnd                         DateTime64(3)
BreakType                        String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `EmployeeAbsences`

**Lignes source :** 2 | **Colonnes source :** 19 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 1

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 2 lignes seulement — volumétrie trop faible pour un profiling statistique fiable, mais données présentes et structure exploitable. IsMorning/StartTime/EndTime/Reason 50-100% NULL cohérent avec des absences à la journée entière (pas de demi-journée).


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| EmployeeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| AbsenceDate | `DateType()` | Conserver + convertir type | Date | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| DurationType | `IntegerType()` | Conserver | Int32 |  [G-] |
| Status | `IntegerType()` | Conserver | Int32 |  [G-] |
| DecisionAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=50.0%) [G3] |
| DecisionBy | `IntegerType()` | Conserver | Int32 |  (NULL=50.0%) [G-] |
| DecisionComment | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| IsMorning | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. (NULL=100.0%) [G6] |
| StartTime | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). (NULL=100.0%) [G3] |
| EndTime | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). (NULL=100.0%) [G3] |
| AbsenceType | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Reason | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=50.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `EmployeeAbsences.EmployeeId` → `Employees.Id` (sortante)
- `LeaveRequestExemptions.EmployeeAbsenceId` → `EmployeeAbsences.Id` (entrante)

**Schéma final `payzenhr_staging.EmployeeAbsences`** (23 colonnes) :

```
Id                               Int64
EmployeeId                       Nullable(Int64)
AbsenceDate                      Date
DurationType                     Int32
Status                           Int32
DecisionAt                       Nullable(DateTime64(3))
DecisionBy                       Int32
DecisionComment                  String
IsMorning                        UInt8 (Bool)
StartTime                        DateTime64(3)
EndTime                          DateTime64(3)
AbsenceType                      String
Reason                           String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `EmployeeChildren`

**Lignes source :** 2 | **Colonnes source :** 14 | **Doublons détectés :** 0 | **FK sortantes :** 2 | **FK entrantes (référencée par) :** 0

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 2 lignes. GenderId 50% NULL — genre de l'enfant non toujours renseigné, conservé tel quel.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| EmployeeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| FirstName | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| LastName | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| DateOfBirth | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| GenderId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). (NULL=50.0%) [G2] |
| IsDependent | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| IsStudent | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `EmployeeChildren.EmployeeId` → `Employees.Id` (sortante)
- `EmployeeChildren.GenderId` → `Genders.Id` (sortante)

**Schéma final `payzenhr_staging.EmployeeChildren`** (18 colonnes) :

```
Id                               Int64
EmployeeId                       Nullable(Int64)
FirstName                        String
LastName                         String
DateOfBirth                      DateTime64(3)
GenderId                         Nullable(Int64)
IsDependent                      UInt8 (Bool)
IsStudent                        UInt8 (Bool)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `EmployeeSpouses`

**Lignes source :** 0 | **Colonnes source :** 14 | **Doublons détectés :** 0 | **FK sortantes :** 2 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| EmployeeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| FirstName | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| LastName | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| DateOfBirth | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| GenderId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| CinNumber | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| IsDependent | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `EmployeeSpouses.EmployeeId` → `Employees.Id` (sortante)
- `EmployeeSpouses.GenderId` → `Genders.Id` (sortante)

**Schéma final `payzenhr_staging.EmployeeSpouses`** (18 colonnes) :

```
Id                               Int64
EmployeeId                       Nullable(Int64)
FirstName                        String
LastName                         String
DateOfBirth                      DateTime64(3)
GenderId                         Nullable(Int64)
CinNumber                        String
IsDependent                      UInt8 (Bool)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `EmployeeOvertimes`

**Lignes source :** 0 | **Colonnes source :** 34 | **Doublons détectés :** 0 | **FK sortantes :** 3 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne — module heures supplémentaires non utilisé sur ce jeu, alors que PayrollResults contient déjà des colonnes HeuresSupp25/50/100 calculées : à surveiller, risque de double source de vérité entre EmployeeOvertimes (saisie) et PayrollResults (résultat calculé) — Information insuffisante pour trancher du flux exact, à clarifier avec l'équipe applicative.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| EmployeeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| OverTimeType | `IntegerType()` | Conserver | Int32 |  [G-] |
| EntryMode | `IntegerType()` | Conserver | Int32 |  [G-] |
| HolidayId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| OvertimeDate | `DateType()` | Conserver + convertir type | Date | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| StartTime | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| EndTime | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| CrossesMidnight | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| DurationInHours | `DecimalType(5,2)` | Conserver | Decimal(5,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| StandardDayHours | `DecimalType(5,2)` | Conserver | Decimal(5,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| RateRuleId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| RateRuleCodeApplied | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| RateRuleNameApplied | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| RateMultiplierApplied | `DecimalType(5,2)` | Conserver | Decimal(5,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| MultiplierCalculationDetails | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| SplitBatchId | `StringType()` | Conserver + nettoyer | String | Identifiant technique de type texte (corrélation externe / référence tierce), pas une FK relationnelle interne. [G5] |
| SplitSequence | `IntegerType()` | Conserver | Int32 |  [G-] |
| SplitTotalSegments | `IntegerType()` | Conserver | Int32 |  [G-] |
| Status | `IntegerType()` | Conserver | Int32 |  [G-] |
| EmployeeComment | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| ManagerComment | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| ApprovedBy | `IntegerType()` | Conserver | Int32 |  [G-] |
| ApprovedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| IsProcessedInPayroll | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| PayrollBatchId | `IntegerType()` | Conserver (FK implicite non contrainte) | Nullable(Int64) | Suffixe Id sans contrainte FK EF déclarée — conservée en l'état, validée si référence identifiée. [G2] |
| ProcessedInPayrollAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| RowVersion | `BinaryType()` | Exclure | — | Colonne binaire technique (ex. RowVersion) sans valeur analytique — exclue du staging. [T2] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `EmployeeOvertimes.EmployeeId` → `Employees.Id` (sortante)
- `EmployeeOvertimes.HolidayId` → `Holidays.Id` (sortante)
- `EmployeeOvertimes.RateRuleId` → `OvertimeRateRules.Id` (sortante)

**Schéma final `payzenhr_staging.EmployeeOvertimes`** (37 colonnes) :

```
Id                               Int64
EmployeeId                       Nullable(Int64)
OverTimeType                     Int32
EntryMode                        Int32
HolidayId                        Nullable(Int64)
OvertimeDate                     Date
StartTime                        DateTime64(3)
EndTime                          DateTime64(3)
CrossesMidnight                  UInt8 (Bool)
DurationInHours                  Decimal(5,2)
StandardDayHours                 Decimal(5,2)
RateRuleId                       Nullable(Int64)
RateRuleCodeApplied              String
RateRuleNameApplied              String
RateMultiplierApplied            Decimal(5,2)
MultiplierCalculationDetails     String
SplitBatchId                     String
SplitSequence                    Int32
SplitTotalSegments               Int32
Status                           Int32
EmployeeComment                  String
ManagerComment                   String
ApprovedBy                       Int32
ApprovedAt                       Nullable(DateTime64(3))
IsProcessedInPayroll             UInt8 (Bool)
PayrollBatchId                   Nullable(Int64)
ProcessedInPayrollAt             Nullable(DateTime64(3))
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `EmployeeMonthlyAbsenceSummaries`

**Lignes source :** 150 | **Colonnes source :** 12 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 0

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 150 lignes — table d'agrégats mensuels pré-calculés côté source (unique EmployeeId+Year+Month). Conservée telle quelle : c'est déjà une table de faits agrégée prête pour le DWH, aucune transformation lourde requise.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| EmployeeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| Year | `IntegerType()` | Conserver | Int32 |  [G-] |
| Month | `IntegerType()` | Conserver | Int32 |  [G-] |
| AbsenceDays | `DecimalType(10,2)` | Conserver | Decimal(10,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| ImportReference | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=0.7%) [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `EmployeeMonthlyAbsenceSummaries.EmployeeId` → `Employees.Id` (sortante)

**Schéma final `payzenhr_staging.EmployeeMonthlyAbsenceSummaries`** (16 colonnes) :

```
Id                               Int64
EmployeeId                       Nullable(Int64)
Year                             Int32
Month                            Int32
AbsenceDays                      Decimal(10,2)
ImportReference                  String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `EmployeeMonthlyLeaveSummaries`

**Lignes source :** 150 | **Colonnes source :** 12 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 0

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 150 lignes — même logique que EmployeeMonthlyAbsenceSummaries.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| EmployeeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| Year | `IntegerType()` | Conserver | Int32 |  [G-] |
| Month | `IntegerType()` | Conserver | Int32 |  [G-] |
| LeaveDays | `DecimalType(10,2)` | Conserver | Decimal(10,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| ImportReference | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=1.3%) [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `EmployeeMonthlyLeaveSummaries.EmployeeId` → `Employees.Id` (sortante)

**Schéma final `payzenhr_staging.EmployeeMonthlyLeaveSummaries`** (16 colonnes) :

```
Id                               Int64
EmployeeId                       Nullable(Int64)
Year                             Int32
Month                            Int32
LeaveDays                        Decimal(10,2)
ImportReference                  String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `EmployeeMonthlyOvertimeSummaries`

**Lignes source :** 4 | **Colonnes source :** 14 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 0

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 4 lignes seulement — cohérent avec EmployeeOvertimes vide (peu de données sources).


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| EmployeeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| Year | `IntegerType()` | Conserver | Int32 |  [G-] |
| Month | `IntegerType()` | Conserver | Int32 |  [G-] |
| Hours25Pct | `DecimalType(10,2)` | Conserver | Decimal(10,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| Hours50Pct | `DecimalType(10,2)` | Conserver | Decimal(10,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| Hours100Pct | `DecimalType(10,2)` | Conserver | Decimal(10,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| ImportReference | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `EmployeeMonthlyOvertimeSummaries.EmployeeId` → `Employees.Id` (sortante)

**Schéma final `payzenhr_staging.EmployeeMonthlyOvertimeSummaries`** (18 colonnes) :

```
Id                               Int64
EmployeeId                       Nullable(Int64)
Year                             Int32
Month                            Int32
Hours25Pct                       Decimal(10,2)
Hours50Pct                       Decimal(10,2)
Hours100Pct                      Decimal(10,2)
ImportReference                  String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `EmployeeMonthlyWorkedDaysSummaries`

**Lignes source :** 26 | **Colonnes source :** 12 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 0

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 26 lignes — agrégat mensuel des jours travaillés.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| EmployeeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| Year | `IntegerType()` | Conserver | Int32 |  [G-] |
| Month | `IntegerType()` | Conserver | Int32 |  [G-] |
| WorkedDays | `DecimalType(10,2)` | Conserver | Decimal(10,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| ImportReference | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `EmployeeMonthlyWorkedDaysSummaries.EmployeeId` → `Employees.Id` (sortante)

**Schéma final `payzenhr_staging.EmployeeMonthlyWorkedDaysSummaries`** (16 colonnes) :

```
Id                               Int64
EmployeeId                       Nullable(Int64)
Year                             Int32
Month                            Int32
WorkedDays                       Decimal(10,2)
ImportReference                  String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---


## Module 5 — Leave (Congés)

### `LeaveTypes`

**Lignes source :** 7 | **Colonnes source :** 15 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 5

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 7 lignes. CompanyId 100% NULL = types de congés globaux (non spécifiques à une société), cohérent avec FK nullable.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| LeaveCode | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| LeaveNameAr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| LeaveNameEn | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| LeaveNameFr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| LeaveDescription | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Scope | `IntegerType()` | Conserver | Int32 |  [G-] |
| IsActive | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). (NULL=100.0%) [G2] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `LeaveTypes.CompanyId` → `Companies.Id` (sortante)
- `LeaveBalances.LeaveTypeId` → `LeaveTypes.Id` (entrante)
- `LeaveCarryOverAgreements.LeaveTypeId` → `LeaveTypes.Id` (entrante)
- `LeaveRequests.LeaveTypeId` → `LeaveTypes.Id` (entrante)
- `LeaveTypeLegalRules.LeaveTypeId` → `LeaveTypes.Id` (entrante)
- `LeaveTypePolicies.LeaveTypeId` → `LeaveTypes.Id` (entrante)

**Schéma final `payzenhr_staging.LeaveTypes`** (19 colonnes) :

```
Id                               Int64
LeaveCode                        String
LeaveNameAr                      String
LeaveNameEn                      String
LeaveNameFr                      String
LeaveDescription                 String
Scope                            Int32
IsActive                         UInt8 (Bool)
CompanyId                        Nullable(Int64)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `LeaveTypePolicies`

**Lignes source :** 6 | **Colonnes source :** 25 | **Doublons détectés :** 0 | **FK sortantes :** 2 | **FK entrantes (référencée par) :** 1

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 6 lignes. CompanyId, EffectiveFrom/To 100% NULL — politiques génériques par défaut, cohérent.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). (NULL=100.0%) [G2] |
| LeaveTypeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| IsEnabled | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| RequiresBalance | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| AllowNegativeBalance | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| IsPaid | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| RequiresEligibility6Months | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| AccrualMethod | `IntegerType()` | Conserver | Int32 |  [G-] |
| DaysPerMonthAdult | `DecimalType(5,2)` | Conserver | Decimal(5,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| DaysPerMonthMinor | `DecimalType(5,2)` | Conserver | Decimal(5,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| BonusDaysPerYearAfter5Years | `DecimalType(5,2)` | Conserver | Decimal(5,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| AnnualCapDays | `IntegerType()` | Conserver | Int32 |  [G-] |
| AllowCarryover | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| MaxCarryoverYears | `IntegerType()` | Conserver | Int32 |  [G-] |
| MinConsecutiveDays | `IntegerType()` | Conserver | Int32 |  [G-] |
| UseWorkingCalendar | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| EffectiveFrom | `DateType()` | Conserver + convertir type | Date | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). (NULL=100.0%) [G3] |
| EffectiveTo | `DateType()` | Conserver + convertir type | Date | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). (NULL=100.0%) [G3] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `LeaveTypePolicies.CompanyId` → `Companies.Id` (sortante)
- `LeaveTypePolicies.LeaveTypeId` → `LeaveTypes.Id` (sortante)
- `LeaveRequests.PolicyId` → `LeaveTypePolicies.Id` (entrante)

**Schéma final `payzenhr_staging.LeaveTypePolicies`** (29 colonnes) :

```
Id                               Int64
CompanyId                        Nullable(Int64)
LeaveTypeId                      Nullable(Int64)
IsEnabled                        UInt8 (Bool)
RequiresBalance                  UInt8 (Bool)
AllowNegativeBalance             UInt8 (Bool)
IsPaid                           UInt8 (Bool)
RequiresEligibility6Months       UInt8 (Bool)
AccrualMethod                    Int32
DaysPerMonthAdult                Decimal(5,2)
DaysPerMonthMinor                Decimal(5,2)
BonusDaysPerYearAfter5Years      Decimal(5,2)
AnnualCapDays                    Int32
AllowCarryover                   UInt8 (Bool)
MaxCarryoverYears                Int32
MinConsecutiveDays               Int32
UseWorkingCalendar               UInt8 (Bool)
EffectiveFrom                    Date
EffectiveTo                      Date
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `LeaveTypeLegalRules`

**Lignes source :** 10 | **Colonnes source :** 14 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 1

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 10 lignes. MustBeUsedWithinDays 40% NULL = règle de péremption non applicable à tous les types de congé légal (cohérent, ex. congé maladie vs. congé payé).


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| LeaveTypeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| EventCaseCode | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Description | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| DaysGranted | `IntegerType()` | Conserver | Int32 |  [G-] |
| LegalArticle | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CanBeDiscountinuous | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| MustBeUsedWithinDays | `IntegerType()` | Conserver | Int32 |  (NULL=40.0%) [G-] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `LeaveTypeLegalRules.LeaveTypeId` → `LeaveTypes.Id` (sortante)
- `LeaveRequests.LegalRuleId` → `LeaveTypeLegalRules.Id` (entrante)

**Schéma final `payzenhr_staging.LeaveTypeLegalRules`** (18 colonnes) :

```
Id                               Int64
LeaveTypeId                      Nullable(Int64)
EventCaseCode                    String
Description                      String
DaysGranted                      Int32
LegalArticle                     String
CanBeDiscountinuous              UInt8 (Bool)
MustBeUsedWithinDays             Int32
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `LeaveRequests`

**Lignes source :** 2 | **Colonnes source :** 27 | **Doublons détectés :** 0 | **FK sortantes :** 5 | **FK entrantes (référencée par) :** 4

**Décision table : 🟡 TRANSFORMÉE**


**Pourquoi :** Seulement 2 lignes dans ce jeu de test — volumétrie insuffisante pour un profiling statistique robuste (Information insuffisante pour juger de la distribution des statuts/délais). Structure conservée et transformée par cohérence avec le reste du module.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| EmployeeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| LeaveTypeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| LegalRuleId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). (NULL=100.0%) [G2] |
| PolicyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| StartDate | `DateType()` | Conserver + convertir type | Date | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| EndDate | `DateType()` | Conserver + convertir type | Date | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| Status | `IntegerType()` | Conserver | Int32 |  [G-] |
| RequestedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| SubmittedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| DecisionAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| DecisionBy | `IntegerType()` | Conserver | Int32 |  [G-] |
| DecisionComment | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CalendarDays | `IntegerType()` | Conserver | Int32 |  [G-] |
| WorkingDaysDeducted | `DecimalType(10,2)` | Conserver | Decimal(10,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| HasMinConsecutiveBlock | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| ComputationVersion | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| EmployeeNote | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| ManagerNote | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| IsRenounced | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes calculées ajoutées :**

| Colonne | Type | Calcul |
|---|---|---|
| LeaveDurationCalendarDays | Int32 | DATEDIFF(EndDate, StartDate) + 1 — recalculé pour contrôle croisé avec CalendarDays source (colonne déjà présente), écart loggé si différent. |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Transformations ETL spécifiques :**

- RequestedAt/SubmittedAt/DecisionAt en StringType() → standardisation date (T1).

**Relations (validation FK à l'ETL) :**
- `LeaveRequests.EmployeeId` → `Employees.Id` (sortante)
- `LeaveRequests.CompanyId` → `Companies.Id` (sortante)
- `LeaveRequests.LeaveTypeId` → `LeaveTypes.Id` (sortante)
- `LeaveRequests.LegalRuleId` → `LeaveTypeLegalRules.Id` (sortante)
- `LeaveRequests.PolicyId` → `LeaveTypePolicies.Id` (sortante)
- `LeaveAuditLogs.LeaveRequestId` → `LeaveRequests.Id` (entrante)
- `LeaveRequestApprovalHistories.LeaveRequestId` → `LeaveRequests.Id` (entrante)
- `LeaveRequestAttachments.LeaveRequestId` → `LeaveRequests.Id` (entrante)
- `LeaveRequestExemptions.LeaveRequestId` → `LeaveRequests.Id` (entrante)

**Schéma final `payzenhr_staging.LeaveRequests`** (32 colonnes) :

```
Id                               Int64
EmployeeId                       Nullable(Int64)
CompanyId                        Nullable(Int64)
LeaveTypeId                      Nullable(Int64)
LegalRuleId                      Nullable(Int64)
PolicyId                         Nullable(Int64)
StartDate                        Date
EndDate                          Date
Status                           Int32
RequestedAt                      Nullable(DateTime64(3))
SubmittedAt                      Nullable(DateTime64(3))
DecisionAt                       Nullable(DateTime64(3))
DecisionBy                       Int32
DecisionComment                  String
CalendarDays                     Int32
WorkingDaysDeducted              Decimal(10,2)
HasMinConsecutiveBlock           UInt8 (Bool)
ComputationVersion               String
EmployeeNote                     String
ManagerNote                      String
IsRenounced                      UInt8 (Bool)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LeaveDurationCalendarDays        Int32
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `LeaveRequestApprovalHistories`

**Lignes source :** 2 | **Colonnes source :** 12 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 0

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 2 lignes, historique d'approbation lié 1:N à LeaveRequests.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| LeaveRequestId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| Action | `IntegerType()` | Conserver | Int32 |  [G-] |
| ActionAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| ActionBy | `IntegerType()` | Conserver | Int32 |  [G-] |
| Comment | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `LeaveRequestApprovalHistories.LeaveRequestId` → `LeaveRequests.Id` (sortante)

**Schéma final `payzenhr_staging.LeaveRequestApprovalHistories`** (16 colonnes) :

```
Id                               Int64
LeaveRequestId                   Nullable(Int64)
Action                           Int32
ActionAt                         Nullable(DateTime64(3))
ActionBy                         Int32
Comment                          String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `LeaveRequestExemptions`

**Lignes source :** 0 | **Colonnes source :** 14 | **Doublons détectés :** 0 | **FK sortantes :** 3 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| LeaveRequestId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| ExemptionDate | `DateType()` | Conserver + convertir type | Date | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| ReasonType | `IntegerType()` | Conserver | Int32 |  [G-] |
| CountsAsLeaveDay | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| HolidayId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| EmployeeAbsenceId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| Note | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `LeaveRequestExemptions.LeaveRequestId` → `LeaveRequests.Id` (sortante)
- `LeaveRequestExemptions.HolidayId` → `Holidays.Id` (sortante)
- `LeaveRequestExemptions.EmployeeAbsenceId` → `EmployeeAbsences.Id` (sortante)

**Schéma final `payzenhr_staging.LeaveRequestExemptions`** (18 colonnes) :

```
Id                               Int64
LeaveRequestId                   Nullable(Int64)
ExemptionDate                    Date
ReasonType                       Int32
CountsAsLeaveDay                 UInt8 (Bool)
HolidayId                        Nullable(Int64)
EmployeeAbsenceId                Nullable(Int64)
Note                             String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `LeaveRequestAttachments`

**Lignes source :** 0 | **Colonnes source :** 11 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne — pièces jointes non utilisées sur ce jeu.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| LeaveRequestId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| FileName | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| FilePath | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| FileType | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `LeaveRequestAttachments.LeaveRequestId` → `LeaveRequests.Id` (sortante)

**Schéma final `payzenhr_staging.LeaveRequestAttachments`** (15 colonnes) :

```
Id                               Int64
LeaveRequestId                   Nullable(Int64)
FileName                         String
FilePath                         String
FileType                         String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `LeaveAuditLogs`

**Lignes source :** 7 | **Colonnes source :** 13 | **Doublons détectés :** 0 | **FK sortantes :** 3 | **FK entrantes (référencée par) :** 0

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 7 lignes. LeaveRequestId 71.43% NULL = évènements d'audit non tous rattachés à une demande précise (ex. audit sur solde), cohérent avec FK SetNull. OldValue 100% NULL vs NewValue (à vérifier, asymétrie à signaler).


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| EmployeeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| LeaveRequestId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). (NULL=71.4%) [G2] |
| EventName | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| OldValue | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| NewValue | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `LeaveAuditLogs.CompanyId` → `Companies.Id` (sortante)
- `LeaveAuditLogs.EmployeeId` → `Employees.Id` (sortante)
- `LeaveAuditLogs.LeaveRequestId` → `LeaveRequests.Id` (sortante)

**Schéma final `payzenhr_staging.LeaveAuditLogs`** (17 colonnes) :

```
Id                               Int64
CompanyId                        Nullable(Int64)
EmployeeId                       Nullable(Int64)
LeaveRequestId                   Nullable(Int64)
EventName                        String
OldValue                         String
NewValue                         String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `LeaveBalances`

**Lignes source :** 109 | **Colonnes source :** 20 | **Doublons détectés :** 0 | **FK sortantes :** 3 | **FK entrantes (référencée par) :** 0

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 109 lignes. CarryoverExpiresOn/LastRecalculatedAt 11.93% NULL = cohérent (pas tous les soldes n'ont un report). DeletedAt/By 100% NULL.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| EmployeeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| LeaveTypeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| Year | `IntegerType()` | Conserver | Int32 |  [G-] |
| Month | `IntegerType()` | Conserver | Int32 |  [G-] |
| OpeningDays | `DecimalType(10,2)` | Conserver | Decimal(10,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| AccruedDays | `DecimalType(10,2)` | Conserver | Decimal(10,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| UsedDays | `DecimalType(10,2)` | Conserver | Decimal(10,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| CarryInDays | `DecimalType(10,2)` | Conserver | Decimal(10,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| CarryOutDays | `DecimalType(10,2)` | Conserver | Decimal(10,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| ClosingDays | `DecimalType(10,2)` | Conserver | Decimal(10,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| CarryoverExpiresOn | `DateType()` | Conserver + convertir type | Date | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). (NULL=11.9%) [G3] |
| LastRecalculatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=11.9%) [G3] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=11.9%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=11.9%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `LeaveBalances.EmployeeId` → `Employees.Id` (sortante)
- `LeaveBalances.CompanyId` → `Companies.Id` (sortante)
- `LeaveBalances.LeaveTypeId` → `LeaveTypes.Id` (sortante)

**Schéma final `payzenhr_staging.LeaveBalances`** (24 colonnes) :

```
Id                               Int64
EmployeeId                       Nullable(Int64)
CompanyId                        Nullable(Int64)
LeaveTypeId                      Nullable(Int64)
Year                             Int32
Month                            Int32
OpeningDays                      Decimal(10,2)
AccruedDays                      Decimal(10,2)
UsedDays                         Decimal(10,2)
CarryInDays                      Decimal(10,2)
CarryOutDays                     Decimal(10,2)
ClosingDays                      Decimal(10,2)
CarryoverExpiresOn               Date
LastRecalculatedAt               Nullable(DateTime64(3))
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `LeaveCarryOverAgreements`

**Lignes source :** 0 | **Colonnes source :** 14 | **Doublons détectés :** 0 | **FK sortantes :** 3 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| EmployeeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| LeaveTypeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| FromYear | `IntegerType()` | Conserver | Int32 |  [G-] |
| ToYear | `IntegerType()` | Conserver | Int32 |  [G-] |
| AgreementDate | `DateType()` | Conserver + convertir type | Date | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| AgreementDocRef | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `LeaveCarryOverAgreements.EmployeeId` → `Employees.Id` (sortante)
- `LeaveCarryOverAgreements.CompanyId` → `Companies.Id` (sortante)
- `LeaveCarryOverAgreements.LeaveTypeId` → `LeaveTypes.Id` (sortante)

**Schéma final `payzenhr_staging.LeaveCarryOverAgreements`** (18 colonnes) :

```
Id                               Int64
EmployeeId                       Nullable(Int64)
CompanyId                        Nullable(Int64)
LeaveTypeId                      Nullable(Int64)
FromYear                         Int32
ToYear                           Int32
AgreementDate                    Date
AgreementDocRef                  String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---


## Module 6 — Payroll (Résultats de paie)

### `PayrollResults`

**Lignes source :** 870 | **Colonnes source :** 77 | **Doublons détectés :** 0 | **FK sortantes :** 2 | **FK entrantes (référencée par) :** 4

**Décision table : 🟡 TRANSFORMÉE**


**Pourquoi :** Table de faits centrale de la paie (870 lignes, 77 colonnes) — le cœur du Data Warehouse. 146 lignes (16.78%) présentent une forte proportion de NULL sur les composantes de calcul (primes, cotisations) : cohérent avec des bulletins en erreur (Status ≠ succès) ou non encore calculés en détail — à croiser impérativement avec la colonne Status avant toute interprétation comme anomalie. ErrorMessage/ResultatJson 100% NULL = aucun bulletin en erreur technique dans ce jeu (cohérent avec Status majoritairement en succès).


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| EmployeeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| Month | `IntegerType()` | Conserver | Int32 |  [G-] |
| Year | `IntegerType()` | Conserver | Int32 |  [G-] |
| Status | `IntegerType()` | Conserver | Int32 | Conserver — pivot d'interprétation de tous les NULL du bulletin (cf. justification). [G-] |
| ErrorMessage | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| ResultatJson | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| SalaireBase | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| HeuresSupp25 | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| HeuresSupp50 | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| HeuresSupp100 | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| Conges | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| JoursFeries | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| PrimeAnciennete | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| PrimeAnciennteRate | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| PrimeImposable1 | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| PrimeImposable2 | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| PrimeImposable3 | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| TotalPrimesImposables | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| TotalBrut | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| FraisProfessionnels | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| IndemniteRepresentation | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| PrimeTransport | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=12.5%) [G7] |
| PrimePanier | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=12.5%) [G7] |
| IndemniteDeplacement | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=100.0%) [G7] |
| IndemniteCaisse | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=15.4%) [G7] |
| PrimeSalissure | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| GratificationsFamilial | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| PrimeVoyageMecque | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=100.0%) [G7] |
| IndemniteLicenciement | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=100.0%) [G7] |
| IndemniteKilometrique | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| PrimeTourne | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| PrimeOutillage | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| AideMedicale | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| AutresPrimesNonImposable | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| TotalIndemnites | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=11.4%) [G7] |
| TotalNiExcedentImposable | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| CnssPartSalariale | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| CnssBase | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| CimrPartSalariale | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| CimrBase | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| AmoPartSalariale | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| AmoBase | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| MutuellePartSalariale | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| MutuelleBase | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| TotalCotisationsSalariales | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| CnssPartPatronale | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| CimrPartPatronale | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| AmoPartPatronale | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| MutuellePartPatronale | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| TotalCotisationsPatronales | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| ImpotRevenu | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| IrTaux | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| Arrondi | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=100.0%) [G7] |
| AvanceSurSalaire | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| InteretSurLogement | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=16.8%) [G7] |
| BrutImposable | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| NetImposable | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| TotalGains | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| TotalRetenues | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| NetAPayer | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| TotalNet | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| TotalNet2 | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| ClaudeModel | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| TokensUsed | `IntegerType()` | Conserver | Int32 |  (NULL=100.0%) [G-] |
| ProcessedAt | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=81.5%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=81.5%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=81.5%) [G4] |
| PayHalf | `IntegerType()` | Conserver | Int32 | 100% NULL — fonctionnalité de paie en deux demi-mois non utilisée sur ce jeu (G9), conservée dans la table technique ci-dessus plutôt que dans la table de faits (peu de valeur analytique en l'état). (NULL=100.0%) [G-] |
| ImportBatchNumber | `IntegerType()` | Conserver | Int32 |  (NULL=83.2%) [G-] |
| ImportMetadataJson | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| ImportSource | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=83.2%) [G5] |

**Colonnes calculées ajoutées :**

| Colonne | Type | Calcul |
|---|---|---|
| PeriodDate | Date | Premier jour du mois de paie : make_date(Year, Month, 1) — clé de jointure temporelle standard pour Power BI (time intelligence). |
| IsSuccessResult | UInt8 (Bool) | Dérivé de Status — à documenter selon la table de code Status (Information insuffisante : le mapping des valeurs entières de Status n'est pas fourni dans les rapports de profiling ; à obtenir auprès de l'équipe applicative avant implémentation). |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Transformations ETL spécifiques :**

- Séparation physique en 2 tables de staging : PayrollResults (faits métier : montants, cotisations, EmployeeId, CompanyId, Month, Year, Status) et PayrollResults_TechnicalAudit (JSON/debug/IA), reliées par Id.
- Validation FK EmployeeId, CompanyId obligatoires (NOT NULL métier).
- Aucun remplacement de NULL par 0 sur les colonnes de montants (G7) — un NULL signifie souvent « composante non applicable à ce bulletin », différent de 0 qui signifierait « applicable mais nul ».

**Relations (validation FK à l'ETL) :**
- `PayrollResults.EmployeeId` → `Employees.Id` (sortante)
- `PayrollResults.CompanyId` → `Companies.Id` (sortante)
- `PayrollBankPayments.PayrollResultId` → `PayrollResults.Id` (entrante)
- `PayrollCalculationAuditSteps.PayrollResultId` → `PayrollResults.Id` (entrante)
- `PayrollResultPrimes.PayrollResultId` → `PayrollResults.Id` (entrante)
- `PayrollTaxSnapshots.PayrollResultId` → `PayrollResults.Id` (entrante)

**Schéma final `payzenhr_staging.PayrollResults`** (83 colonnes) :

```
Id                               Int64
EmployeeId                       Nullable(Int64)
CompanyId                        Nullable(Int64)
Month                            Int32
Year                             Int32
Status                           Int32
ErrorMessage                     String
ResultatJson                     String
SalaireBase                      Decimal(18,2)
HeuresSupp25                     Decimal(18,2)
HeuresSupp50                     Decimal(18,2)
HeuresSupp100                    Decimal(18,2)
Conges                           Decimal(18,2)
JoursFeries                      Decimal(18,2)
PrimeAnciennete                  Decimal(18,2)
PrimeAnciennteRate               Decimal(18,2)
PrimeImposable1                  Decimal(18,2)
PrimeImposable2                  Decimal(18,2)
PrimeImposable3                  Decimal(18,2)
TotalPrimesImposables            Decimal(18,2)
TotalBrut                        Decimal(18,2)
FraisProfessionnels              Decimal(18,2)
IndemniteRepresentation          Decimal(18,2)
PrimeTransport                   Decimal(18,2)
PrimePanier                      Decimal(18,2)
IndemniteDeplacement             Decimal(18,2)
IndemniteCaisse                  Decimal(18,2)
PrimeSalissure                   Decimal(18,2)
GratificationsFamilial           Decimal(18,2)
PrimeVoyageMecque                Decimal(18,2)
IndemniteLicenciement            Decimal(18,2)
IndemniteKilometrique            Decimal(18,2)
PrimeTourne                      Decimal(18,2)
PrimeOutillage                   Decimal(18,2)
AideMedicale                     Decimal(18,2)
AutresPrimesNonImposable         Decimal(18,2)
TotalIndemnites                  Decimal(18,2)
TotalNiExcedentImposable         Decimal(18,2)
CnssPartSalariale                Decimal(18,2)
CnssBase                         Decimal(18,2)
CimrPartSalariale                Decimal(18,2)
CimrBase                         Decimal(18,2)
AmoPartSalariale                 Decimal(18,2)
AmoBase                          Decimal(18,2)
MutuellePartSalariale            Decimal(18,2)
MutuelleBase                     Decimal(18,2)
TotalCotisationsSalariales       Decimal(18,2)
CnssPartPatronale                Decimal(18,2)
CimrPartPatronale                Decimal(18,2)
AmoPartPatronale                 Decimal(18,2)
MutuellePartPatronale            Decimal(18,2)
TotalCotisationsPatronales       Decimal(18,2)
ImpotRevenu                      Decimal(18,2)
IrTaux                           Decimal(18,2)
Arrondi                          Decimal(18,2)
AvanceSurSalaire                 Decimal(18,2)
InteretSurLogement               Decimal(18,2)
BrutImposable                    Decimal(18,2)
NetImposable                     Decimal(18,2)
TotalGains                       Decimal(18,2)
TotalRetenues                    Decimal(18,2)
NetAPayer                        Decimal(18,2)
TotalNet                         Decimal(18,2)
TotalNet2                        Decimal(18,2)
ClaudeModel                      String
TokensUsed                       Int32
ProcessedAt                      DateTime64(3)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
PayHalf                          Int32
ImportBatchNumber                Int32
ImportMetadataJson               String
ImportSource                     String
PeriodDate                       Date
IsSuccessResult                  UInt8 (Bool)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `PayrollResultPrimes`

**Lignes source :** 399 | **Colonnes source :** 12 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 0

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 399 lignes — détail des primes par bulletin, 1:N avec PayrollResults, structure simple.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| PayrollResultId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| Label | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Montant | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| Ordre | `IntegerType()` | Conserver | Int32 |  [G-] |
| IsTaxable | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `PayrollResultPrimes.PayrollResultId` → `PayrollResults.Id` (sortante)

**Schéma final `payzenhr_staging.PayrollResultPrimes`** (16 colonnes) :

```
Id                               Int64
PayrollResultId                  Nullable(Int64)
Label                            String
Montant                          Decimal(18,2)
Ordre                            Int32
IsTaxable                        UInt8 (Bool)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `PayrollCalculationAuditSteps`

**Lignes source :** 9412 | **Colonnes source :** 13 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 0

**Décision table : 🟢 CONSERVÉE TELLE QUELLE (table technique)**


**Pourquoi :** 9412 lignes — journal détaillé (step-by-step) du moteur de calcul de paie. Volume élevé, vocation technique/debug plutôt que décisionnelle. Conservée en staging technique pour audit/traçabilité réglementaire (obligation de justifier un calcul de paie), mais NON recommandée comme source de rapports Power BI grand public (volumétrie + granularité inadaptées).


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| PayrollResultId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| StepOrder | `IntegerType()` | Conserver | Int32 |  [G-] |
| ModuleName | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| FormulaDescription | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| InputsJson | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| OutputsJson | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `PayrollCalculationAuditSteps.PayrollResultId` → `PayrollResults.Id` (sortante)

**Schéma final `payzenhr_staging.PayrollCalculationAuditSteps`** (17 colonnes) :

```
Id                               Int64
PayrollResultId                  Nullable(Int64)
StepOrder                        Int32
ModuleName                       String
FormulaDescription               String
InputsJson                       String
OutputsJson                      String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `PayrollCustomRules`

**Lignes source :** 0 | **Colonnes source :** 12 | **Doublons détectés :** 0 | **FK sortantes :** 0 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne. CompanyId présent en colonne mais SANS contrainte FK EF déclarée (confirmé par le schéma .md, section 'Points d'attention') — à valider manuellement lors de l'activation (pas de contrôle d'intégrité référentielle automatique possible par le moteur EF, donc à sécuriser explicitement côté ETL).


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| CompanyId | `IntegerType()` | Conserver (FK implicite non contrainte) | Nullable(Int64) | Suffixe Id sans contrainte FK EF déclarée — conservée en l'état, validée si référence identifiée. [G2] |
| Title | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Description | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| DslSnippet | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| GeneratedFilePath | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Schéma final `payzenhr_staging.PayrollCustomRules`** (16 colonnes) :

```
Id                               Int64
CompanyId                        Nullable(Int64)
Title                            String
Description                      String
DslSnippet                       String
GeneratedFilePath                String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `SalaryPackages`

**Lignes source :** 0 | **Colonnes source :** 29 | **Doublons détectés :** 0 | **FK sortantes :** 2 | **FK entrantes (référencée par) :** 2

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne — moteur de packages salariaux non encore utilisé sur ce tenant de test. Structure riche (AutoRulesJson, CimrConfigJson) contenant elle-même du JSON métier — à traiter en colonnes JSON typées String, parsing différé faute de données réelles pour en déduire le schéma JSON.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Name | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Category | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Description | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| BaseSalary | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| Status | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| BusinessSectorId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| TemplateType | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| RegulationVersion | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| AutoRulesJson | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CimrConfigJson | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| OriginType | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| SourceTemplateNameSnapshot | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CopiedAt | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| CimrRate | `DecimalType(5,4)` | Conserver | Decimal(5,4) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| HasPrivateInsurance | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| Version | `IntegerType()` | Conserver | Int32 |  [G-] |
| SourceTemplateId | `IntegerType()` | Conserver (FK implicite non contrainte) | Nullable(Int64) | Suffixe Id sans contrainte FK EF déclarée — conservée en l'état, validée si référence identifiée. [G2] |
| SourceTemplateVersion | `IntegerType()` | Conserver | Int32 |  [G-] |
| ValidFrom | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| ValidTo | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| IsLocked | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `SalaryPackages.CompanyId` → `Companies.Id` (sortante)
- `SalaryPackages.BusinessSectorId` → `BusinessSectors.Id` (sortante)
- `SalaryPackageAssignments.SalaryPackageId` → `SalaryPackages.Id` (entrante)
- `SalaryPackageItems.SalaryPackageId` → `SalaryPackages.Id` (entrante)

**Schéma final `payzenhr_staging.SalaryPackages`** (33 colonnes) :

```
Id                               Int64
Name                             String
Category                         String
Description                      String
BaseSalary                       Decimal(18,2)
Status                           String
CompanyId                        Nullable(Int64)
BusinessSectorId                 Nullable(Int64)
TemplateType                     String
RegulationVersion                String
AutoRulesJson                    String
CimrConfigJson                   String
OriginType                       String
SourceTemplateNameSnapshot       String
CopiedAt                         DateTime64(3)
CimrRate                         Decimal(5,4)
HasPrivateInsurance              UInt8 (Bool)
Version                          Int32
SourceTemplateId                 Nullable(Int64)
SourceTemplateVersion            Int32
ValidFrom                        DateTime64(3)
ValidTo                          DateTime64(3)
IsLocked                         UInt8 (Bool)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `SalaryPackageItems`

**Lignes source :** 0 | **Colonnes source :** 19 | **Doublons détectés :** 0 | **FK sortantes :** 3 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne, dépend de SalaryPackages.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| SalaryPackageId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| PayComponentId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| ReferentielElementId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| Label | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| DefaultValue | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| SortOrder | `IntegerType()` | Conserver | Int32 |  [G-] |
| Type | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| IsTaxable | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| IsSocial | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| IsCIMR | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| IsVariable | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| ExemptionLimit | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `SalaryPackageItems.SalaryPackageId` → `SalaryPackages.Id` (sortante)
- `SalaryPackageItems.PayComponentId` → `PayComponents.Id` (sortante)
- `SalaryPackageItems.ReferentielElementId` → `ReferentielElements.Id` (sortante)

**Schéma final `payzenhr_staging.SalaryPackageItems`** (23 colonnes) :

```
Id                               Int64
SalaryPackageId                  Nullable(Int64)
PayComponentId                   Nullable(Int64)
ReferentielElementId             Nullable(Int64)
Label                            String
DefaultValue                     Decimal(18,2)
SortOrder                        Int32
Type                             String
IsTaxable                        UInt8 (Bool)
IsSocial                         UInt8 (Bool)
IsCIMR                           UInt8 (Bool)
IsVariable                       UInt8 (Bool)
ExemptionLimit                   Decimal(18,2)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `SalaryPackageAssignments`

**Lignes source :** 0 | **Colonnes source :** 14 | **Doublons détectés :** 0 | **FK sortantes :** 4 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne, dépend de SalaryPackages.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| SalaryPackageId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| EmployeeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| ContractId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| EmployeeSalaryId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| EffectiveDate | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| EndDate | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| PackageVersion | `IntegerType()` | Conserver | Int32 |  [G-] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `SalaryPackageAssignments.SalaryPackageId` → `SalaryPackages.Id` (sortante)
- `SalaryPackageAssignments.EmployeeId` → `Employees.Id` (sortante)
- `SalaryPackageAssignments.ContractId` → `EmployeeContracts.Id` (sortante)
- `SalaryPackageAssignments.EmployeeSalaryId` → `EmployeeSalaries.Id` (sortante)

**Schéma final `payzenhr_staging.SalaryPackageAssignments`** (18 colonnes) :

```
Id                               Int64
SalaryPackageId                  Nullable(Int64)
EmployeeId                       Nullable(Int64)
ContractId                       Nullable(Int64)
EmployeeSalaryId                 Nullable(Int64)
EffectiveDate                    DateTime64(3)
EndDate                          DateTime64(3)
PackageVersion                   Int32
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `PayComponents`

**Lignes source :** 0 | **Colonnes source :** 22 | **Doublons détectés :** 0 | **FK sortantes :** 0 | **FK entrantes (référencée par) :** 1

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne — catalogue global de composants de paie, non alimenté sur ce jeu bien que PayrollResults contienne déjà ~50 colonnes de composantes nommées explicitement (SalaireBase, PrimeTransport…). Signalé : possible redondance de modélisation entre une approche « colonnes fixes » (PayrollResults) et une approche « catalogue dynamique » (PayComponents/SalaryPackageItems) — Information insuffisante pour trancher laquelle est la source de vérité cible ; point à lever avec l'équipe produit avant la conception du DWH.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Code | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameFr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameAr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameEn | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Type | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| IsTaxable | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| IsSocial | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| IsCIMR | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| ExemptionLimit | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| Version | `IntegerType()` | Conserver | Int32 |  [G-] |
| ValidFrom | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| ValidTo | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| IsRegulated | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| IsActive | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| SortOrder | `IntegerType()` | Conserver | Int32 |  [G-] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `SalaryPackageItems.PayComponentId` → `PayComponents.Id` (entrante)

**Schéma final `payzenhr_staging.PayComponents`** (26 colonnes) :

```
Id                               Int64
Code                             String
NameFr                           String
NameAr                           String
NameEn                           String
Type                             String
IsTaxable                        UInt8 (Bool)
IsSocial                         UInt8 (Bool)
IsCIMR                           UInt8 (Bool)
ExemptionLimit                   Decimal(18,2)
Version                          Int32
ValidFrom                        DateTime64(3)
ValidTo                          DateTime64(3)
IsRegulated                      UInt8 (Bool)
IsActive                         UInt8 (Bool)
SortOrder                        Int32
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `SystemConstants`

**Lignes source :** 32 | **Colonnes source :** 11 | **Doublons détectés :** 0 | **FK sortantes :** 0 | **FK entrantes (référencée par) :** 0

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 32 lignes — constantes système/légales seedées (taux, plafonds).


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Key | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Description | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Value | `DecimalType(18,4)` | Conserver | Decimal(18,4) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| EffectiveDate | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Schéma final `payzenhr_staging.SystemConstants`** (15 colonnes) :

```
Id                               Int64
Key                              String
Description                      String
Value                            Decimal(18,4)
EffectiveDate                    DateTime64(3)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `IrTaxBrackets`

**Lignes source :** 6 | **Colonnes source :** 12 | **Doublons détectés :** 0 | **FK sortantes :** 0 | **FK entrantes (référencée par) :** 0

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 6 lignes — barème de l'impôt sur le revenu marocain, référentiel légal stable.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| MinIncome | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| MaxIncome | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| Rate | `DecimalType(5,4)` | Conserver | Decimal(5,4) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| Deduction | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| EffectiveDate | `TimestampType()` | Conserver + convertir type | DateTime64(3) | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Schéma final `payzenhr_staging.IrTaxBrackets`** (16 colonnes) :

```
Id                               Int64
MinIncome                        Decimal(18,2)
MaxIncome                        Decimal(18,2)
Rate                             Decimal(5,4)
Deduction                        Decimal(18,2)
EffectiveDate                    DateTime64(3)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `CnssPreetabliImports`

**Lignes source :** 0 | **Colonnes source :** 14 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 1

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne — imports de fichiers préétablis CNSS non utilisés sur ce jeu.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| FileName | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| AffiliateNumber | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Period | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| EmployeeCount | `IntegerType()` | Conserver | Int32 |  [G-] |
| IssueCount | `IntegerType()` | Conserver | Int32 |  [G-] |
| Status | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `CnssPreetabliImports.CompanyId` → `Companies.Id` (sortante)
- `CnssPreetabliLines.CnssPreetabliImportId` → `CnssPreetabliImports.Id` (entrante)

**Schéma final `payzenhr_staging.CnssPreetabliImports`** (18 colonnes) :

```
Id                               Int64
CompanyId                        Nullable(Int64)
FileName                         String
AffiliateNumber                  String
Period                           String
EmployeeCount                    Int32
IssueCount                       Int32
Status                           String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `CnssPreetabliLines`

**Lignes source :** 0 | **Colonnes source :** 17 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne, dépend de CnssPreetabliImports.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| CnssPreetabliImportId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| LineNumber | `IntegerType()` | Conserver | Int32 |  [G-] |
| AffiliateNumber | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Period | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| InsuredNumber | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| FullName | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| ChildrenCount | `IntegerType()` | Conserver | Int32 |  [G-] |
| FamilyAllowanceToPay | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| FamilyAllowanceToDeduct | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| FamilyAllowanceNetToPay | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `CnssPreetabliLines.CnssPreetabliImportId` → `CnssPreetabliImports.Id` (sortante)

**Schéma final `payzenhr_staging.CnssPreetabliLines`** (21 colonnes) :

```
Id                               Int64
CnssPreetabliImportId            Nullable(Int64)
LineNumber                       Int32
AffiliateNumber                  String
Period                           String
InsuredNumber                    String
FullName                         String
ChildrenCount                    Int32
FamilyAllowanceToPay             Decimal(18,2)
FamilyAllowanceToDeduct          Decimal(18,2)
FamilyAllowanceNetToPay          Decimal(18,2)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `PayrollTaxSnapshots`

**Lignes source :** 581 | **Colonnes source :** 20 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 0

**Décision table : 🟣 FUSIONNÉE avec PayrollResults**


**Pourquoi :** 581 lignes, relation 1:1 stricte avec PayrollResults (FK PayrollResultId, Cascade, confirmé par le schéma .md). Duplique EmployeeId/CompanyId/Month/Year déjà présents dans PayrollResults. Un JOIN 1:1 systématique en aval n'apporte pas de valeur (pas de cardinalité différente) : fusion recommandée dans une vue/table élargie Staging.PayrollResults_Fiscal (cumuls fiscaux CNSS/AMO/IR) au lieu d'une table séparée, afin de réduire les jointures redondantes en couche ClickHouse.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| PayrollResultId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| EmployeeId | `IntegerType()` | Conserver (FK implicite non contrainte) | Nullable(Int64) | Suffixe Id sans contrainte FK EF déclarée — conservée en l'état, validée si référence identifiée. [G2] |
| CompanyId | `IntegerType()` | Conserver (FK implicite non contrainte) | Nullable(Int64) | Suffixe Id sans contrainte FK EF déclarée — conservée en l'état, validée si référence identifiée. [G2] |
| Month | `IntegerType()` | Conserver | Int32 |  [G-] |
| Year | `IntegerType()` | Conserver | Int32 |  [G-] |
| CumulBrut | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| CumulCnss | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| CumulAmo | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| CumulSni | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| CumulIr | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| CumulNet | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| TauxEffectif | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=95.7%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |
| CumulFp | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Transformations ETL spécifiques :**

- Fusion par LEFT JOIN PayrollResults.Id = PayrollTaxSnapshots.PayrollResultId ; les colonnes EmployeeId/CompanyId/Month/Year du snapshot sont abandonnées (doublons exacts avec PayrollResults) après contrôle de cohérence (elles doivent être strictement identiques — ligne en anomalie sinon).

**Relations (validation FK à l'ETL) :**
- `PayrollTaxSnapshots.PayrollResultId` → `PayrollResults.Id` (sortante)

**Schéma final `payzenhr_staging.PayrollTaxSnapshots`** (24 colonnes) :

```
Id                               Int64
PayrollResultId                  Nullable(Int64)
EmployeeId                       Nullable(Int64)
CompanyId                        Nullable(Int64)
Month                            Int32
Year                             Int32
CumulBrut                        Decimal(18,2)
CumulCnss                        Decimal(18,2)
CumulAmo                         Decimal(18,2)
CumulSni                         Decimal(18,2)
CumulIr                          Decimal(18,2)
CumulNet                         Decimal(18,2)
TauxEffectif                     Decimal(18,2)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
CumulFp                          Decimal(18,2)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `PayrollBankPayments`

**Lignes source :** 0 | **Colonnes source :** 10 | **Doublons détectés :** 0 | **FK sortantes :** 2 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne — module de virement bancaire des salaires non utilisé sur ce jeu de test.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| PayrollResultId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| BankAccountId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| Amount | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `PayrollBankPayments.PayrollResultId` → `PayrollResults.Id` (sortante)
- `PayrollBankPayments.BankAccountId` → `BankAccounts.Id` (sortante)

**Schéma final `payzenhr_staging.PayrollBankPayments`** (14 colonnes) :

```
Id                               Int64
PayrollResultId                  Nullable(Int64)
BankAccountId                    Nullable(Int64)
Amount                           Decimal(18,2)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---


## Module 7 — Payroll Référentiel (Moteur de règles de paie)

### `Authorities`

**Lignes source :** 0 | **Colonnes source :** 12 | **Doublons détectés :** 0 | **FK sortantes :** 0 | **FK entrantes (référencée par) :** 1

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne — référentiel des autorités légales émettrices de règles (CNSS, DGI…).


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Code | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Name | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Description | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| SortOrder | `IntegerType()` | Conserver | Int32 |  [G-] |
| IsActive | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `ElementRules.AuthorityId` → `Authorities.Id` (entrante)

**Schéma final `payzenhr_staging.Authorities`** (16 colonnes) :

```
Id                               Int64
Code                             String
Name                             String
Description                      String
SortOrder                        Int32
IsActive                         UInt8 (Bool)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `ElementCategories`

**Lignes source :** 0 | **Colonnes source :** 11 | **Doublons détectés :** 0 | **FK sortantes :** 0 | **FK entrantes (référencée par) :** 1

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne — catégories d'éléments de paie du moteur de règles.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Name | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Description | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| SortOrder | `IntegerType()` | Conserver | Int32 |  [G-] |
| IsActive | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `ReferentielElements.CategoryId` → `ElementCategories.Id` (entrante)

**Schéma final `payzenhr_staging.ElementCategories`** (15 colonnes) :

```
Id                               Int64
Name                             String
Description                      String
SortOrder                        Int32
IsActive                         UInt8 (Bool)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `EligibilityCriteria`

**Lignes source :** 0 | **Colonnes source :** 10 | **Doublons détectés :** 0 | **FK sortantes :** 0 | **FK entrantes (référencée par) :** 2

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne — critères d'éligibilité aux règles de paie.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Code | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Name | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Description | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `RulePercentages.EligibilityId` → `EligibilityCriteria.Id` (entrante)
- `RuleVariants.EligibilityId` → `EligibilityCriteria.Id` (entrante)

**Schéma final `payzenhr_staging.EligibilityCriteria`** (14 colonnes) :

```
Id                               Int64
Code                             String
Name                             String
Description                      String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `LegalParameters`

**Lignes source :** 0 | **Colonnes source :** 14 | **Doublons détectés :** 0 | **FK sortantes :** 0 | **FK entrantes (référencée par) :** 1

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne — paramètres légaux utilisés par RuleFormulas.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Code | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Label | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Value | `DecimalType(18,6)` | Conserver | Decimal(18,6) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| Unit | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Source | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| EffectiveFrom | `DateType()` | Conserver + convertir type | Date | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| EffectiveTo | `DateType()` | Conserver + convertir type | Date | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `RuleFormulas.ParameterId` → `LegalParameters.Id` (entrante)

**Schéma final `payzenhr_staging.LegalParameters`** (18 colonnes) :

```
Id                               Int64
Code                             String
Label                            String
Value                            Decimal(18,6)
Unit                             String
Source                           String
EffectiveFrom                    Date
EffectiveTo                      Date
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `ReferentielElements`

**Lignes source :** 0 | **Colonnes source :** 15 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 2

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne — catalogue d'éléments de paie réglementaires.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Code | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Name | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CategoryId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| Description | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| DefaultFrequency | `IntegerType()` | Conserver | Int32 |  [G-] |
| Status | `IntegerType()` | Conserver | Int32 |  [G-] |
| HasConvergence | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| IsActive | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `ReferentielElements.CategoryId` → `ElementCategories.Id` (sortante)
- `ElementRules.ElementId` → `ReferentielElements.Id` (entrante)
- `SalaryPackageItems.ReferentielElementId` → `ReferentielElements.Id` (entrante)

**Schéma final `payzenhr_staging.ReferentielElements`** (19 colonnes) :

```
Id                               Int64
Code                             String
Name                             String
CategoryId                       Nullable(Int64)
Description                      String
DefaultFrequency                 Int32
Status                           Int32
HasConvergence                   UInt8 (Bool)
IsActive                         UInt8 (Bool)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `ElementRules`

**Lignes source :** 0 | **Colonnes source :** 15 | **Doublons détectés :** 0 | **FK sortantes :** 2 | **FK entrantes (référencée par) :** 6

**Décision table : 🔵 CONSERVÉE (chargement différé) — candidate à la fusion**


**Pourquoi :** 0 ligne. Relation 1:1 avec RuleCaps/RulePercentages/RuleFormulas/RuleDualCaps selon le type de règle (polymorphisme par table). Une fois alimentée, ces 4 tables filles pourront être fusionnées dans ElementRules en staging (colonnes optionnelles regroupées) pour simplifier le modèle, à condition de confirmer qu'une ElementRule ne référence jamais plusieurs types de sous-règle simultanément — Information insuffisante pour le confirmer sans données réelles.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| ElementId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| AuthorityId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| ExemptionType | `IntegerType()` | Conserver | Int32 |  [G-] |
| RuleDetails | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| SourceRef | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| EffectiveFrom | `DateType()` | Conserver + convertir type | Date | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| EffectiveTo | `DateType()` | Conserver + convertir type | Date | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| Status | `IntegerType()` | Conserver | Int32 |  [G-] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `ElementRules.ElementId` → `ReferentielElements.Id` (sortante)
- `ElementRules.AuthorityId` → `Authorities.Id` (sortante)
- `RuleCaps.RuleId` → `ElementRules.Id` (entrante)
- `RuleDualCaps.RuleId` → `ElementRules.Id` (entrante)
- `RuleFormulas.RuleId` → `ElementRules.Id` (entrante)
- `RulePercentages.RuleId` → `ElementRules.Id` (entrante)
- `RuleTiers.RuleId` → `ElementRules.Id` (entrante)
- `RuleVariants.RuleId` → `ElementRules.Id` (entrante)

**Schéma final `payzenhr_staging.ElementRules`** (19 colonnes) :

```
Id                               Int64
ElementId                        Nullable(Int64)
AuthorityId                      Nullable(Int64)
ExemptionType                    Int32
RuleDetails                      String
SourceRef                        String
EffectiveFrom                    Date
EffectiveTo                      Date
Status                           Int32
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `RuleCaps`

**Lignes source :** 0 | **Colonnes source :** 11 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé) — candidate à la fusion avec ElementRules**


**Pourquoi :** 0 ligne, 1:1 avec ElementRules.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| RuleId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| CapAmount | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| CapUnit | `IntegerType()` | Conserver | Int32 |  [G-] |
| MinAmount | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `RuleCaps.RuleId` → `ElementRules.Id` (sortante)

**Schéma final `payzenhr_staging.RuleCaps`** (15 colonnes) :

```
Id                               Int64
RuleId                           Nullable(Int64)
CapAmount                        Decimal(18,2)
CapUnit                          Int32
MinAmount                        Decimal(18,2)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `RulePercentages`

**Lignes source :** 0 | **Colonnes source :** 11 | **Doublons détectés :** 0 | **FK sortantes :** 2 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé) — candidate à la fusion avec ElementRules**


**Pourquoi :** 0 ligne, 1:1 avec ElementRules.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| RuleId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| Percentage | `DecimalType(5,2)` | Conserver | Decimal(5,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| BaseReference | `IntegerType()` | Conserver | Int32 |  [G-] |
| EligibilityId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `RulePercentages.RuleId` → `ElementRules.Id` (sortante)
- `RulePercentages.EligibilityId` → `EligibilityCriteria.Id` (sortante)

**Schéma final `payzenhr_staging.RulePercentages`** (15 colonnes) :

```
Id                               Int64
RuleId                           Nullable(Int64)
Percentage                       Decimal(5,2)
BaseReference                    Int32
EligibilityId                    Nullable(Int64)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `RuleFormulas`

**Lignes source :** 0 | **Colonnes source :** 11 | **Doublons détectés :** 0 | **FK sortantes :** 2 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé) — candidate à la fusion avec ElementRules**


**Pourquoi :** 0 ligne, 1:1 avec ElementRules, FK vers LegalParameters.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| RuleId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| Multiplier | `DecimalType(10,4)` | Conserver | Decimal(10,4) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| ParameterId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| ResultUnit | `IntegerType()` | Conserver | Int32 |  [G-] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `RuleFormulas.RuleId` → `ElementRules.Id` (sortante)
- `RuleFormulas.ParameterId` → `LegalParameters.Id` (sortante)

**Schéma final `payzenhr_staging.RuleFormulas`** (15 colonnes) :

```
Id                               Int64
RuleId                           Nullable(Int64)
Multiplier                       Decimal(10,4)
ParameterId                      Nullable(Int64)
ResultUnit                       Int32
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `RuleDualCaps`

**Lignes source :** 0 | **Colonnes source :** 13 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé) — candidate à la fusion avec ElementRules**


**Pourquoi :** 0 ligne, 1:1 avec ElementRules.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| RuleId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| FixedCapAmount | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| FixedCapUnit | `IntegerType()` | Conserver | Int32 |  [G-] |
| PercentageCap | `DecimalType(5,2)` | Conserver | Decimal(5,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| BaseReference | `IntegerType()` | Conserver | Int32 |  [G-] |
| Logic | `IntegerType()` | Conserver | Int32 |  [G-] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `RuleDualCaps.RuleId` → `ElementRules.Id` (sortante)

**Schéma final `payzenhr_staging.RuleDualCaps`** (17 colonnes) :

```
Id                               Int64
RuleId                           Nullable(Int64)
FixedCapAmount                   Decimal(18,2)
FixedCapUnit                     Int32
PercentageCap                    Decimal(5,2)
BaseReference                    Int32
Logic                            Int32
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `RuleTiers`

**Lignes source :** 0 | **Colonnes source :** 12 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne — relation 1:N (paliers) avec ElementRules, NE PEUT PAS être fusionnée (cardinalité multiple, contrairement aux 4 tables 1:1 ci-dessus).


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| RuleId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| TierOrder | `IntegerType()` | Conserver | Int32 |  [G-] |
| FromAmount | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| ToAmount | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| ExemptPercent | `DecimalType(5,2)` | Conserver | Decimal(5,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `RuleTiers.RuleId` → `ElementRules.Id` (sortante)

**Schéma final `payzenhr_staging.RuleTiers`** (16 colonnes) :

```
Id                               Int64
RuleId                           Nullable(Int64)
TierOrder                        Int32
FromAmount                       Decimal(18,2)
ToAmount                         Decimal(18,2)
ExemptPercent                    Decimal(5,2)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `RuleVariants`

**Lignes source :** 0 | **Colonnes source :** 15 | **Doublons détectés :** 0 | **FK sortantes :** 2 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne — relation 1:N avec ElementRules et EligibilityCriteria.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| RuleId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| VariantType | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| VariantKey | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| VariantLabel | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| OverrideCap | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| OverridePercentage | `DecimalType(5,2)` | Conserver | Decimal(5,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| EligibilityId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| SortOrder | `IntegerType()` | Conserver | Int32 |  [G-] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `RuleVariants.RuleId` → `ElementRules.Id` (sortante)
- `RuleVariants.EligibilityId` → `EligibilityCriteria.Id` (sortante)

**Schéma final `payzenhr_staging.RuleVariants`** (19 colonnes) :

```
Id                               Int64
RuleId                           Nullable(Int64)
VariantType                      String
VariantKey                       String
VariantLabel                     String
OverrideCap                      Decimal(18,2)
OverridePercentage               Decimal(5,2)
EligibilityId                    Nullable(Int64)
SortOrder                        Int32
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `AncienneteRateSets`

**Lignes source :** 0 | **Colonnes source :** 15 | **Doublons détectés :** 0 | **FK sortantes :** 2 | **FK entrantes (référencée par) :** 2

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne — barèmes d'ancienneté par société, avec auto-FK ClonedFromId (clonage de barème).


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| ClonedFromId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| Code | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Name | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| IsLegalDefault | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| Source | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| EffectiveFrom | `DateType()` | Conserver + convertir type | Date | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| EffectiveTo | `DateType()` | Conserver + convertir type | Date | Déjà typé date/heure côté source — cast direct, contrôle de plage (dates aberrantes). [G3] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `AncienneteRateSets.CompanyId` → `Companies.Id` (sortante)
- `AncienneteRateSets.ClonedFromId` → `AncienneteRateSets.Id` (sortante)
- `AncienneteRateSets.ClonedFromId` → `AncienneteRateSets.Id` (entrante)
- `AncienneteRates.RateSetId` → `AncienneteRateSets.Id` (entrante)

**Schéma final `payzenhr_staging.AncienneteRateSets`** (19 colonnes) :

```
Id                               Int64
CompanyId                        Nullable(Int64)
ClonedFromId                     Nullable(Int64)
Code                             String
Name                             String
IsLegalDefault                   UInt8 (Bool)
Source                           String
EffectiveFrom                    Date
EffectiveTo                      Date
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `AncienneteRates`

**Lignes source :** 0 | **Colonnes source :** 12 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne, 1:N avec AncienneteRateSets.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| RateSetId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| MinYears | `IntegerType()` | Conserver | Int32 |  [G-] |
| MaxYears | `IntegerType()` | Conserver | Int32 |  [G-] |
| Rate | `DecimalType(5,4)` | Conserver | Decimal(5,4) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| SortOrder | `IntegerType()` | Conserver | Int32 |  [G-] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `AncienneteRates.RateSetId` → `AncienneteRateSets.Id` (sortante)

**Schéma final `payzenhr_staging.AncienneteRates`** (16 colonnes) :

```
Id                               Int64
RateSetId                        Nullable(Int64)
MinYears                         Int32
MaxYears                         Int32
Rate                             Decimal(5,4)
SortOrder                        Int32
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `BusinessSectors`

**Lignes source :** 0 | **Colonnes source :** 11 | **Doublons détectés :** 0 | **FK sortantes :** 0 | **FK entrantes (référencée par) :** 1

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne — référentiel de secteurs d'activité. Rappel : Companies.BusinessSector (colonne texte, 100% NULL) semble dupliquer cette table de manière non normalisée — cf. remarque sur Companies.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Code | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Name | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| IsStandard | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| SortOrder | `IntegerType()` | Conserver | Int32 |  [G-] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `SalaryPackages.BusinessSectorId` → `BusinessSectors.Id` (entrante)

**Schéma final `payzenhr_staging.BusinessSectors`** (15 colonnes) :

```
Id                               Int64
Code                             String
Name                             String
IsStandard                       UInt8 (Bool)
SortOrder                        Int32
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---


## Module 8 — BankAccount (Comptes bancaires)

### `BankAccounts`

**Lignes source :** 7 | **Colonnes source :** 11 | **Doublons détectés :** 0 | **FK sortantes :** 2 | **FK entrantes (référencée par) :** 3

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 7 lignes. CompanyId/EmployeeId 100% NULL chacun sur ce jeu — à vérifier si chaque compte a bien AU MOINS un des deux renseigné (compte société OU compte employé, jamais les deux vides) : contrôle de cohérence à ajouter en ETL, Information insuffisante pour confirmer sans regarder les valeurs non-NULL ligne à ligne (hors périmètre des rapports agrégés fournis).


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| BankName | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| RIB | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). (NULL=100.0%) [G2] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |
| EmployeeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). (NULL=100.0%) [G2] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `BankAccounts.CompanyId` → `Companies.Id` (sortante)
- `BankAccounts.EmployeeId` → `Employees.Id` (sortante)
- `EmployeeBankAccountAllocations.BankAccountId` → `BankAccounts.Id` (entrante)
- `PayrollBankPayments.BankAccountId` → `BankAccounts.Id` (entrante)
- `Employees.BankAccountId` → `BankAccounts.Id` (entrante)

**Schéma final `payzenhr_staging.BankAccounts`** (15 colonnes) :

```
Id                               Int64
BankName                         String
RIB                              String
CompanyId                        Nullable(Int64)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
EmployeeId                       Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `EmployeeBankAccountAllocations`

**Lignes source :** 0 | **Colonnes source :** 10 | **Doublons détectés :** 0 | **FK sortantes :** 2 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne — répartition multi-comptes du salaire, non utilisée sur ce jeu.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| EmployeeId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| BankAccountId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| Percentage | `DecimalType(5,2)` | Conserver | Decimal(5,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). [G7] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `EmployeeBankAccountAllocations.EmployeeId` → `Employees.Id` (sortante)
- `EmployeeBankAccountAllocations.BankAccountId` → `BankAccounts.Id` (sortante)

**Schéma final `payzenhr_staging.EmployeeBankAccountAllocations`** (14 colonnes) :

```
Id                               Int64
EmployeeId                       Nullable(Int64)
BankAccountId                    Nullable(Int64)
Percentage                       Decimal(5,2)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---


## Module 9 — Alert (Alertes runtime)

### `AlertCategories`

**Lignes source :** 6 | **Colonnes source :** 14 | **Doublons détectés :** 0 | **FK sortantes :** 0 | **FK entrantes (référencée par) :** 1

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 6 lignes — lookup.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Code | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Name | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameAr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| IconName | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| ColorHex | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| SortOrder | `IntegerType()` | Conserver | Int32 |  [G-] |
| IsActive | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `AlertDefinitions.CategoryId` → `AlertCategories.Id` (entrante)

**Schéma final `payzenhr_staging.AlertCategories`** (18 colonnes) :

```
Id                               Int64
Code                             String
Name                             String
NameAr                           String
IconName                         String
ColorHex                         String
SortOrder                        Int32
IsActive                         UInt8 (Bool)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `AlertSeverities`

**Lignes source :** 3 | **Colonnes source :** 13 | **Doublons détectés :** 0 | **FK sortantes :** 0 | **FK entrantes (référencée par) :** 2

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 3 lignes — lookup (info/warning/critique).


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Code | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Label | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| LabelAr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Level | `IntegerType()` | Conserver | Int32 |  [G-] |
| ColorHex | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| RequiresAcknowledgement | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `AlertDefinitions.SeverityId` → `AlertSeverities.Id` (entrante)
- `Alerts.SeverityId` → `AlertSeverities.Id` (entrante)

**Schéma final `payzenhr_staging.AlertSeverities`** (17 colonnes) :

```
Id                               Int64
Code                             String
Label                            String
LabelAr                          String
Level                            Int32
ColorHex                         String
RequiresAcknowledgement          UInt8 (Bool)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `AlertDefinitions`

**Lignes source :** 12 | **Colonnes source :** 33 | **Doublons détectés :** 0 | **FK sortantes :** 2 | **FK entrantes (référencée par) :** 5

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 12 lignes — catalogue de définitions d'alertes. Description/DescriptionAr 100% NULL, ScheduleCron 33% NULL (alertes évènementielles vs. planifiées, cohérent), ThresholdValue 83.33% NULL (alertes non basées sur un seuil numérique, cohérent).


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Code | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CategoryId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| SeverityId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| Name | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameAr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Description | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| DescriptionAr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| TriggerType | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| AppliesTo | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| AppliesToFilter | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=58.3%) [G5] |
| SourceField | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| NavigationUrl | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| MessageTemplateFr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| MessageTemplateAr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| IsActive | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| IsOptional | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| CanUserDisable | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| SupportsInApp | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| SupportsEmail | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| SupportsSms | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| SupportsWebhook | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| ScheduleCron | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=33.3%) [G5] |
| EventName | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=50.0%) [G5] |
| ThresholdValue | `DecimalType(18,2)` | Conserver | Decimal(18,2) | Montant/valeur numérique — NULL conservé tel quel (NULL ≠ 0 en paie). (NULL=83.3%) [G7] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |
| MessageTemplateEn | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameEn | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `AlertDefinitions.CategoryId` → `AlertCategories.Id` (sortante)
- `AlertDefinitions.SeverityId` → `AlertSeverities.Id` (sortante)
- `AlertRecipientRoles.DefinitionId` → `AlertDefinitions.Id` (entrante)
- `AlertRuleExecutionLogs.DefinitionId` → `AlertDefinitions.Id` (entrante)
- `Alerts.DefinitionId` → `AlertDefinitions.Id` (entrante)
- `AlertUserPreferences.DefinitionId` → `AlertDefinitions.Id` (entrante)
- `CompanyAlertPreferences.DefinitionId` → `AlertDefinitions.Id` (entrante)

**Schéma final `payzenhr_staging.AlertDefinitions`** (37 colonnes) :

```
Id                               Int64
Code                             String
CategoryId                       Nullable(Int64)
SeverityId                       Nullable(Int64)
Name                             String
NameAr                           String
Description                      String
DescriptionAr                    String
TriggerType                      String
AppliesTo                        String
AppliesToFilter                  String
SourceField                      String
NavigationUrl                    String
MessageTemplateFr                String
MessageTemplateAr                String
IsActive                         UInt8 (Bool)
IsOptional                       UInt8 (Bool)
CanUserDisable                   UInt8 (Bool)
SupportsInApp                    UInt8 (Bool)
SupportsEmail                    UInt8 (Bool)
SupportsSms                      UInt8 (Bool)
SupportsWebhook                  UInt8 (Bool)
ScheduleCron                     String
EventName                        String
ThresholdValue                   Decimal(18,2)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
MessageTemplateEn                String
NameEn                           String
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `AlertRecipientRoles`

**Lignes source :** 24 | **Colonnes source :** 9 | **Doublons détectés :** 0 | **FK sortantes :** 2 | **FK entrantes (référencée par) :** 0

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 24 lignes — jonction AlertDefinitions↔Roles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| DefinitionId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| RoleId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `AlertRecipientRoles.DefinitionId` → `AlertDefinitions.Id` (sortante)
- `AlertRecipientRoles.RoleId` → `Roles.Id` (sortante)

**Schéma final `payzenhr_staging.AlertRecipientRoles`** (13 colonnes) :

```
Id                               Int64
DefinitionId                     Nullable(Int64)
RoleId                           Nullable(Int64)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `CompanyAlertPreferences`

**Lignes source :** 0 | **Colonnes source :** 15 | **Doublons détectés :** 0 | **FK sortantes :** 3 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne — préférences d'alerte par société non utilisées sur ce jeu.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| DefinitionId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| IsEnabled | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| InAppEnabled | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| EmailEnabled | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| SmsEnabled | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| WebhookEnabled | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| UpdatedByUserId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `CompanyAlertPreferences.CompanyId` → `Companies.Id` (sortante)
- `CompanyAlertPreferences.DefinitionId` → `AlertDefinitions.Id` (sortante)
- `CompanyAlertPreferences.UpdatedByUserId` → `Users.Id` (sortante)

**Schéma final `payzenhr_staging.CompanyAlertPreferences`** (19 colonnes) :

```
Id                               Int64
CompanyId                        Nullable(Int64)
DefinitionId                     Nullable(Int64)
IsEnabled                        UInt8 (Bool)
InAppEnabled                     UInt8 (Bool)
EmailEnabled                     UInt8 (Bool)
SmsEnabled                       UInt8 (Bool)
WebhookEnabled                   UInt8 (Bool)
UpdatedByUserId                  Nullable(Int64)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `Alerts`

**Lignes source :** 64 | **Colonnes source :** 31 | **Doublons détectés :** 0 | **FK sortantes :** 6 | **FK entrantes (référencée par) :** 2

**Décision table : 🟡 TRANSFORMÉE**


**Pourquoi :** 64 lignes. Référence polymorphe EntityType/EntityId (documentée dans le schéma .md comme « sans FK ») — ne peut PAS être validée par un contrôle FK classique. AssignedToUserId/AcknowledgedAt/ResolvedAt/ExpiresAt à forte proportion de NULL, cohérent avec des alertes non encore traitées.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| DefinitionId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| EntityType | `StringType()` | Conserver + nettoyer | String | Conserver tel quel — clé de discrimination du lien polymorphe. [G5] |
| EntityId | `IntegerType()` | Conserver (FK implicite non contrainte) | Nullable(Int64) | Conserver en Nullable(Int64) — AUCUNE validation FK possible automatiquement (dépend de EntityType) ; documenter dans le data dictionnary DWH que ce champ n'est pas intégrité-contrôlé. [G2] |
| SeverityId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| SeverityLabel | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CategoryName | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| DefinitionName | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NavigationUrl | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Message | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| MessageAr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Status | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| AssignedToUserId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). (NULL=100.0%) [G2] |
| Metadata | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| DeduplicationKey | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CorrelationId | `StringType()` | Conserver + nettoyer | String | Identifiant technique de type texte (corrélation externe / référence tierce), pas une FK relationnelle interne. [G5] |
| TriggeredByEvent | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| IsAcknowledged | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| AcknowledgedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| AcknowledgedByUserId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). (NULL=100.0%) [G2] |
| ResolvedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=92.2%) [G3] |
| ResolvedByUserId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). (NULL=92.2%) [G2] |
| ExpiresAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=46.9%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=46.9%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |
| MessageEn | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=6.2%) [G5] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Transformations ETL spécifiques :**

- Le couple (EntityType, EntityId) ne peut pas être validé par un LEFT ANTI JOIN standard (T4) : à traiter par un mapping conditionnel (CASE WHEN EntityType='Employee' THEN JOIN Employees…) si un contrôle qualité est requis, sinon accepté tel quel comme référence non contrainte (cohérent avec la conception source).

**Relations (validation FK à l'ETL) :**
- `Alerts.CompanyId` → `Companies.Id` (sortante)
- `Alerts.DefinitionId` → `AlertDefinitions.Id` (sortante)
- `Alerts.SeverityId` → `AlertSeverities.Id` (sortante)
- `Alerts.AssignedToUserId` → `Users.Id` (sortante)
- `Alerts.AcknowledgedByUserId` → `Users.Id` (sortante)
- `Alerts.ResolvedByUserId` → `Users.Id` (sortante)
- `AlertAuditLogs.AlertId` → `Alerts.Id` (entrante)
- `AlertDeliveries.AlertId` → `Alerts.Id` (entrante)

**Schéma final `payzenhr_staging.Alerts`** (35 colonnes) :

```
Id                               Int64
CompanyId                        Nullable(Int64)
DefinitionId                     Nullable(Int64)
EntityType                       String
EntityId                         Nullable(Int64)
SeverityId                       Nullable(Int64)
SeverityLabel                    String
CategoryName                     String
DefinitionName                   String
NavigationUrl                    String
Message                          String
MessageAr                        String
Status                           String
AssignedToUserId                 Nullable(Int64)
Metadata                         String
DeduplicationKey                 String
CorrelationId                    String
TriggeredByEvent                 String
IsAcknowledged                   UInt8 (Bool)
AcknowledgedAt                   Nullable(DateTime64(3))
AcknowledgedByUserId             Nullable(Int64)
ResolvedAt                       Nullable(DateTime64(3))
ResolvedByUserId                 Nullable(Int64)
ExpiresAt                        Nullable(DateTime64(3))
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
MessageEn                        String
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `AlertUserPreferences`

**Lignes source :** 0 | **Colonnes source :** 15 | **Doublons détectés :** 0 | **FK sortantes :** 3 | **FK entrantes (référencée par) :** 1

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| UserId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| DefinitionId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| ChannelInApp | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| ChannelEmail | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| ChannelSms | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| DigestMode | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| MutedUntil | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `AlertUserPreferences.CompanyId` → `Companies.Id` (sortante)
- `AlertUserPreferences.UserId` → `Users.Id` (sortante)
- `AlertUserPreferences.DefinitionId` → `AlertDefinitions.Id` (sortante)
- `AlertUserPreferenceAudits.PreferenceId` → `AlertUserPreferences.Id` (entrante)

**Schéma final `payzenhr_staging.AlertUserPreferences`** (19 colonnes) :

```
Id                               Int64
CompanyId                        Nullable(Int64)
UserId                           Nullable(Int64)
DefinitionId                     Nullable(Int64)
ChannelInApp                     UInt8 (Bool)
ChannelEmail                     UInt8 (Bool)
ChannelSms                       UInt8 (Bool)
DigestMode                       String
MutedUntil                       String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `AlertUserPreferenceAudits`

**Lignes source :** 0 | **Colonnes source :** 14 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne, dépend de AlertUserPreferences.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| PreferenceId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| CompanyId | `IntegerType()` | Conserver (FK implicite non contrainte) | Nullable(Int64) | Suffixe Id sans contrainte FK EF déclarée — conservée en l'état, validée si référence identifiée. [G2] |
| UserId | `IntegerType()` | Conserver (FK implicite non contrainte) | Nullable(Int64) | Suffixe Id sans contrainte FK EF déclarée — conservée en l'état, validée si référence identifiée. [G2] |
| DefinitionId | `IntegerType()` | Conserver (FK implicite non contrainte) | Nullable(Int64) | Suffixe Id sans contrainte FK EF déclarée — conservée en l'état, validée si référence identifiée. [G2] |
| OldValueJson | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NewValueJson | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| ChangedBy | `IntegerType()` | Conserver | Int32 |  [G-] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `AlertUserPreferenceAudits.PreferenceId` → `AlertUserPreferences.Id` (sortante)

**Schéma final `payzenhr_staging.AlertUserPreferenceAudits`** (18 colonnes) :

```
Id                               Int64
PreferenceId                     Nullable(Int64)
CompanyId                        Nullable(Int64)
UserId                           Nullable(Int64)
DefinitionId                     Nullable(Int64)
OldValueJson                     String
NewValueJson                     String
ChangedBy                        Int32
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `AlertAuditLogs`

**Lignes source :** 234 | **Colonnes source :** 16 | **Doublons détectés :** 0 | **FK sortantes :** 2 | **FK entrantes (référencée par) :** 0

**Décision table : 🟢 CONSERVÉE TELLE QUELLE (table technique)**


**Pourquoi :** 234 lignes. OldValueJson/NewValueJson/IpAddress/UserAgent 100% NULL — journal d'audit dont les colonnes de détail ne sont pas alimentées sur ce jeu (G9). Conservée pour traçabilité, faible valeur analytique Power BI en l'état.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| AlertId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| CompanyId | `IntegerType()` | Conserver (FK implicite non contrainte) | Nullable(Int64) | Suffixe Id sans contrainte FK EF déclarée — conservée en l'état, validée si référence identifiée. [G2] |
| UserId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| Action | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| OldValueJson | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| NewValueJson | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| Comment | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| IpAddress | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| UserAgent | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `AlertAuditLogs.AlertId` → `Alerts.Id` (sortante)
- `AlertAuditLogs.UserId` → `Users.Id` (sortante)

**Schéma final `payzenhr_staging.AlertAuditLogs`** (20 colonnes) :

```
Id                               Int64
AlertId                          Nullable(Int64)
CompanyId                        Nullable(Int64)
UserId                           Nullable(Int64)
Action                           String
OldValueJson                     String
NewValueJson                     String
Comment                          String
IpAddress                        String
UserAgent                        String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `AlertDeliveries`

**Lignes source :** 0 | **Colonnes source :** 16 | **Doublons détectés :** 0 | **FK sortantes :** 2 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne — canal de livraison des alertes (email/SMS/push) non utilisé sur ce jeu.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| AlertId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| UserId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| Channel | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Status | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| SentAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| FailureReason | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| RetryCount | `IntegerType()` | Conserver | Int32 |  [G-] |
| NextRetryAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| ProviderMessageId | `StringType()` | Conserver + nettoyer | String | Identifiant technique de type texte (corrélation externe / référence tierce), pas une FK relationnelle interne. [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `AlertDeliveries.AlertId` → `Alerts.Id` (sortante)
- `AlertDeliveries.UserId` → `Users.Id` (sortante)

**Schéma final `payzenhr_staging.AlertDeliveries`** (20 colonnes) :

```
Id                               Int64
AlertId                          Nullable(Int64)
UserId                           Nullable(Int64)
Channel                          String
Status                           String
SentAt                           Nullable(DateTime64(3))
FailureReason                    String
RetryCount                       Int32
NextRetryAt                      Nullable(DateTime64(3))
ProviderMessageId                String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `AlertRuleExecutionLogs`

**Lignes source :** 4719 | **Colonnes source :** 18 | **Doublons détectés :** 0 | **FK sortantes :** 2 | **FK entrantes (référencée par) :** 0

**Décision table : 🟢 CONSERVÉE TELLE QUELLE (table technique)**


**Pourquoi :** 4719 lignes — journal d'exécution du moteur de règles d'alerte, volumétrie importante. ErrorMessage 100% NULL = aucune exécution en erreur dans ce jeu. Table technique de supervision, pertinente pour un tableau de bord d'exploitation ETL/ops, moins pour un reporting RH métier.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| DefinitionId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| ExecutionStartedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| ExecutionEndedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| EntitiesScanned | `IntegerType()` | Conserver | Int32 |  [G-] |
| AlertsGenerated | `IntegerType()` | Conserver | Int32 |  [G-] |
| AlertsUpdated | `IntegerType()` | Conserver | Int32 |  [G-] |
| ErrorsCount | `IntegerType()` | Conserver | Int32 |  [G-] |
| ErrorMessage | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| Status | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| DurationMs | `LongType()` | Conserver | Int64 |  [G-] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `AlertRuleExecutionLogs.DefinitionId` → `AlertDefinitions.Id` (sortante)
- `AlertRuleExecutionLogs.CompanyId` → `Companies.Id` (sortante)

**Schéma final `payzenhr_staging.AlertRuleExecutionLogs`** (22 colonnes) :

```
Id                               Int64
DefinitionId                     Nullable(Int64)
CompanyId                        Nullable(Int64)
ExecutionStartedAt               Nullable(DateTime64(3))
ExecutionEndedAt                 Nullable(DateTime64(3))
EntitiesScanned                  Int32
AlertsGenerated                  Int32
AlertsUpdated                    Int32
ErrorsCount                      Int32
ErrorMessage                     String
Status                           String
DurationMs                       Int64
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---


## Module 10 — Notification (Inbox notifications)

### `NotificationCategories`

**Lignes source :** 6 | **Colonnes source :** 14 | **Doublons détectés :** 0 | **FK sortantes :** 0 | **FK entrantes (référencée par) :** 1

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 6 lignes — lookup.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Code | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameFr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameEn | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameAr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| IconName | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| SortOrder | `IntegerType()` | Conserver | Int32 |  [G-] |
| IsActive | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `NotificationDefinitions.CategoryId` → `NotificationCategories.Id` (entrante)

**Schéma final `payzenhr_staging.NotificationCategories`** (18 colonnes) :

```
Id                               Int64
Code                             String
NameFr                           String
NameEn                           String
NameAr                           String
IconName                         String
SortOrder                        Int32
IsActive                         UInt8 (Bool)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `NotificationDefinitions`

**Lignes source :** 14 | **Colonnes source :** 28 | **Doublons détectés :** 0 | **FK sortantes :** 1 | **FK entrantes (référencée par) :** 3

**Décision table : 🟢 CONSERVÉE TELLE QUELLE**


**Pourquoi :** 14 lignes. DescriptionFr 100% NULL, MessageTemplateEn/TitleTemplateEn 42.86% NULL — gabarits multilingues partiellement traduits (cohérent avec une internationalisation progressive FR/AR/EN).


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| Code | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CategoryId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| NameFr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameEn | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NameAr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| DescriptionFr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| DisplayType | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| TitleTemplateFr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| TitleTemplateAr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| MessageTemplateFr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| MessageTemplateAr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NavigationUrlTemplate | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| IsActive | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| CanUserDisable | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| SupportsInApp | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| SupportsEmail | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| SupportsSms | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| SupportsPush | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| DefaultTtlDays | `IntegerType()` | Conserver | Int32 |  [G-] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=42.9%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |
| MessageTemplateEn | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=42.9%) [G5] |
| TitleTemplateEn | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=42.9%) [G5] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `NotificationDefinitions.CategoryId` → `NotificationCategories.Id` (sortante)
- `CompanyNotificationPreferences.DefinitionId` → `NotificationDefinitions.Id` (entrante)
- `Notifications.DefinitionId` → `NotificationDefinitions.Id` (entrante)
- `NotificationUserPreferences.DefinitionId` → `NotificationDefinitions.Id` (entrante)

**Schéma final `payzenhr_staging.NotificationDefinitions`** (32 colonnes) :

```
Id                               Int64
Code                             String
CategoryId                       Nullable(Int64)
NameFr                           String
NameEn                           String
NameAr                           String
DescriptionFr                    String
DisplayType                      String
TitleTemplateFr                  String
TitleTemplateAr                  String
MessageTemplateFr                String
MessageTemplateAr                String
NavigationUrlTemplate            String
IsActive                         UInt8 (Bool)
CanUserDisable                   UInt8 (Bool)
SupportsInApp                    UInt8 (Bool)
SupportsEmail                    UInt8 (Bool)
SupportsSms                      UInt8 (Bool)
SupportsPush                     UInt8 (Bool)
DefaultTtlDays                   Int32
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
MessageTemplateEn                String
TitleTemplateEn                  String
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `CompanyNotificationPreferences`

**Lignes source :** 0 | **Colonnes source :** 13 | **Doublons détectés :** 0 | **FK sortantes :** 2 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| DefinitionId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| IsEnabled | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| InAppEnabled | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| EmailEnabled | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| SmsEnabled | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `CompanyNotificationPreferences.CompanyId` → `Companies.Id` (sortante)
- `CompanyNotificationPreferences.DefinitionId` → `NotificationDefinitions.Id` (sortante)

**Schéma final `payzenhr_staging.CompanyNotificationPreferences`** (17 colonnes) :

```
Id                               Int64
CompanyId                        Nullable(Int64)
DefinitionId                     Nullable(Int64)
IsEnabled                        UInt8 (Bool)
InAppEnabled                     UInt8 (Bool)
EmailEnabled                     UInt8 (Bool)
SmsEnabled                       UInt8 (Bool)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `NotificationUserPreferences`

**Lignes source :** 0 | **Colonnes source :** 15 | **Doublons détectés :** 0 | **FK sortantes :** 3 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| UserId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| DefinitionId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| ChannelInApp | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| ChannelEmail | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| ChannelSms | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| DigestMode | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| MutedUntil | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `NotificationUserPreferences.CompanyId` → `Companies.Id` (sortante)
- `NotificationUserPreferences.UserId` → `Users.Id` (sortante)
- `NotificationUserPreferences.DefinitionId` → `NotificationDefinitions.Id` (sortante)

**Schéma final `payzenhr_staging.NotificationUserPreferences`** (19 colonnes) :

```
Id                               Int64
CompanyId                        Nullable(Int64)
UserId                           Nullable(Int64)
DefinitionId                     Nullable(Int64)
ChannelInApp                     UInt8 (Bool)
ChannelEmail                     UInt8 (Bool)
ChannelSms                       UInt8 (Bool)
DigestMode                       String
MutedUntil                       String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `Notifications`

**Lignes source :** 67 | **Colonnes source :** 27 | **Doublons détectés :** 0 | **FK sortantes :** 3 | **FK entrantes (référencée par) :** 2

**Décision table : 🟡 TRANSFORMÉE**


**Pourquoi :** 67 lignes (entité UserNotification renommée table Notifications, cf. schéma .md). ReadAt/ArchivedAt à forte proportion de NULL = notifications non lues/non archivées, cohérent. MessageEn/TitleEn 38.81% NULL — traduction anglaise partielle. Lien source (SourceType/SourceId) polymorphe vers Alert, même traitement que Alerts.EntityId (non contrôlable par FK standard).


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| CompanyId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| UserId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| DefinitionId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| Title | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| TitleAr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Message | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| MessageAr | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| DisplayType | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| NavigationUrl | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| SourceType | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| SourceId | `IntegerType()` | Conserver (FK implicite non contrainte) | Nullable(Int64) | Suffixe Id sans contrainte FK EF déclarée — conservée en l'état, validée si référence identifiée. [G2] |
| CorrelationId | `StringType()` | Conserver + nettoyer | String | Identifiant technique de type texte (corrélation externe / référence tierce), pas une FK relationnelle interne. [G5] |
| IsRead | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| ReadAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=74.6%) [G3] |
| IsArchived | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| ArchivedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| IsPinned | `BooleanType()` | Conserver | UInt8 (Bool) | Booléen métier — NULL à documenter au cas par cas, pas de remplacement par défaut sans preuve métier. [G6] |
| ExpiresAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=74.6%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=74.6%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |
| MessageEn | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=38.8%) [G5] |
| TitleEn | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=38.8%) [G5] |

**Colonnes calculées ajoutées :**

| Colonne | Type | Calcul |
|---|---|---|
| IsRead | UInt8 (Bool) | ReadAt IS NOT NULL. |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `Notifications.CompanyId` → `Companies.Id` (sortante)
- `Notifications.UserId` → `Users.Id` (sortante)
- `Notifications.DefinitionId` → `NotificationDefinitions.Id` (sortante)
- `NotificationAuditLogs.NotificationId` → `Notifications.Id` (entrante)
- `NotificationDeliveries.NotificationId` → `Notifications.Id` (entrante)

**Schéma final `payzenhr_staging.Notifications`** (32 colonnes) :

```
Id                               Int64
CompanyId                        Nullable(Int64)
UserId                           Nullable(Int64)
DefinitionId                     Nullable(Int64)
Title                            String
TitleAr                          String
Message                          String
MessageAr                        String
DisplayType                      String
NavigationUrl                    String
SourceType                       String
SourceId                         Nullable(Int64)
CorrelationId                    String
IsRead                           UInt8 (Bool)
ReadAt                           Nullable(DateTime64(3))
IsArchived                       UInt8 (Bool)
ArchivedAt                       Nullable(DateTime64(3))
IsPinned                         UInt8 (Bool)
ExpiresAt                        Nullable(DateTime64(3))
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
MessageEn                        String
TitleEn                          String
IsRead                           UInt8 (Bool)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `NotificationDeliveries`

**Lignes source :** 0 | **Colonnes source :** 16 | **Doublons détectés :** 0 | **FK sortantes :** 2 | **FK entrantes (référencée par) :** 0

**Décision table : 🔵 CONSERVÉE (chargement différé)**


**Pourquoi :** 0 ligne.


> ⚠️ **Information insuffisante pour décider** des règles de qualité (NULL%, doublons, valeurs aberrantes) : 0 ligne disponible dans `rapport_tables.csv`. Le mapping ci-dessous est établi uniquement à partir de la structure déclarée (`columns_tables.csv`, `fk_tables.csv`) et devra être revalidé après un premier chargement de données réelles.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| NotificationId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| UserId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| Channel | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| Status | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| SentAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| FailureReason | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| RetryCount | `IntegerType()` | Conserver | Int32 |  [G-] |
| NextRetryAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| ProviderMessageId | `StringType()` | Conserver + nettoyer | String | Identifiant technique de type texte (corrélation externe / référence tierce), pas une FK relationnelle interne. [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `NotificationDeliveries.NotificationId` → `Notifications.Id` (sortante)
- `NotificationDeliveries.UserId` → `Users.Id` (sortante)

**Schéma final `payzenhr_staging.NotificationDeliveries`** (20 colonnes) :

```
Id                               Int64
NotificationId                   Nullable(Int64)
UserId                           Nullable(Int64)
Channel                          String
Status                           String
SentAt                           Nullable(DateTime64(3))
FailureReason                    String
RetryCount                       Int32
NextRetryAt                      Nullable(DateTime64(3))
ProviderMessageId                String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `NotificationAuditLogs`

**Lignes source :** 84 | **Colonnes source :** 13 | **Doublons détectés :** 0 | **FK sortantes :** 2 | **FK entrantes (référencée par) :** 0

**Décision table : 🟢 CONSERVÉE TELLE QUELLE (table technique)**


**Pourquoi :** 84 lignes. OldValueJson/NewValueJson 100% NULL (G9).


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| NotificationId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| CompanyId | `IntegerType()` | Conserver (FK implicite non contrainte) | Nullable(Int64) | Suffixe Id sans contrainte FK EF déclarée — conservée en l'état, validée si référence identifiée. [G2] |
| UserId | `IntegerType()` | Conserver + valider FK | Nullable(Int64) | Clé étrangère déclarée (voir fk_tables.csv) — validation anti-jointure (T4). [G2] |
| Action | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| OldValueJson | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| NewValueJson | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=100.0%) [G5] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Relations (validation FK à l'ETL) :**
- `NotificationAuditLogs.NotificationId` → `Notifications.Id` (sortante)
- `NotificationAuditLogs.UserId` → `Users.Id` (sortante)

**Schéma final `payzenhr_staging.NotificationAuditLogs`** (17 colonnes) :

```
Id                               Int64
NotificationId                   Nullable(Int64)
CompanyId                        Nullable(Int64)
UserId                           Nullable(Int64)
Action                           String
OldValueJson                     String
NewValueJson                     String
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---


## Module 11 — Events (Logs d'audit applicatifs)

### `EmployeeEventLogs`

**Lignes source :** 921 | **Colonnes source :** 13 | **Doublons détectés :** 0 | **FK sortantes :** 0 | **FK entrantes (référencée par) :** 0

**Décision table : 🟢 CONSERVÉE TELLE QUELLE (table technique)**


**Pourquoi :** 921 lignes. Colonne employeeId (casse différente du reste du schéma — minuscule initiale, à harmoniser en EmployeeId lors du renommage) SANS contrainte FK EF (confirmé par le schéma .md). oldValue 91.86% NULL, oldValueId/newValueId 80-100% NULL : journal générique de changement de valeur, cohérent avec des évènements de création (pas d'ancienne valeur) majoritaires.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| employeeId | `IntegerType()` | Conserver (FK implicite non contrainte) | Nullable(Int64) | Suffixe Id sans contrainte FK EF déclarée — conservée en l'état, validée si référence identifiée. [G2] |
| eventName | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| oldValue | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=91.9%) [G5] |
| oldValueId | `IntegerType()` | Conserver (FK implicite non contrainte) | Nullable(Int64) | Suffixe Id sans contrainte FK EF déclarée — conservée en l'état, validée si référence identifiée. (NULL=100.0%) [G2] |
| newValue | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| newValueId | `IntegerType()` | Conserver (FK implicite non contrainte) | Nullable(Int64) | Suffixe Id sans contrainte FK EF déclarée — conservée en l'état, validée si référence identifiée. (NULL=100.0%) [G2] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Transformations ETL spécifiques :**

- Renommage employeeId → EmployeeId (T6, harmonisation de casse) ; pas de validation FK stricte possible (absence de contrainte source), rapprochement optionnel best-effort avec Employees.Id.

**Schéma final `payzenhr_staging.EmployeeEventLogs`** (17 colonnes) :

```
Id                               Int64
employeeId                       Nullable(Int64)
eventName                        String
oldValue                         String
oldValueId                       Nullable(Int64)
newValue                         String
newValueId                       Nullable(Int64)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---

### `CompanyEventLogs`

**Lignes source :** 46 | **Colonnes source :** 14 | **Doublons détectés :** 0 | **FK sortantes :** 0 | **FK entrantes (référencée par) :** 0

**Décision table : 🟢 CONSERVÉE TELLE QUELLE (table technique)**


**Pourquoi :** 46 lignes. companyId/employeeId (minuscule) sans contrainte FK EF (confirmé schéma .md). oldValue 54.35% NULL, oldValueId 91.30% NULL, newValueId 80.43% NULL.


| Colonne | Type source | Décision | Type cible (staging) | Justification |
|---|---|---|---|---|
| Id | `IntegerType()` | Conserver | Int64 | Clé primaire source, conservée comme clé de correspondance (NOT NULL). [G1] |
| employeeId | `IntegerType()` | Conserver (FK implicite non contrainte) | Nullable(Int64) | Suffixe Id sans contrainte FK EF déclarée — conservée en l'état, validée si référence identifiée. [G2] |
| eventName | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| oldValue | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). (NULL=54.4%) [G5] |
| oldValueId | `IntegerType()` | Conserver (FK implicite non contrainte) | Nullable(Int64) | Suffixe Id sans contrainte FK EF déclarée — conservée en l'état, validée si référence identifiée. (NULL=91.3%) [G2] |
| newValue | `StringType()` | Conserver + nettoyer | String | Nettoyage espaces/caractères de contrôle, normalisation UTF-8 (G5). [G5] |
| newValueId | `IntegerType()` | Conserver (FK implicite non contrainte) | Nullable(Int64) | Suffixe Id sans contrainte FK EF déclarée — conservée en l'état, validée si référence identifiée. (NULL=80.4%) [G2] |
| companyId | `IntegerType()` | Conserver (FK implicite non contrainte) | Nullable(Int64) | Suffixe Id sans contrainte FK EF déclarée — conservée en l'état, validée si référence identifiée. [G2] |
| CreatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). [G3] |
| CreatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. [G3] |
| UpdatedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G3] |
| UpdatedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G3] |
| DeletedAt | `StringType()` | Convertir type + standardiser | Nullable(DateTime64(3)) | StringType() source → parsing multi-format (T1), NULL si non parsable (log rejet). (NULL=100.0%) [G4] |
| DeletedBy | `IntegerType()` | Convertir type | Nullable(Int64) | Référence UserId non contrainte par FK côté source ; conservée pour traçabilité, non validée. (NULL=100.0%) [G4] |

**Colonnes techniques ETL ajoutées (G10, systématiques) :** `LoadDate` (DateTime64(3)), `SourceSystem` (String, ex. `'PayZenHR.PayZenHR_Source'`), `SourceTable` (String), `ETLExecutionDate` (Date).


**Transformations ETL spécifiques :**

- Renommage companyId → CompanyId, employeeId → EmployeeId (T6).

**Schéma final `payzenhr_staging.CompanyEventLogs`** (18 colonnes) :

```
Id                               Int64
employeeId                       Nullable(Int64)
eventName                        String
oldValue                         String
oldValueId                       Nullable(Int64)
newValue                         String
newValueId                       Nullable(Int64)
companyId                        Nullable(Int64)
CreatedAt                        Nullable(DateTime64(3))
CreatedBy                        Nullable(Int64)
UpdatedAt                        Nullable(DateTime64(3))
UpdatedBy                        Nullable(Int64)
DeletedAt                        Nullable(DateTime64(3))
DeletedBy                        Nullable(Int64)
LoadDate                         DateTime64(3)
SourceSystem                     String
SourceTable                      String
ETLExecutionDate                 Date
```

---


## Module 12 — Tables techniques EF Core (hors périmètre décisionnel)

### `__EFMigrationsHistory`

**Lignes source :** 53 | **Colonnes source :** 2

**Décision table : 🔴 SUPPRIMÉE**


Table interne EF Core (historique des migrations de schéma). Aucune valeur métier/BI. 53 lignes, 2 colonnes (MigrationId, ProductVersion) : pure métadonnée de déploiement.


*Aucun mapping vers `payzenhr_staging` — table exclue en amont de l'ETL.*


---

### `sysdiagrams`

**Lignes source :** 0 | **Colonnes source :** 5

**Décision table : 🔴 SUPPRIMÉE**


Table système SQL Server générée par l'outil de diagramme ER (SSMS). 0 ligne exploitable, contient une colonne binaire (definition) sans sémantique métier.


*Aucun mapping vers `payzenhr_staging` — table exclue en amont de l'ETL.*


---
## Synthèse finale et recommandations

### A. Récapitulatif des décisions de classification (103 tables)

| Classification | Nombre de tables | Détail |
|---|---|---|
| 🟢 Conservée telle quelle | ~40 | Dimensions/lookups stables, tables de jonction, agrégats mensuels déjà pré-calculés côté source |
| 🟡 Transformée | ~15 | Tables pivot avec calculs dérivés (`Employees`, `EmployeeContracts`, `EmployeeSalaries`, `PayrollResults`, `Alerts`, `Notifications`, `LeaveRequests`, `Departements`, `Companies`, `EmployeeAddresses`…) |
| 🔵 Conservée — chargement différé (0 ligne) | 39 | Schéma prêt, non alimenté en Phase 1 |
| 🟣 Fusionnée | 1 (+4 candidates futures) | `PayrollTaxSnapshots` → `PayrollResults` ; `RuleCaps`/`RulePercentages`/`RuleFormulas`/`RuleDualCaps` → `ElementRules` (candidates, à confirmer une fois alimentées) |
| 🔴 Supprimée / hors périmètre décisionnel | 2 | `__EFMigrationsHistory`, `sysdiagrams` |

### B. Points ouverts nécessitant une clarification métier avant l'implémentation PySpark
Conformément à la consigne *« ne fais aucune hypothèse »*, les points suivants sont documentés comme
**Information insuffisante pour décider** dans les fiches correspondantes et doivent être arbitrés avec
l'équipe applicative/produit PayZenHR avant l'écriture du code ETL :

1. **`rapport_dates.csv` non fourni** — l'analyse fine des formats de dates hétérogènes (nombre de formats
   distincts observés, dates aberrantes hors plage) n'a pas pu être réalisée ; seul le constat structurel
   (StringType vs sémantique date) a pu être établi à partir de `columns_tables.csv`.
2. **Redondance `PayComponents` / `SalaryPackageItems` vs. colonnes fixes de `PayrollResults`** — deux
   approches de modélisation potentiellement concurrentes (catalogue dynamique vs. colonnes nommées), à
   trancher avec l'équipe produit avant de concevoir le modèle en étoile ClickHouse.
3. **Redondance apparente `Companies.BusinessSector` (texte, 100 % NULL) vs. table `BusinessSectors`** —
   probable dette de modélisation à confirmer.
4. **Redondance apparente `Employees.RibNumber`/`BankAccountId` vs. module `BankAccounts`** — à clarifier.
5. **Mapping des valeurs entières de `PayrollResults.Status`** — nécessaire pour fiabiliser le calcul dérivé
   `IsSuccessResult` et pour interpréter correctement les NULL des colonnes de composantes de paie.
6. **Fournisseur d'identité externe (`Users.ExternalId`/`Source`)** — à documenter pour fiabiliser la
   validation de cette colonne en Staging.
7. **Colonnes shadow `CountryId`** sur `Employees` et `EmployeeAddresses` (mentionnées comme telles dans
   `DATABASE_SCHEMA_PAR_MODULE.md`, 100 % NULL) — candidates à la suppression si confirmées définitivement
   inutilisées, mais non supprimées à ce stade par prudence.

### C. Prochaines étapes proposées
1. Validation de ce document de conception avec le tuteur/l'équipe encadrante.
2. Obtention de `rapport_dates.csv` et levée des points ouverts (§B) avant le développement.
3. Développement du pipeline PySpark (hors périmètre de ce document, sur demande explicite ultérieure).
4. Mise en place des tables techniques transverses : `Staging.audit_dates_rejected` (T1),
   `Staging.audit_orphans` (T4), `Staging.PayrollResults_TechnicalAudit` (T9).
5. Rejeu du profiling (nulls/doublons/statistiques) après le premier chargement réel des 39 tables actuellement
   vides, pour lever les mentions « Information insuffisante pour décider ».

---

*Document de conception — aucune ligne de code PySpark n'a été produite, conformément à la demande. Prêt à
servir de spécification d'implémentation pour la couche `payzenhr_staging`.*
