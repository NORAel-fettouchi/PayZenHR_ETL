# Workflow général ETL / BI — PayZenHR

## 1. Objectif

Ce document présente le workflow général mis en place pour le projet **PayZenHR**, depuis la base de données source jusqu'à Power BI.

L'architecture suit le flux suivant :

```text
SQL Server
   │
   │ Extraction via JDBC
   ▼
PySpark
   │
   │ Nettoyage / Transformation / Préparation
   ▼
Docker
   │
   └── ClickHouse
          │
          │ Base analytique
          ▼
       payzen_bi
          │
          │ Connexion BI
          ▼
       Power BI
          │
          ▼
      Dashboards BI
```

---

# 2. Architecture générale

Le pipeline est composé de quatre grandes couches :

1. **SQL Server** : source des données PayZenHR
2. **PySpark** : extraction, nettoyage et transformation des données
3. **Docker + ClickHouse** : exécution et stockage analytique des données préparées
4. **Power BI** : modélisation, analyse et visualisation

```text
┌──────────────────────────────┐
│        SQL Server            │
│      PayZenHR_Source         │
│                              │
│  Base transactionnelle       │
│  Données RH / Paie           │
└──────────────┬───────────────┘
               │
               │ JDBC / READ ONLY
               ▼
┌──────────────────────────────┐
│           PySpark            │
│                              │
│ Extraction                   │
│ Nettoyage                    │
│ Transformation               │
│ Préparation BI               │
└──────────────┬───────────────┘
               │
               │ JDBC
               ▼
┌──────────────────────────────────┐
│             Docker               │
│                                  │
│   ┌──────────────────────────┐   │
│   │       ClickHouse         │   │
│   │                          │   │
│   │       payzen_bi          │   │
│   │                          │   │
│   │  5 dimensions            │   │
│   │  2 tables de faits       │   │
│   └────────────┬─────────────┘   │
└────────────────┼─────────────────┘
                 │
                 │ ODBC
                 ▼
┌──────────────────────────────┐
│          Power BI            │
│                              │
│ Modèle de données            │
│ Relations                    │
│ Mesures / KPI                │
│ Visualisations               │
│ Dashboards                   │
└──────────────────────────────┘
```

---

# 3. SQL Server — base de données source

La base source du projet est **SQL Server**.

La base utilisée dans le processus ETL est :

```text
PayZenHR_Source
```

Elle contient les données opérationnelles provenant de PayZenHR.

SQL Server constitue la **source de données** du pipeline.

## Rôle de SQL Server

SQL Server sert principalement à :

* conserver les données sources ;
* fournir les données au processus ETL ;
* permettre à PySpark de récupérer les données nécessaires ;
* conserver les données dans leur structure opérationnelle d'origine.

Le principe est :

```text
SQL Server
     │
     │ READ ONLY
     ▼
  PySpark
```

Le pipeline lit les données de SQL Server sans modifier la base source.

Les opérations de transformation sont réalisées en dehors de la base source, principalement dans PySpark.

---

# 4. Connexion SQL Server → PySpark

PySpark communique avec SQL Server à travers **JDBC**.

JDBC signifie :

> Java Database Connectivity

Le driver JDBC SQL Server permet à Spark de communiquer avec la base SQL Server.

Le flux est :

```text
PySpark
   │
   │ JDBC
   ▼
SQL Server
```

Dans l'environnement du projet, le driver SQL Server utilisé est notamment :

```text
mssql-jdbc-13.4.0.jre8.jar
```

Grâce à ce driver, Spark peut effectuer des lectures JDBC sur SQL Server.

---

# 5. PySpark — couche ETL

**PySpark** constitue le moteur ETL du projet.

ETL signifie :

```text
E = Extract
T = Transform
L = Load
```

Dans le projet PayZenHR :

```text
Extract
   ↓
SQL Server

Transform
   ↓
PySpark

Load
   ↓
ClickHouse
```

PySpark permet de :

* récupérer les données depuis SQL Server ;
* charger les données dans des DataFrames Spark ;
* nettoyer les données ;
* transformer les données ;
* préparer les dimensions ;
* préparer les tables de faits ;
* charger les données préparées dans ClickHouse.

---

# 6. Étape Extract — extraction

La première étape consiste à lire les données nécessaires depuis SQL Server.

Le processus est conceptuellement :

```text
SQL Server
    │
    ├── Données clients
    ├── Données employés
    ├── Données paie
    ├── Données absences
    ├── Données calendrier
    └── Autres données nécessaires
          │
          ▼
       PySpark
```

PySpark charge les données dans des **Spark DataFrames**.

Conceptuellement, on peut avoir par exemple :

```text
df_clients
df_employees
df_payroll
df_absences
...
```

Ces DataFrames constituent la base des transformations réalisées par la suite.

---

# 7. Étape Transform — nettoyage et transformation

Une fois les données extraites, PySpark applique les traitements nécessaires avant leur chargement dans ClickHouse.

Les traitements peuvent comprendre :

* sélection des colonnes nécessaires ;
* renommage des colonnes ;
* conversion des types de données ;
* traitement des valeurs NULL ;
* nettoyage des données ;
* gestion des doublons ;
* normalisation des formats ;
* préparation des clés ;
* filtrage des données ;
* jointures entre différentes sources ;
* calcul de certains attributs ;
* préparation des dimensions ;
* préparation des tables de faits.

Le principe est :

```text
Données sources
      │
      ▼
Nettoyage
      │
      ▼
Transformation
      │
      ▼
Données préparées pour le BI
```

---

# 8. Construction des dimensions

Après transformation, PySpark prépare les dimensions du modèle BI.

Les dimensions utilisées dans le modèle sont :

```text
dim_clients
dim_employees
dim_calendar
dim_composant_paie
dim_echeance_legale
```

Le flux est donc :

```text
SQL Server
      │
      ▼
   PySpark
      │
      ├── dim_clients
      ├── dim_employees
      ├── dim_calendar
      ├── dim_composant_paie
      └── dim_echeance_legale
```

Ces tables sont ensuite chargées dans ClickHouse.

---

# 9. Construction des tables de faits

PySpark prépare également les tables de faits nécessaires au modèle BI.

Les tables de faits utilisées sont :

```text
fact_bulletin_ligne
fact_absence
```

Le résultat côté PySpark est donc :

```text
                     PySpark
                        │
          ┌─────────────┴──────────────┐
          │                            │
      Dimensions                    Faits
          │                            │
          ├── dim_clients              ├── fact_bulletin_ligne
          ├── dim_employees            └── fact_absence
          ├── dim_calendar
          ├── dim_composant_paie
          └── dim_echeance_legale
```

---

# 10. Docker — rôle dans l'architecture

Docker n'est pas une base de données et ne réalise pas les transformations ETL.

Dans ce projet, **Docker sert principalement à exécuter et isoler ClickHouse dans un conteneur**.

L'architecture est :

```text
Windows
   │
   ▼
Docker Desktop
   │
   ▼
Conteneur ClickHouse
   │
   ▼
Base payzen_bi
```

Docker permet notamment :

* d'isoler l'environnement ClickHouse ;
* de faciliter son installation et son démarrage ;
* de gérer son environnement d'exécution ;
* d'exposer les ports nécessaires ;
* de rendre l'environnement plus facilement reproductible ;
* de maintenir ClickHouse séparé du système principal.

Il faut donc bien distinguer les rôles :

```text
Docker      = conteneurisation / environnement d'exécution
ClickHouse  = moteur de base de données analytique
PySpark     = ETL / transformation
```

---

# 11. ClickHouse dans Docker

ClickHouse fonctionne dans un conteneur Docker.

On peut représenter l'environnement comme suit :

```text
Docker Desktop
└── ClickHouse Container
      │
      └── payzen_bi
            │
            ├── dim_clients
            ├── dim_employees
            ├── dim_calendar
            ├── dim_composant_paie
            ├── dim_echeance_legale
            ├── fact_bulletin_ligne
            └── fact_absence
```

ClickHouse constitue la **couche analytique / Data Warehouse** du projet.

---

# 12. Ports et accès à ClickHouse

ClickHouse peut être accessible par différents protocoles.

Dans l'environnement du projet, les ports utilisés sont notamment :

```text
8124 → HTTP
9001 → Native / JDBC
```

Le mapping exact dépend de la configuration Docker.

Le principe peut être représenté ainsi :

```text
Machine Windows
       │
       ├── localhost:8124
       │
       └── localhost:9001
               │
               ▼
         ClickHouse
```

Ces ports permettent aux différentes applications de communiquer avec ClickHouse.

---

# 13. PySpark → ClickHouse

Une fois les transformations terminées, PySpark charge les DataFrames préparés dans ClickHouse.

Le flux est :

```text
SQL Server
     │
     │ Extraction
     ▼
 PySpark
     │
     │ Transformation
     ▼
DataFrames préparés
     │
     │ JDBC
     ▼
 ClickHouse
     │
     ▼
 payzen_bi
```

Exemples conceptuels :

```text
df_employees
      │
      ▼
dim_employees
```

```text
df_absences
      │
      ▼
fact_absence
```

Le chargement permet donc de transférer les données transformées depuis la couche ETL vers la couche analytique.

---

# 14. Base ClickHouse `payzen_bi`

La base analytique utilisée dans ClickHouse est :

```text
payzen_bi
```

Elle contient actuellement les tables suivantes.

## Dimensions

```text
payzen_bi.dim_clients
payzen_bi.dim_employees
payzen_bi.dim_calendar
payzen_bi.dim_composant_paie
payzen_bi.dim_echeance_legale
```

## Tables de faits

```text
payzen_bi.fact_bulletin_ligne
payzen_bi.fact_absence
```

Le modèle peut être résumé ainsi :

```text
                  ClickHouse
                      │
                  payzen_bi
                      │
          ┌───────────┴───────────┐
          │                       │
     Dimensions                 Faits
          │                       │
       5 tables                2 tables
```

---

# 15. Pourquoi ClickHouse ?

ClickHouse est utilisé comme couche analytique entre la base transactionnelle et Power BI.

La séparation est donc :

```text
SQL Server
= source opérationnelle

PySpark
= traitement ETL

ClickHouse
= stockage analytique / Data Warehouse

Power BI
= analyse et visualisation
```

Cette architecture évite de faire reposer directement les analyses BI sur la base transactionnelle source.

On sépare ainsi :

```text
Base opérationnelle
        ≠
Base analytique
```

---

# 16. Gestion du chargement

Le principe du pipeline est de lire les données depuis SQL Server, de les transformer avec PySpark, puis de charger les données préparées dans ClickHouse.

```text
SQL Server
     │
     │ READ ONLY
     ▼
  PySpark
     │
     │ Transformation
     ▼
 ClickHouse
     │
     │ INSERT / LOAD
     ▼
 payzen_bi
```

Le processus peut également vérifier les clés déjà présentes dans ClickHouse afin d'éviter les duplications lors des chargements.

La base SQL Server reste la source et n'est pas modifiée par le processus ETL.

---

# 17. ClickHouse → Power BI

Après le chargement des données, Power BI se connecte à ClickHouse.

Le flux final est :

```text
ClickHouse
     │
     │ Connexion BI / ODBC
     ▼
 Power BI
```

Power BI récupère les tables du modèle :

```text
dim_clients
dim_employees
dim_calendar
dim_composant_paie
dim_echeance_legale
fact_bulletin_ligne
fact_absence
```

---

# 18. Modélisation dans Power BI

Une fois les données disponibles dans Power BI, la partie BI peut être réalisée.

Les principales étapes sont :

## 18.1 Connexion aux données

Power BI se connecte à ClickHouse à travers le connecteur/protocole approprié.

```text
Power BI
    │
    ▼
ClickHouse
```

## 18.2 Chargement des tables

Les dimensions et tables de faits sont récupérées.

## 18.3 Création des relations

Les relations entre les dimensions et les tables de faits sont définies selon le modèle de données.

## 18.4 Création des mesures

Des mesures DAX peuvent être créées pour calculer les indicateurs nécessaires.

Exemples :

```text
Total Brut
Nombre d'employés
Nombre d'absences
Masse salariale
```

## 18.5 Création des filtres

Exemples :

```text
Année
Mois
Client
Employé
```

## 18.6 Création des visualisations

Power BI permet ensuite de créer :

```text
KPI
Graphiques
Tableaux
Courbes
Slicers
Cartes
```

## 18.7 Construction des dashboards

Les visualisations sont finalement organisées dans les dashboards correspondant aux besoins métier.

---

# 19. Workflow technique complet

Le workflow de bout en bout est donc :

```text
┌───────────────────────────────────────────────┐
│                  1. SOURCE                    │
│                                               │
│                SQL Server                     │
│              PayZenHR_Source                  │
│                                               │
│       Données transactionnelles               │
└───────────────────────┬───────────────────────┘
                        │
                        │ JDBC
                        │ READ ONLY
                        ▼
┌───────────────────────────────────────────────┐
│                    2. ETL                     │
│                                               │
│                  PySpark                      │
│                                               │
│  Extraction                                   │
│      ↓                                        │
│  DataFrames                                   │
│      ↓                                        │
│  Nettoyage                                    │
│      ↓                                        │
│  Transformation                               │
│      ↓                                        │
│  Construction dimensions + faits              │
└───────────────────────┬───────────────────────┘
                        │
                        │ JDBC
                        ▼
┌───────────────────────────────────────────────┐
│              3. DATA WAREHOUSE                │
│                                               │
│                   Docker                      │
│                      │                        │
│                      ▼                        │
│                 ClickHouse                    │
│                      │                        │
│                  payzen_bi                    │
│                                               │
│  dim_clients                                  │
│  dim_employees                                │
│  dim_calendar                                 │
│  dim_composant_paie                           │
│  dim_echeance_legale                          │
│  fact_bulletin_ligne                          │
│  fact_absence                                 │
└───────────────────────┬───────────────────────┘
                        │
                        │ ODBC / connexion BI
                        ▼
┌───────────────────────────────────────────────┐
│                 4. BI                         │
│                                               │
│                 Power BI                      │
│                                               │
│  Connexion                                    │
│      ↓                                        │
│  Chargement des tables                        │
│      ↓                                        │
│  Modélisation                                 │
│      ↓                                        │
│  Relations                                    │
│      ↓                                        │
│  Mesures DAX                                  │
│      ↓                                        │
│  KPI / Visualisations                         │
│      ↓                                        │
│  Dashboards                                   │
└───────────────────────────────────────────────┘
```

---

# 20. Flux des données

Le flux des données peut être résumé en quatre étapes principales :

### Étape 1 — Source

```text
SQL Server
PayZenHR_Source
```

Les données métier sont stockées dans la base source.

### Étape 2 — ETL

```text
SQL Server
      ↓
    JDBC
      ↓
  PySpark
      ↓
Extraction + Transformation
```

PySpark récupère et prépare les données.

### Étape 3 — Data Warehouse

```text
PySpark
    ↓
  JDBC
    ↓
ClickHouse
    ↓
payzen_bi
```

Les données préparées sont stockées dans la couche analytique.

### Étape 4 — BI

```text
ClickHouse
     ↓
Connexion BI
     ↓
Power BI
     ↓
Modèle
     ↓
KPI / Visualisations
     ↓
Dashboards
```

---

# 21. Technologies utilisées

| Étape                              | Technologie         | Rôle                                    |
| ---------------------------------- | ------------------- | --------------------------------------- |
| Source                             | SQL Server          | Base de données transactionnelle source |
| Base source                        | PayZenHR_Source     | Données opérationnelles PayZenHR        |
| ETL                                | PySpark             | Extraction et transformation            |
| Communication SQL Server → PySpark | JDBC                | Connexion aux données source            |
| Conteneurisation                   | Docker              | Exécution et isolation de ClickHouse    |
| Stockage BI                        | ClickHouse          | Data Warehouse / couche analytique      |
| Base BI                            | payzen_bi           | Base ClickHouse destinée au BI          |
| Communication PySpark → ClickHouse | JDBC                | Chargement des données                  |
| Accès Power BI → ClickHouse        | ODBC / connexion BI | Accès aux données analytiques           |
| Restitution                        | Power BI            | Analyse et dashboards                   |
| Calculs BI                         | DAX                 | Mesures et KPI                          |

---

# 22. Rôle de chaque composant

| Composant               | Rôle                                                             |
| ----------------------- | ---------------------------------------------------------------- |
| **SQL Server**          | Stockage des données sources PayZenHR                            |
| **PayZenHR_Source**     | Base SQL Server utilisée comme source                            |
| **JDBC SQL Server**     | Communication entre PySpark et SQL Server                        |
| **PySpark**             | Extraction, nettoyage, transformation et préparation des données |
| **Docker**              | Conteneurisation et exécution de ClickHouse                      |
| **ClickHouse**          | Stockage analytique / Data Warehouse                             |
| **payzen_bi**           | Base ClickHouse destinée au BI                                   |
| **JDBC ClickHouse**     | Chargement des données depuis PySpark vers ClickHouse            |
| **ODBC / connexion BI** | Accès de Power BI aux données ClickHouse                         |
| **Power BI**            | Modélisation, analyse et visualisation                           |
| **DAX**                 | Création des mesures et KPI                                      |

---

# 23. Ce qui se passe lors d'une exécution du pipeline

Lorsqu'on lance le processus ETL, le déroulement général est le suivant :

```text
1. Démarrage du pipeline
        │
        ▼
2. Connexion à SQL Server
        │
        ▼
3. Lecture des données sources
        │
        ▼
4. Création des DataFrames PySpark
        │
        ▼
5. Nettoyage des données
        │
        ▼
6. Transformation des données
        │
        ▼
7. Préparation des dimensions
        │
        ▼
8. Préparation des tables de faits
        │
        ▼
9. Connexion à ClickHouse
        │
        ▼
10. Chargement dans payzen_bi
        │
        ▼
11. Vérification des données
        │
        ▼
12. ClickHouse devient la source analytique
        │
        ▼
13. Power BI se connecte à ClickHouse
        │
        ▼
14. Modélisation / KPI / Visualisations
        │
        ▼
15. Dashboards BI
```

---

# 24. Architecture finale

L'architecture complète du projet peut être représentée comme suit :

```text
                         PAYZENHR
                            │
                            ▼
              ┌─────────────────────────┐
              │       SQL SERVER        │
              │                         │
              │     PayZenHR_Source     │
              │                         │
              │  Base transactionnelle  │
              └────────────┬────────────┘
                           │
                           │ JDBC
                           │ READ ONLY
                           ▼
              ┌─────────────────────────┐
              │        PYSPARK          │
              │                         │
              │       Extraction        │
              │          ↓              │
              │       Nettoyage         │
              │          ↓              │
              │     Transformation      │
              │          ↓              │
              │   Préparation BI        │
              └────────────┬────────────┘
                           │
                           │ JDBC
                           ▼
              ┌─────────────────────────┐
              │         DOCKER          │
              │                         │
              │  ┌───────────────────┐  │
              │  │    CLICKHOUSE     │  │
              │  │                   │  │
              │  │     payzen_bi     │  │
              │  │                   │  │
              │  │  5 dimensions     │  │
              │  │  2 tables faits   │  │
              │  └─────────┬─────────┘  │
              └────────────┼────────────┘
                           │
                           │ ODBC
                           ▼
              ┌─────────────────────────┐
              │        POWER BI         │
              │                         │
              │      Connexion          │
              │          ↓              │
              │     Modélisation       │
              │          ↓              │
              │      Mesures DAX       │
              │          ↓              │
              │       KPI / Graphs      │
              │          ↓              │
              │       Dashboards        │
              └─────────────────────────┘
```

---

# 25. Résumé

Le pipeline PayZenHR repose sur une séparation claire des responsabilités :

```text
SQL Server
     ↓
SOURCE
     ↓
PySpark
     ↓
ETL
     ↓
ClickHouse
     ↓
DATA WAREHOUSE
     ↓
Power BI
     ↓
BI / DASHBOARDS
```

### En une phrase

> **SQL Server constitue la source opérationnelle, PySpark réalise l'ETL en extrayant, nettoyant et transformant les données, Docker héberge le conteneur ClickHouse, ClickHouse constitue la couche analytique `payzen_bi`, puis Power BI se connecte à ClickHouse pour réaliser la modélisation, les KPI et les dashboards.**

### Vue très simplifiée

```text
SQL Server
   │
   │ Extract
   ▼
PySpark
   │
   │ Transform
   ▼
ClickHouse
   │
   │ Store / Analyze
   ▼
Power BI
   │
   │ Visualize
   ▼
Dashboards
```
