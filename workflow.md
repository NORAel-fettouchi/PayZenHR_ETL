# Workflow général ETL / BI

## 1. Introduction

Ce document décrit un workflow général de traitement et d'exploitation des données, depuis une base de données source jusqu'à la visualisation dans Power BI.

L'architecture repose sur plusieurs composants :

* **SQL Server** : base de données source ;
* **PySpark** : moteur de traitement ETL ;
* **Docker** : environnement de conteneurisation ;
* **ClickHouse** : base de données analytique ;
* **Power BI** : outil de Business Intelligence et de visualisation.

Le flux global est :

```text
SQL Server
     │
     │ Extraction
     ▼
  PySpark
     │
     │ Transformation
     ▼
  ClickHouse
     │
     │ Connexion BI
     ▼
  Power BI
     │
     ▼
 Dashboards
```

---

# 2. Vue globale de l'architecture

L'architecture peut être divisée en quatre couches principales :

```text
┌──────────────────────────────────────┐
│          SOURCE DE DONNÉES           │
│                                      │
│             SQL Server               │
│                                      │
│       Données opérationnelles        │
└──────────────────┬───────────────────┘
                   │
                   │ JDBC
                   ▼
┌──────────────────────────────────────┐
│              ETL                     │
│                                      │
│             PySpark                  │
│                                      │
│  Extraction → Nettoyage →            │
│  Transformation → Préparation        │
└──────────────────┬───────────────────┘
                   │
                   │ JDBC
                   ▼
┌──────────────────────────────────────┐
│       STOCKAGE ANALYTIQUE            │
│                                      │
│       Docker + ClickHouse            │
│                                      │
│       Données préparées pour BI      │
└──────────────────┬───────────────────┘
                   │
                   │ Connexion BI
                   ▼
┌──────────────────────────────────────┐
│          BUSINESS INTELLIGENCE       │
│                                      │
│             Power BI                 │
│                                      │
│  Modélisation → KPI → Visualisation  │
└──────────────────────────────────────┘
```

---

# 3. SQL Server — source des données

SQL Server représente la **source de données** du système.

Il peut contenir les données provenant d'une application métier ou d'un système opérationnel.

Exemples de données pouvant être présentes :

```text
Clients
Employés
Transactions
Ventes
Paie
Absences
Produits
Commandes
Dates
etc.
```

Le rôle de SQL Server dans cette architecture est principalement de fournir les données nécessaires au processus ETL.

```text
SQL Server
     │
     │ Lecture
     ▼
  Processus ETL
```

La base source doit généralement être considérée comme une source opérationnelle.

Le traitement analytique est réalisé dans les couches suivantes afin de limiter l'impact sur le système opérationnel.

---

# 4. Connexion entre SQL Server et PySpark

PySpark doit pouvoir communiquer avec SQL Server afin de récupérer les données.

Cette communication est généralement réalisée à l'aide de **JDBC**.

JDBC signifie :

> Java Database Connectivity

Le JDBC Driver permet à Spark de communiquer avec SQL Server.

Le flux est :

```text
             JDBC
SQL Server ──────────► PySpark
```

Le driver adapté au système de base de données doit être disponible dans l'environnement Spark.

---

# 5. PySpark — moteur ETL

PySpark constitue la couche principale de traitement des données.

ETL signifie :

```text
Extract
Transform
Load
```

PySpark est particulièrement adapté lorsque les données sont volumineuses ou lorsque plusieurs transformations doivent être réalisées.

Le processus général est :

```text
SQL Server
     │
     │ Extract
     ▼
 PySpark
     │
     │ Transform
     ▼
DataFrames préparés
     │
     │ Load
     ▼
ClickHouse
```

---

# 6. Étape Extract

La première étape consiste à récupérer les données depuis la source.

PySpark se connecte à SQL Server et lit les données nécessaires.

```text
SQL Server
    │
    ├── Table A
    ├── Table B
    ├── Table C
    ├── Table D
    └── ...
          │
          ▼
       PySpark
```

Les données sont chargées dans des **DataFrames Spark**.

Les DataFrames permettent ensuite d'effectuer les traitements nécessaires.

---

# 7. Étape Transform

Après l'extraction, les données sont nettoyées et transformées.

Cette étape peut comprendre :

### Nettoyage

* traitement des valeurs NULL ;
* suppression ou gestion des doublons ;
* correction des formats ;
* gestion des valeurs incorrectes.

### Transformation

* changement des types de données ;
* renommage des colonnes ;
* filtrage ;
* calcul de nouvelles valeurs ;
* normalisation ;
* agrégation.

### Jointures

Plusieurs sources peuvent être combinées :

```text
Table A
    +
Table B
    +
Table C
    │
    ▼
DataFrame transformé
```

### Préparation du modèle analytique

Les données peuvent ensuite être organisées sous une forme adaptée à l'analyse :

```text
Dimensions
     +
Tables de faits
     ↓
Modèle analytique
```

---

# 8. Étape Load

Après transformation, les données sont prêtes à être chargées dans la base analytique.

Le flux devient :

```text
PySpark
    │
    │ Données transformées
    ▼
ClickHouse
```

La couche ETL est donc responsable de préparer les données avant leur stockage analytique.

---

# 9. Docker — rôle dans l'architecture

Docker est une technologie de **conteneurisation**.

Docker n'est pas le moteur ETL et n'est pas la base de données analytique.

Il fournit un environnement isolé permettant notamment d'exécuter des services comme ClickHouse.

Le principe est :

```text
Machine hôte
     │
     ▼
Docker
     │
     └── Conteneur
             │
             └── ClickHouse
```

---

# 10. Pourquoi utiliser Docker ?

Docker permet notamment :

* d'isoler les applications ;
* de simplifier l'installation ;
* de gérer les dépendances ;
* de faciliter le démarrage et l'arrêt des services ;
* de rendre l'environnement plus reproductible ;
* de faciliter le déploiement.

Par exemple :

```text
Docker
│
├── Conteneur ClickHouse
├── Réseau
├── Volumes
└── Configuration
```

Le conteneur permet à ClickHouse de fonctionner indépendamment du reste du système.

---

# 11. ClickHouse — base analytique

ClickHouse est utilisé comme **base de données analytique**.

Contrairement à la base source, qui est principalement orientée vers les opérations métier, ClickHouse est destiné à stocker des données préparées pour l'analyse.

Le principe est :

```text
SQL Server
= Source opérationnelle

        ↓

PySpark
= ETL

        ↓

ClickHouse
= Stockage analytique

        ↓

Power BI
= Analyse et visualisation
```

---

# 12. Pourquoi utiliser ClickHouse ?

ClickHouse est conçu pour effectuer rapidement des requêtes analytiques sur de grandes quantités de données.

Il est particulièrement adapté aux :

* agrégations ;
* statistiques ;
* analyses historiques ;
* calculs de KPI ;
* requêtes sur de grands volumes ;
* workloads de Business Intelligence.

L'objectif est donc de disposer d'une couche spécialisée pour l'analyse.

---

# 13. Organisation des données dans ClickHouse

Les données transformées par PySpark sont chargées dans une base analytique.

Une organisation typique peut être :

```text
ClickHouse
    │
    └── Base analytique
          │
          ├── Tables de dimensions
          │
          ├── Tables de faits
          │
          └── Tables analytiques
```

La structure exacte dépend du modèle de données et des besoins métier.

---

# 14. Communication PySpark → ClickHouse

PySpark doit également pouvoir communiquer avec ClickHouse.

Cette communication peut être réalisée avec un **driver JDBC ClickHouse** ou une autre interface adaptée.

Le flux est :

```text
PySpark
   │
   │ JDBC
   ▼
ClickHouse
```

Le driver permet à Spark de charger les données transformées dans les tables ClickHouse.

---

# 15. Gestion des ports

Les services exécutés dans Docker peuvent être accessibles à travers des ports.

Le principe est :

```text
Machine hôte
     │
     │ Port exposé
     ▼
Docker
     │
     ▼
ClickHouse
```

ClickHouse dispose de plusieurs interfaces de communication selon le protocole utilisé.

Par exemple :

```text
HTTP
Native
JDBC
ODBC
```

Les ports utilisés dépendent de la configuration de ClickHouse et du mapping défini dans Docker.

---

# 16. Power BI — couche Business Intelligence

Power BI constitue la couche de **Business Intelligence**.

Il se connecte à la base analytique afin d'exploiter les données préparées.

Le flux est :

```text
ClickHouse
     │
     │ Connexion BI
     ▼
 Power BI
```

Power BI n'a donc pas besoin de réaliser tout le traitement effectué précédemment par PySpark.

Les données arrivent déjà préparées pour l'analyse.

---

# 17. Connexion ClickHouse → Power BI

Power BI peut accéder aux données ClickHouse via le connecteur ou le protocole approprié, notamment à travers **ODBC** lorsque cette configuration est utilisée.

Le flux général est :

```text
ClickHouse
     │
     │ ODBC / Connector
     ▼
Power BI
```

Power BI récupère ensuite les données nécessaires à la modélisation.

---

# 18. Modélisation dans Power BI

Après la connexion, les tables sont intégrées dans le modèle Power BI.

Les principales étapes sont :

```text
Connexion
    ↓
Chargement des données
    ↓
Modélisation
    ↓
Relations
    ↓
Mesures
    ↓
KPI
    ↓
Visualisations
```

---

# 19. Relations entre les tables

Lorsque le modèle contient plusieurs tables, Power BI permet de définir les relations entre celles-ci.

Un modèle analytique peut par exemple être organisé autour de :

```text
             Dimension A
                  │
                  │
Dimension B ── Table de faits ── Dimension C
                  │
                  │
             Dimension D
```

L'objectif est de permettre aux utilisateurs d'analyser les données selon différentes dimensions.

---

# 20. Mesures et KPI

Power BI permet de créer des mesures à l'aide de **DAX**.

DAX signifie :

> Data Analysis Expressions

Les mesures permettent de calculer les indicateurs nécessaires au reporting.

Exemples génériques :

```text
Total
Moyenne
Nombre
Pourcentage
Évolution
Variation
Taux
KPI métier
```

Ces mesures sont ensuite utilisées dans les visualisations.

---

# 21. Création des dashboards

La dernière étape consiste à construire les dashboards.

Le flux est :

```text
Données
   ↓
Modèle Power BI
   ↓
Mesures / KPI
   ↓
Visualisations
   ↓
Dashboards
```

Les dashboards peuvent contenir :

* cartes KPI ;
* graphiques ;
* tableaux ;
* courbes ;
* histogrammes ;
* filtres ;
* segments (slicers) ;
* indicateurs d'évolution.

---

# 22. Workflow complet de bout en bout

Le processus complet peut être résumé comme suit :

```text
┌─────────────────────────────────────┐
│          1. SQL SERVER              │
│                                     │
│       Base de données source        │
│                                     │
│       Données opérationnelles       │
└──────────────────┬──────────────────┘
                   │
                   │ JDBC
                   ▼
┌─────────────────────────────────────┐
│             2. PYSPARK              │
│                                     │
│ Extract                             │
│    ↓                                │
│ Nettoyage                           │
│    ↓                                │
│ Transformation                      │
│    ↓                                │
│ Préparation des données             │
└──────────────────┬──────────────────┘
                   │
                   │ JDBC
                   ▼
┌─────────────────────────────────────┐
│        3. DOCKER + CLICKHOUSE       │
│                                     │
│ Docker : environnement d'exécution  │
│                                     │
│ ClickHouse :                        │
│ stockage analytique                 │
└──────────────────┬──────────────────┘
                   │
                   │ Connexion BI
                   ▼
┌─────────────────────────────────────┐
│             4. POWER BI             │
│                                     │
│ Connexion                           │
│    ↓                                │
│ Modélisation                        │
│    ↓                                │
│ Relations                           │
│    ↓                                │
│ Mesures DAX                         │
│    ↓                                │
│ KPI                                 │
│    ↓                                │
│ Visualisations                      │
└──────────────────┬──────────────────┘
                   │
                   ▼
             ┌──────────────┐
             │  Dashboards  │
             └──────────────┘
```

---

# 23. Flux des données

Le flux peut être résumé en quatre étapes :

## Étape 1 — Source

```text
SQL Server
```

Stockage des données opérationnelles.

## Étape 2 — ETL

```text
SQL Server
      ↓
    JDBC
      ↓
  PySpark
      ↓
Extraction + Nettoyage + Transformation
```

## Étape 3 — Stockage analytique

```text
PySpark
    ↓
  JDBC
    ↓
ClickHouse
```

Les données préparées sont stockées dans la couche analytique.

## Étape 4 — Business Intelligence

```text
ClickHouse
     ↓
Connexion BI
     ↓
Power BI
     ↓
Modèle
     ↓
KPI
     ↓
Visualisations
     ↓
Dashboards
```

---

# 24. Responsabilité de chaque composant

| Composant      | Responsabilité                                                          |
| -------------- | ----------------------------------------------------------------------- |
| **SQL Server** | Stocker les données sources et opérationnelles                          |
| **JDBC**       | Assurer la communication entre les applications et les bases de données |
| **PySpark**    | Extraire, nettoyer, transformer et préparer les données                 |
| **Docker**     | Conteneuriser et isoler les services                                    |
| **ClickHouse** | Stocker et interroger les données analytiques                           |
| **Power BI**   | Modéliser, analyser et visualiser les données                           |
| **DAX**        | Créer les mesures et indicateurs analytiques                            |

---

# 25. Séparation des responsabilités

L'un des principes importants de cette architecture est la séparation des rôles.

```text
┌──────────────────────┐
│     SQL Server       │
│                      │
│       SOURCE         │
└──────────────────────┘
           │
           ▼
┌──────────────────────┐
│       PySpark        │
│                      │
│        ETL           │
└──────────────────────┘
           │
           ▼
┌──────────────────────┐
│     ClickHouse       │
│                      │
│     ANALYTIQUE       │
└──────────────────────┘
           │
           ▼
┌──────────────────────┐
│      Power BI        │
│                      │
│    VISUALISATION     │
└──────────────────────┘
```

Chaque technologie possède donc une responsabilité différente.

---

# 26. Avantages de cette architecture

Cette architecture permet notamment :

### Séparation des systèmes

La base opérationnelle est séparée de la base analytique.

### Centralisation des transformations

Les traitements sont regroupés dans la couche PySpark.

### Préparation des données

Power BI reçoit des données déjà nettoyées et préparées.

### Performance analytique

ClickHouse est utilisé pour les requêtes analytiques.

### Isolation

Docker permet d'isoler les services.

### Évolutivité

De nouvelles sources, transformations ou tables analytiques peuvent être ajoutées au pipeline.

### Maintenance

Chaque couche possède un rôle clairement défini, ce qui facilite la maintenance et le diagnostic.

---

# 27. Vue finale de l'architecture

```text
                         DATA SOURCES
                              │
                              ▼
                   ┌───────────────────┐
                   │     SQL SERVER    │
                   │                   │
                   │ Operational DB    │
                   └─────────┬─────────┘
                             │
                             │ JDBC
                             ▼
                   ┌───────────────────┐
                   │      PYSPARK      │
                   │                   │
                   │     EXTRACT       │
                   │        ↓          │
                   │     CLEAN         │
                   │        ↓          │
                   │   TRANSFORM       │
                   │        ↓          │
                   │     PREPARE       │
                   └─────────┬─────────┘
                             │
                             │ JDBC
                             ▼
              ┌─────────────────────────────┐
              │            DOCKER            │
              │                             │
              │    ┌─────────────────────┐  │
              │    │     CLICKHOUSE      │  │
              │    │                     │  │
              │    │ Analytical Database │  │
              │    └──────────┬──────────┘  │
              └───────────────┼─────────────┘
                              │
                              │ ODBC / Connector
                              ▼
                   ┌───────────────────┐
                   │     POWER BI      │
                   │                   │
                   │    MODEL          │
                   │      ↓            │
                   │    DAX            │
                   │      ↓            │
                   │    KPI            │
                   │      ↓            │
                   │ VISUALISATIONS    │
                   └─────────┬─────────┘
                             │
                             ▼
                   ┌───────────────────┐
                   │     DASHBOARDS    │
                   └───────────────────┘
```

---

# 28. Résumé final

Le workflow général est :

```text
SQL Server
     ↓
Extraction
     ↓
PySpark
     ↓
Nettoyage + Transformation
     ↓
ClickHouse
     ↓
Stockage analytique
     ↓
Power BI
     ↓
Modélisation + KPI
     ↓
Dashboards
```

En résumé :

> **SQL Server fournit les données sources. PySpark réalise le processus ETL en extrayant, nettoyant et transformant les données. Docker fournit l'environnement de conteneurisation dans lequel peut fonctionner ClickHouse. ClickHouse stocke les données préparées dans une base analytique. Enfin, Power BI se connecte à ClickHouse pour construire le modèle BI, les mesures, les KPI et les dashboards.**
