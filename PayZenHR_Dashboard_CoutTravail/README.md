# PayZenHR — Dashboard "Coût du Travail" (socle BI granulaire)

Projet conforme à `PayZen_Cahier_Charges_v3_Architecture_BI.md` (destinataire : Nora).
Construit le socle commun (5 dimensions + `fact_bulletin_ligne`) puis le dashboard exemple
"Coût du Travail — Vue Décomposée" montré dans `PayZen_Architecture_BI_Visuel.pdf`.

## Structure du projet

```
PayZenHR_Dashboard_CoutTravail/
│
├── etl/
│   └── etl_socle_bi.py        # Extraction SQL Server → Transformation → Chargement ClickHouse
│
├── sql/
│   └── clickhouse.sql         # Création des 5 tables cibles (dimensions + fact_bulletin_ligne)
│
├── powerbi/
│   └── guide_dashboard_cout_travail.md   # Construction du dashboard (arborescence de décomposition)
│
└── README.md                  # Ce fichier
```

## Ordre d'exécution

1. **Créer les tables ClickHouse**
   Exécuter `sql/clickhouse.sql` sur la base ClickHouse cible (`payzen`).

2. **Configurer les identifiants**
   Définir la variable d'environnement avant de lancer le script Python :
   ```powershell
   $env:PAYZENHR_SQL_PASSWORD = "votre_mot_de_passe_sql_server"
   $env:PAYZENHR_CLICKHOUSE_PASSWORD = "votre_mot_de_passe_clickhouse"   # optionnel si vide
   ```

3. **Lancer l'ETL**
   ```bash
   spark-submit etl/etl_socle_bi.py
   ```
   Le script extrait depuis `PayZenHR_Source` (SQL Server), construit les 5 tables du socle,
   affiche des contrôles (comptages, montants par composant, par client, par employé), puis
   charge chaque table dans ClickHouse en mode `append` (les tables doivent déjà exister,
   voir étape 1).

4. **Construire le dashboard Power BI**
   Suivre `powerbi/guide_dashboard_cout_travail.md` : connexion à ClickHouse, relations,
   mesure DAX unique (`Montant Total`), visuel Arborescence de décomposition.

## Ce qui est intentionnellement absent

Conformément à la règle du projet ("ne pas inventer, ne pas ajouter de fonctionnalités non
demandées") :
- Pas de KPI cards, pas de filtres/slicers, pas de courbe d'évolution pour ce module — le
  cahier des charges v3 et le PDF ne les demandent pas pour ce dashboard précis.
- `dim_echeance_legale` et les tables de faits `fact_echeance`, `fact_absence`,
  `fact_variable_collecte`, `fact_notes_frais`, `fact_evaluation` ne sont pas construites :
  elles servent aux 10 modules futurs listés dans le `.md`, pas au dashboard actuel.

## Points ouverts à valider avec Mohamed

Listés en détail dans `powerbi/guide_dashboard_cout_travail.md`, section 6, et dans les
commentaires `etl/etl_socle_bi.py` (colonnes `poste`/`niveau`/`date_embauche` de
`dim_employees`, fiabilité de `ComponentType` comme identifiant de composant tant que
`PayComponents` est vide côté source).
