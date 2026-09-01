# Power BI — Dashboard "Coût du Travail — Vue Décomposée"

Construit strictement à partir de ce que montre `PayZen_Architecture_BI_Visuel.pdf` (page 2) :
un fil de navigation **Portefeuille → Client → Service → Composant de paie**, où chaque
segment vient de `fact_bulletin_ligne` filtré par `dim_composant_paie`, avec clic sur un
segment → détail par collaborateur.

Aucun KPI card, aucun filtre supplémentaire n'est ajouté : le `.md` n'en demande pas pour
ce dashboard précis (contrairement au premier dashboard "Masse Salariale").

---

## 1. Connexion à ClickHouse

Power BI Desktop → Obtenir les données → **ODBC** (ou connecteur ClickHouse s'il est installé)
→ sélectionner la base `payzen` → importer les 5 tables :
`dim_clients`, `dim_employees`, `dim_calendar`, `dim_composant_paie`, `fact_bulletin_ligne`.

## 2. Modèle — relations

```
dim_clients[client_id]        (1) → (*) fact_bulletin_ligne[client_id]
dim_composant_paie[composant_id] (1) → (*) fact_bulletin_ligne[composant_id]
dim_employees[employee_id]    (1) → (*) fact_bulletin_ligne[employee_id]
```

`dim_calendar` n'est pas nécessaire pour ce dashboard précis (l'exemple du PDF n'affiche
aucune évolution temporelle) — on ne la relie pas ici pour ne pas complexifier inutilement.

## 3. Mesure DAX

Une seule mesure est justifiée par les données disponibles (colonne `montant` = `Amount`
source, confirmée dans le script ETL) :

```DAX
Montant Total = SUM ( fact_bulletin_ligne[montant] )
```

Aucune autre mesure (variation, moyenne, score...) n'est demandée dans le cahier des
charges pour ce dashboard — je n'en ajoute pas.

## 4. Visuel — Arborescence de décomposition (Decomposition Tree)

C'est le visuel natif Power BI qui correspond exactement à la description du PDF
(navigation par segments cliquables, breadcrumb automatique, drill libre) :
**Insérer → Visuels IA → Arborescence de décomposition**.

Configuration :
- **Analyser** (Analyze) : `[Montant Total]`
- **Expliquer par** (Explain by), dans cet ordre pour respecter le fil du PDF :
  1. `dim_clients[nom]` → niveau "Client"
  2. `fact_bulletin_ligne[service]` → niveau "Service"
  3. `dim_composant_paie[libelle]` → niveau "Composant de paie"
  4. `dim_employees[nom]` → niveau supplémentaire pour le détail par collaborateur
     (l'utilisateur l'ajoute lui-même via le bouton "+" sur un segment, exactement le
     comportement "clic sur un segment → détail par collaborateur" décrit dans le PDF)

Le nœud racine du visuel (le total, avant tout clic) correspond au niveau "Portefeuille"
du breadcrumb — c'est un comportement natif de l'arborescence de décomposition, aucun champ
supplémentaire n'est nécessaire pour le représenter.

**Formatage** :
- Couleur du nœud racine et des barres : bleu marine `#152A6B` (cohérence avec le premier
  dashboard, mais **information non précisée** dans ce cahier des charges — à confirmer
  si une charte graphique spécifique est attendue pour ce module).
- Taille recommandée : pleine largeur de page.

## 5. Filtres

Aucun filtre n'est spécifié dans le cahier des charges pour ce dashboard. Le PDF ne montre
que la navigation par arborescence elle-même, qui fait déjà office de filtre progressif.

## 6. Points à confirmer avec Mohamed avant de considérer ce module terminé

- Faut-il ajouter une charte de couleurs identique au premier dashboard (bleu foncé /
  bleu clair / orange) ou ce module a-t-il un style propre ? Non précisé.
- Le niveau "Portefeuille" doit-il filtrer sur un cabinet précis si plusieurs cabinets
  utilisent la même base (`cabinet_id` dans `dim_clients`) ? Non précisé.
- Faut-il exclure certains statuts de bulletin (comme dans le premier projet, statuts
  2/5/6) dans ce module ? Le cahier des charges v3 ne mentionne plus cette notion de
  statut — à confirmer si elle s'applique toujours à `fact_bulletin_ligne`.
