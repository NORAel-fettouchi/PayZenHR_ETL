-- =============================================================================
-- organisation_dw.sql
-- ClickHouse DDL - Star Schema Power BI
-- =============================================================================
-- Convention :
--   - Noms de tables et colonnes identiques a ceux produits par etl_powerbi.py
--   - ENGINE = MergeTree pour toutes les tables
--   - ORDER BY = cle naturelle (PK logique) de chaque table
--   - Booleens stockes en UInt8 (0/1) - ClickHouse n'a pas de type Bool natif
--   - Colonnes FK toujours Nullable pour tolerer les jointures incompletes
-- =============================================================================


-- ============================================================
-- DIMENSIONS
-- ============================================================
CREATE TABLE IF NOT EXISTS PROJETS
(
            id_du_projet                                Int64,
            nom_du_domaine                              Nullable(String),
            pays_du_projet                              Nullable(String),
            nom_du_projet                               Nullable(String),
            ville_du_projet                             Nullable(String),
            projet_est_finance                          Nullable(UInt8),
            niveau_de_maturite_du_projet                Nullable(String),
            phase_du_projet                             Nullable(String),
            projet_a_but_lucratif                       Nullable(UInt8),
            nom_pseudo                                  Nullable(String),
            pourcentage_avancement_du_projet            Nullable(Float64),
            projet_recherche_un_expert                  Nullable(UInt8),
            projet_est_actif                            Nullable(UInt8),
            donnees_du_projet_sont_valides              Nullable(UInt8),
            taux_de_completion_du_projet                Nullable(Float64),
            montant_avance_du_projet                    Nullable(Float64),
            budget_estime_du_projet                     Nullable(Float64),
            montant_de_l_avance_du_projet               Nullable(Float64),
            projet_veut_une_avance                      Nullable(UInt8),
            etape_du_parcours                           Nullable(String),
            seuil_de_frais                              Nullable(Float64),
            pourcentage_de_frais                        Nullable(Float64),
            taxe_du_projet                              Nullable(Float64),
            pourboire_du_projet                         Nullable(Float64),
            nouveau_montant_total                       Nullable(Float64),
            date_seuil_des_frais                        Nullable(DateTime),
            projet_est_bloque                           Nullable(UInt8),
            date_debut_du_projet                        Nullable(DateTime),
            date_fin_du_projet                          Nullable(DateTime),
            categorie_estimee_du_projet                 Nullable(String),
            reference_du_projet                         Nullable(String),
            date_debut_collecte_fonds                   Nullable(DateTime),
            duree_collecte_fonds_en_mois                Nullable(Int32),
            montant_total_collecte_fonds                Nullable(Float64),
            collecte_fonds_est_un_don                   Nullable(UInt8),
            collecte_fonds_est_contre_valeur            Nullable(UInt8),
            collecte_fonds_est_un_credit                Nullable(UInt8),
            montant_de_cloture                          Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY id_du_projet
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS PROGRAMMES
(
            id_du_programme                             Int64,
            nom_du_programme                            Nullable(String),
            date_debut_des_candidatures                 Nullable(DateTime),
            date_fin_des_candidatures                   Nullable(DateTime),
            programme_est_public                        Nullable(UInt8),
            id_de_la_phase_courante                     Nullable(Int64),
            region_du_programme                         Nullable(String),
            date_debut_du_programme                     Nullable(DateTime),
            date_fin_du_programme                       Nullable(DateTime),
            budget_du_programme                         Nullable(Float64),
            programme_avec_financement                  Nullable(UInt8),
            programme_avec_crowdfunding                 Nullable(UInt8),
            programme_avec_crowdfunding_hybride         Nullable(UInt8),
            secteur_du_programme                        Nullable(String),
            programme_est_restreint                     Nullable(UInt8),
            programme_est_vip                           Nullable(UInt8),
            programme_avec_incubateur                   Nullable(UInt8),
            programme_incub                             Nullable(Int64),
            linkedin_url                                Nullable(String),
            instagram_url                               Nullable(String),
            facebook_url                                Nullable(String),
            website_url                                 Nullable(String),
            duree_des_candidatures_en_jours             Nullable(Int32),
            type_de_financement                         Nullable(String)
)
ENGINE = MergeTree
ORDER BY id_du_programme
SETTINGS index_granularity = 8192;
-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS PHASES_DU_PROGRAMME
(
            id_de_la_phase                              Int64,
            id_du_programme                             Nullable(Int64),
            nom_de_la_phase                             Nullable(String),
            phase_est_fermee                            Nullable(UInt8)
)
ENGINE = MergeTree
ORDER BY id_de_la_phase
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS PROJETS_PAR_PROGRAMME
(
            id_du_programme_projet                      Int64,
            id_du_programme                             Nullable(Int64),
            id_du_projet                                Nullable(Int64),
            id_de_la_phase                              Nullable(Int64),
            statut_du_projet_dans_programme             Nullable(String),
            note_moyenne_du_projet                      Nullable(Float64),
            note_globale_du_projet                      Nullable(Float64),
            projet_est_soumis                           Nullable(UInt8),
            date_de_soumission_du_projet                Nullable(DateTime),
            id_d_evaluateur                             Nullable(Int64),
            note                                        Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY id_du_programme_projet
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS UTILISATEURS
(
            id_de_l_utilisateur                         Int64,
            prenom_de_l_utilisateur                     Nullable(String),
            nom_de_famille_de_l_utilisateur             Nullable(String),
            ville_de_l_utilisateur                      Nullable(String),
            entreprise_de_l_utilisateur                 Nullable(String),
            utilisateur_est_financeur                   Nullable(String),
            utilisateur_est_organisateur                Nullable(String),
            utilisateur_est_porteur_de_projet           Nullable(String),
            utilisateur_est_investisseur                Nullable(String),
            utilisateur_est_expert                      Nullable(String),
            utilisateur_est_vip                         Nullable(String),
            langue_de_l_utilisateur                     Nullable(String),
            date_de_naissance_de_l_utilisateur          Nullable(DateTime),
            nationalite_de_l_utilisateur                Nullable(String),
            genre                                       Nullable(String),
            tranche_age                                 Nullable(String),
            statut_de_l_utilisateur                     Nullable(String),
            id_de_l_organisation                        Nullable(Int64)
)
ENGINE = MergeTree
ORDER BY id_de_l_utilisateur
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ORGANISATIONS
(
            id_de_l_organisation                        Int64,
            nom_de_l_organisation                       Nullable(String),
            ville_de_l_organisation                     Nullable(String),
            capital_de_l_organisation                   Nullable(Float64),
            est_une_ong                                 Nullable(UInt8),
            est_un_incubateur                           Nullable(UInt8),
            est_un_financeur                            Nullable(UInt8),
            lien_facebook                               Nullable(String),
            lien_instagram                              Nullable(String),
            lien_linkedlin                              Nullable(String),
            lien_twitter                                Nullable(String),
            lien_youtube                                Nullable(String),
            organisation_est_bloquee                    Nullable(UInt8),
            organisation_est_vip                        Nullable(UInt8),
            date_fondation_organisation                 Nullable(DateTime)
)
ENGINE = MergeTree
ORDER BY id_de_l_organisation
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS EVENEMENTS
(
            id_de_l_evenement                           Int64,
            nom_de_l_evenement                          Nullable(String),
            id_du_programme                             Nullable(Int64),
            id_expert                                   Nullable(Int64),
            categorie_evenement                         Nullable(String),
            type_evenement                              Nullable(String),
            date_debut_de_l_evenement                   Nullable(DateTime),
            date_fin_de_l_evenement                     Nullable(DateTime),
            evenement_dure_toute_la_journee             Nullable(UInt8),
            evenement_est_public                        Nullable(UInt8),
            duree_de_l_evenement_en_jours               Nullable(Int32)
)
ENGINE = MergeTree
ORDER BY id_de_l_evenement
SETTINGS index_granularity = 8192;
-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS CATEGORIES_EVENEMENTS
(
            id_de_la_categorie_evenement                Int64,
            nom_de_la_categorie_evenement               Nullable(String)
)
ENGINE = MergeTree
ORDER BY id_de_la_categorie_evenement
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS TACHES_DU_PROGRAMME
(
            id_de_la_tache                              Int64,
            id_du_programme                             Nullable(Int64),
            nom_de_la_tache                             Nullable(String),
            date_debut_de_la_tache                      Nullable(DateTime),
            date_fin_de_la_tache                        Nullable(DateTime),
            tache_est_en_retard                         UInt8
)
ENGINE = MergeTree
ORDER BY id_de_la_tache
SETTINGS index_granularity = 8192;
-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS PHASE_CRITERE
(
            id_du_critere                               Int64,
            id_du_programme                             Nullable(Int64),
            id_de_la_phase                              Nullable(Int64),
            poids_du_critere                            Nullable(Float64),
            nom_du_critere                              Nullable(String)
)
ENGINE = MergeTree
ORDER BY id_du_critere
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS FORUMS
(
            id_de_l_utilisateur                         Nullable(Int64),
            id_du_forum                                 Int64,
            id_du_programme                             Nullable(Int64),
            active                                      Nullable(UInt8),
            est_public                                  Nullable(UInt8),
            id_du_post_forum                            Nullable(Int64),
            utilisateur_est_accepte                     Nullable(UInt8)
)
ENGINE = MergeTree
ORDER BY id_du_forum
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS POSTS_DE_FORUM
(
            id_du_post_forum                            Int64,
            titre_du_post_forum                         Nullable(String)
)
ENGINE = MergeTree
ORDER BY id_du_post_forum
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS INTERACTIONS_SUR_LES_POSTS
(
            id_de_l_interaction                         Int64,
            id_du_post_forum                            Nullable(Int64),
            type_d_interaction                          Nullable(String)
)
ENGINE = MergeTree
ORDER BY id_de_l_interaction
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS QUESTIONS_DU_MANAGER
(
            id_de_la_question                           Int64,
            id_du_programme                             Nullable(Int64),
            nom_de_la_question                          Nullable(String),
            type_de_la_question                         Nullable(String),
            question_est_requise                        Nullable(UInt8),
            question_avec_options                       Nullable(UInt8),
            question_utilisee_comme_filtre              Nullable(UInt8),
            question_utilisee_comme_graphique           Nullable(UInt8)
)
ENGINE = MergeTree
ORDER BY id_de_la_question
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS OPTIONS_DES_QUESTIONS
(
            id_de_la_question                           Int64,
            nom_de_l_option                             Nullable(String)
)
ENGINE = MergeTree
ORDER BY id_de_la_question
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS REPONSES_AUX_QUESTIONS
(
            id_du_projet                                Int64,
            id_du_porteur_de_projet                     Nullable(Int64),
            id_de_la_question                           Nullable(Int64),
            reponse_a_la_question                       Nullable(String)
)
ENGINE = MergeTree
ORDER BY id_du_projet
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS DETAILS_DES_QUESTIONS
(
            id_du_detail_de_question                    Int64,
            id_du_programme                             Nullable(Int64),
            id_de_la_question_akkan                     Nullable(Int64),
            question_est_choisie                        Nullable(UInt8),
            ordre_de_la_question                        Nullable(Int32)
)
ENGINE = MergeTree
ORDER BY id_du_detail_de_question
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS QUESTIONS_AKKAN
(
            id_de_la_question_akkan                     Int64,
            categorie_de_la_question                    Nullable(String),
            code_de_la_question                         Nullable(String),
            type_de_la_question                         Nullable(String),
            question_requise_par_akkan                  Nullable(UInt8)
)
ENGINE = MergeTree
ORDER BY id_de_la_question_akkan
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS INCUBATEURS
(
            id_du_programme_avec_incubateur             Int64,
            id_de_l_organisation                        Nullable(Int64)
)
ENGINE = MergeTree
ORDER BY id_du_programme_avec_incubateur
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS QUESTIONS_FORMULAIRE
(
            nom_de_la_question                          Nullable(String),
            reponse                                     Nullable(String),
            type_de_question                            Nullable(String),
            id_du_programme                             Int64,
            id_du_projet                                Nullable(Int64),
            id_porteur_de_projet                        Nullable(Int64),
            id_de_l_organisation                        Nullable(Int64),
            provenance                                  Nullable(String)
)
ENGINE = MergeTree
ORDER BY id_du_programme
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS EVAL_EVALUATION_PAR_CRITERE
(
            id_du_projet                                Int64,
            id_du_programme                             Nullable(Int64),
            id_de_la_phase                              Nullable(Int64),
            id_de_l_evaluateur                          Nullable(Int64),
            id_du_critere                               Nullable(Int64),
            note                                        Nullable(Float64),
            poids                                       Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY id_du_projet
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS EVAL_EVALUATION_FINALE_PAR_PHASE
(
            id_du_projet                                Int64,
            id_du_programme                             Nullable(Int64),
            id_de_la_phase                              Nullable(Int64),
            id_de_l_evaluateur                          Nullable(Int64),
            note_final                                  Nullable(Float64),
            soumis                                      Nullable(UInt8)
)
ENGINE = MergeTree
ORDER BY id_du_projet
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS EVAL_EVALUATEURS_PAR_PHASE
(
            id_du_programme                             Int64,
            id_de_la_phase                              Nullable(Int64),
            id_de_l_evaluateur                          Nullable(Int64)
)
ENGINE = MergeTree
ORDER BY id_du_programme
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS NOTES_DES_PROJETS
(
            id_du_projet                                Int64,
            id_du_programme                             Nullable(Int64),
            id_de_la_phase                              Nullable(Int64),
            note_globale_du_projet                      Nullable(Float64),
            note_moyenne_du_projet                      Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY id_du_projet
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS PROGRAMME_PORTEURS_DE_PROJET
(
            id_du_programme                             Nullable(Int64),
            id_de_l_utilisateur                         Int64,
            id_du_projet                                Nullable(Int64),
            id_de_l_organisation                        Nullable(Int64),
            created_at                                  Nullable(DateTime),
            id_phase                                    Nullable(Int64),
            form_reopened                               Nullable(UInt8),
            submitted                                   Nullable(UInt8),
            submited_date                               Nullable(DateTime),
            status                                      Nullable(String)
)
ENGINE = MergeTree
ORDER BY id_de_l_utilisateur
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS PARTICIPATIONS_AUX_EVENEMENTS
(
            id_du_programme                             Nullable(Int64),
            id_de_l_evenement                           Int64,
            id_de_l_expert_du_programme                 Nullable(Int64),
            id_de_l_utilisateur                         Nullable(Int64),
            role_dans_event                             Nullable(String),
            participation_obligatoire                   Nullable(UInt8),
            a_participe                                 Nullable(UInt8),
            a_accepte                                   Nullable(UInt8),
            participation_confirmee                     Nullable(UInt8)
)
ENGINE = MergeTree
ORDER BY id_de_l_evenement
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS PARTENAIRES_DU_PROGRAMME
(
            id_du_programme                             Int64,
            id_de_l_organisation                        Nullable(Int64)
)
ENGINE = MergeTree
ORDER BY id_du_programme
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS INTERACTIONS_DES_FORUMS
(
            id_de_l_utilisateur                         Nullable(Int64),
            id_du_forum                                 Int64,
            id_du_programme                             Nullable(Int64),
            id_du_post_forum                            Nullable(Int64)
)
ENGINE = MergeTree
ORDER BY id_du_forum
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS VUES_ET_ABONNEMENTS
(
            id_de_l_utilisateur                         Nullable(Int64),
            id_du_programme                             Int64,
            type_d_activite                             Nullable(String)
)
ENGINE = MergeTree
ORDER BY id_du_programme
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS TACHES_PAR_PROJET
(
            id_de_la_tache_du_projet                    Int64,
            id_de_la_tache                              Nullable(Int64),
            id_du_projet                                Nullable(Int64),
            status_du_programme                         Nullable(String),
            status_de_l_utilisateur                     Nullable(String)
)
ENGINE = MergeTree
ORDER BY id_de_la_tache_du_projet
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS PROGRAMME_UTILISATEURS_INTERNES
(
            id_du_programme                             Int64,
            id_de_l_utilisateur                         Nullable(Int64),
            role_dans_le_programme                      Nullable(String)
)
ENGINE = MergeTree
ORDER BY id_du_programme
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS QUESTIONS_PAR_PROGRAMME
(
            id_du_programme                             Int64,
            id_de_la_question_du_manager                Nullable(Int64),
            id_du_detail_du_programme                   Nullable(Int64)
)
ENGINE = MergeTree
ORDER BY id_du_programme
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS REMARQUE
(
            id_de_la_remarque                           Int64,
            id_projet                                   Nullable(Int64),
            id_de_porteur_de_projet                     Nullable(Int64),
            id_de_l_utilisateur                         Nullable(Int64)
)
ENGINE = MergeTree
ORDER BY id_de_la_remarque
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ENTRETIENS
(
            id_de_l_entretien                           Int64,
            date_debut_entretien                        Nullable(DateTime),
            date_fin_entretien                          Nullable(DateTime),
            status_global_entretien                     Nullable(String),
            id_du_projet                                Nullable(Int64),
            nom_du_projet                               Nullable(String),
            id_du_programme                             Nullable(Int64),
            statut_projet_entretien                     Nullable(String),
            id_de_la_question                           Nullable(Int64),
            texte_de_la_question                        Nullable(String),
            id_de_l_utilisateur                         Nullable(Int64),
            texte_de_la_reponse                         Nullable(String)
)
ENGINE = MergeTree
ORDER BY id_de_l_entretien
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS SONDAGES
(
            survey_id                                   Int64,
            survey_titre                                Nullable(String),
            survey_est_general                          Nullable(String),
            question_id                                 Nullable(Int64),
            question_text                               Nullable(String),
            question_type                               Nullable(String),
            question_obligatoire                        Nullable(String),
            choix_id                                    Nullable(Int64),
            choix_label                                 Nullable(String),
            id_du_repondant                             Nullable(String),
            statut_participant                          Nullable(String),
            reponse_libre                               Nullable(String),
            id_de_l_utilisateur                         Nullable(Int64),
            flag_est_selectionne                        Nullable(UInt8),
            prenom_utilisateur                          Nullable(String)
)
ENGINE = MergeTree
ORDER BY survey_id
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS CLICS_LIENS
(
            id_du_clic                                  Int64,
            id_du_lien                                  Nullable(Int64),
            nom_de_l_agence                             Nullable(String),
            type_de_lien                                Nullable(String),
            date_debut                                  Nullable(DateTime),
            date_fin                                    Nullable(DateTime),
            date_du_clic                                Nullable(DateTime),
            nombre_de_clics                             Nullable(Int32),
            id_du_programme                             Nullable(Int64),
            id_du_projet                                Nullable(Int64)
)
ENGINE = MergeTree
ORDER BY id_du_clic
SETTINGS index_granularity = 8192;