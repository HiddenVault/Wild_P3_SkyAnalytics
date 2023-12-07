Projet 3 - SkyAnalytics

# BUT PEDAGOGIQUE :

Le projet a pour objectif d'initier les étudiants à l'analyse de données complexes dans un contexte réel et pratique. 

Les compétences visées sont :

- **Analyse de données** : Comprendre et interpréter des jeux de données simulés reflétant des opérations aériennes.

- **Modélisation prédictive** : Utiliser des techniques de data science pour prédire l'état des systèmes d'un avion.

- **Prise de décision basée sur les données** : Évaluer l'impact financier des décisions et optimiser les opérations.

- **Visualisation de données** : Représenter graphiquement les résultats pour faciliter la compréhension et la communication.

------

# DESCRIPTIF : 

SkyAnalytics est un projet de simulation qui fournit un cadre pour l'analyse des données de maintenance aéronautique. 

Les étudiants travaillent avec un ensemble de données qui comprend les détails des avions (aéronefs), leurs composants spécifiques, les logs de vols, et les données sur l'usure et l'état des pièces. 

Ils devront nettoyer, traiter et analyser ces données, et enfin, construire et entraîner des modèles prédictifs. 

Le projet culminera dans la création d'un système de recommandation pour les interventions de maintenance.

------

# IMPERATIFS

- Importance de la **chronologie des données**

- **L’automatisation** : un élément clé

- L’aspect essentiel de la **prévision budgétaire**

------

# DESCRIPTION DES DONNEES :

1. **Aéronefs** (aeronefs.csv) :

- Informations sur chaque avion, y compris les dates de service et de maintenance.

2. **Composants** (composants.csv) :

- Détails sur les différents composants de chaque avion, leur usure et leur coût.

3. **Logs des Vols** (logs_vols.csv) :

- Enregistrements des vols effectués, avec des informations telles que la durée, les données des capteurs, et l'état du voyant après le vol.

4. **Dégradations** (degradation.csv) :

- Données sur l'usure des composants au fil du temps et la nécessité de leur remplacement.

------

# CONSIGNES DE TRAVAIL : 

1. ## Analyse de Données

   - **Comprendre les données** : Explorez chaque jeu de données pour comprendre sa structure et son contenu.


   - **Nettoyage des données** : Traitez les valeurs manquantes ou aberrantes.


   - **Analyse exploratoire** : Identifiez les tendances, les anomalies, et les corrélations.


2. ## Modélisation Prédictive

   - **Prédiction de l'état du voyant** :
     - 0 : Aucun problème détecté (pas d'immobilisation nécessaire).
     
     - 1 : Problème mineur (immobilisation d'une journée).
     
     - 2 : Problème modéré (immobilisation de 7 jours).
     
     - 3 : Problème critique (immobilisation de 14 jours).
     
   - Développez un modèle capable de prédire l'état du voyant pour chaque vol en fonction des données disponibles.
   
3. ## Calcul des Coûts

   - **Coût d'immobilisation** : Chaque journée d'immobilisation coûte 15 000 euros.
   
   
      - Calculez le **coût total** d'immobilisation basé sur les états réels des voyants.
   
   
      - **Comparez** ce coût avec celui que votre modèle aurait prédit.
   
4. ## Visualisation et Rapport

	- Présentez vos résultats à travers des visualisations claires et informatives.

Rédigez un rapport expliquant vos méthodes, résultats, et recommandations.

------

# Evaluation et Livrables

- **Rapport Final** : Document détaillant l'approche, l'analyse, les modèles développés, et les conclusions.

- **Présentation** : Une présentation synthétique des résultats obtenus.

- **Code Source** : Tous les scripts et notebooks développés pendant le projet.

Temps nécessaire pour finalisation :

- Préparation et Nettoyage des Données : 2 semaines

- Analyse Exploratoire : 2 semaines

- Modélisation et Machine Learning : 2 semaines

- Validation et Optimisation des Modèles : 1 semaine

- Présentation des Résultats : 1 semaine

- Rédaction du Rapport : 1 semaine
