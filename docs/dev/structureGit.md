Structure du git

1. **Dossier racine du projet :**

   - `readme.md`: Fichier contenant la documentation de haut niveau du projet.

2. **Dossier de code source :**

   - `src/`: Fichiers source Python du projet.
   - `requirements.txt`: Fichier spécifiant les dépendances du projet pour faciliter l'installation des bibliothèques nécessaires.

3. **Dossier de tests :**

   - `tests/`: Fichiers de tests unitaires, d'intégration, etc.

4. **Dossier de documentation :**

   - docs :

     - `api/`: Documentation de l'API du code.
     - `user/`: Documentation utilisateur.
     - `dev/`: Documentation pour les développeurs.
     - `index.html` ou `index.md`: Page d'accueil de la documentation.

5. **Dossier de données :**

   - `data/`: Données statiques, vous pouvez les stocker ici.

6. **Dossier de livrables :**

   - `dist/`: Fichiers générés pour la distribution, tels que les fichiers exécutables ou les packages.

7. **Dossier de scripts :**

   - `scripts/`: Scripts utiles pour des tâches spécifiques, comme la gestion des bases de données, l'analyse de données, etc.

8. **Dossier de configuration :**

   - `config/`: Fichiers de configuration, par exemple, des fichiers de configuration YAML ou JSON.

9. **Dossiers de virtualenv (environnement virtuel) et fichiers Git :**

   - `venv/` (ou tout autre nom utilisé pour l'environnement virtuel).
   - Fichiers Git, tels que `.gitignore`, pour spécifier les fichiers et dossiers à ignorer lors de la gestion de version.

10. **Fichiers de gestion de version :**

    - `.gitignore`: Pour spécifier les fichiers et dossiers à ignorer lors de la gestion de version.
    - `.gitattributes` (si nécessaire) : Pour spécifier les attributs de fichiers Git.