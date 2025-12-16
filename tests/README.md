# Tests WebHDFSMagic

## Structure des Tests

Les tests sont organisés par module fonctionnel :

### Tests Principaux

- **test_client.py** (267 lignes) - Tests du client WebHDFS
  - Initialisation du client
  - Exécution des requêtes HTTP
  - Gestion des paramètres d'authentification
  - Gestion des exceptions

- **test_magics.py** (421 lignes) - Tests des commandes magiques IPython
  - Toutes les commandes hdfs (cat, ls, mkdir, rm, get, put)
  - Gestion des arguments et options
  - Intégration avec IPython
  - Cas d'erreur et validation

### Tests des Commandes

- **test_file_ops.py** (477 lignes) - Opérations sur fichiers
  - cat : lecture avec options -n, --raw, --format
  - get : téléchargement avec wildcards
  - put : upload de fichiers

- **test_directory_ops.py** (270 lignes) - Opérations sur répertoires
  - ls : listage avec différents formats
  - mkdir : création de répertoires
  - rm : suppression (récursive, wildcards)

- **test_permission_ops.py** (302 lignes) - Gestion des permissions
  - chmod : modification permissions (récursif)
  - chown : changement propriétaire/groupe

- **test_smart_cat.py** (337 lignes) - Smart cat feature
  - Détection automatique CSV/TSV/Parquet
  - Inférence de délimiteurs
  - Formatage en tableaux
  - Options de format (table, pandas, raw)

### Tests de Configuration et Utilitaires

- **test_config.py** (666 lignes) - Configuration
  - Chargement depuis ~/.webhdfsmagic/config.json
  - Fallback sur sparkmagic config
  - Vérification SSL

- **test_utils.py** (225 lignes) - Fonctions utilitaires
  - Formatage des permissions
  - Formatage des tailles de fichiers
  - Parsing des chemins HDFS

- **test_logger.py** (140 lignes) - Système de logging
  - Création des logs
  - Masquage des mots de passe
  - Rotation des fichiers

### Tests d'Installation

- **test_autoload.py** (100 lignes) - Installation automatique
  - Création du script de démarrage IPython
  - Gestion de l'idempotence
  - Gestion des erreurs

- **test_ssl_verification.py** (215 lignes) - Vérification SSL
  - Configuration SSL true/false
  - Chemins de certificats
  - Expansion tilde

## Exécution des Tests

```bash
# Tous les tests
pytest tests/

# Avec couverture
pytest tests/ --cov=webhdfsmagic --cov-report=term-missing

# Tests spécifiques
pytest tests/test_smart_cat.py -v
pytest tests/test_magics.py::test_cat_command -v

# Avec rapport HTML
pytest tests/ --cov=webhdfsmagic --cov-report=html
```

## Couverture Actuelle

- **Total: 86%** (856 statements, 118 miss)
- client.py: **100%**
- directory_ops.py: **100%**
- permission_ops.py: **100%**
- config.py: **100%**
- utils.py: **100%**
- magics.py: **97%**
- logger.py: **97%**
- file_ops.py: 68% (fonctions internes de formatage peu testées)
- install.py: 66%
- base.py: 50%
- __init__.py: 45% (auto-setup code)

## Fixtures Communes

Voir `conftest.py` pour les fixtures partagées :
- `magics_instance` : Instance de WebHDFSMagics pour tests
- `mock_requests_get` : Mock pour requests.get
- `mock_requests_request` : Mock flexible pour requests.request

## Standards de Test

1. **Nommage** : `test_<fonction>_<scenario>`
2. **Organisation** : Classes pour grouper tests similaires
3. **Documentation** : Docstrings claires pour chaque test
4. **Isolation** : Utilisation de fixtures et mocks
5. **Couverture** : Tester cas normaux + edge cases + erreurs
