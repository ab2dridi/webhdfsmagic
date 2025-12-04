#!/usr/bin/env python3
"""
Script de test simple pour vérifier que webhdfsmagic fonctionne avec le système HDFS local
"""

import sys
import os

# Ajouter le chemin local pour importer webhdfsmagic
sys.path.insert(0, '/workspaces/webhdfsmagic')

from webhdfsmagic.magics import WebHDFSMagics
from IPython import get_ipython
from IPython.terminal.interactiveshell import TerminalInteractiveShell

# Créer une session IPython
ipython = TerminalInteractiveShell.instance()

# Charger l'extension
magics = WebHDFSMagics(ipython)
ipython.register_magics(magics)

print("=" * 60)
print("Test de webhdfsmagic avec HDFS local")
print("=" * 60)

# Configuration
print("\n1️⃣ Configuration...")
config_file = os.path.expanduser("~/.webhdfsmagic/config.json")
if os.path.exists(config_file):
    print(f"✓ Fichier de configuration trouvé: {config_file}")
    import json
    with open(config_file) as f:
        config = json.load(f)
        print(f"  URL: {config.get('knox_url')}{config.get('webhdfs_api')}")
        print(f"  User: {config.get('username')}")
        print(f"  SSL Verify: {config.get('verify_ssl')}")
else:
    print(f"✗ Fichier de configuration non trouvé: {config_file}")
    sys.exit(1)

# Test des commandes
print("\n2️⃣ Test de listing du répertoire racine...")
try:
    result = ipython.run_line_magic('hdfs', 'ls /')
    print("✓ Listing réussi")
except Exception as e:
    print(f"✗ Erreur: {e}")

print("\n3️⃣ Création d'un répertoire de test...")
try:
    result = ipython.run_line_magic('hdfs', 'mkdir /test_webhdfs')
    print("✓ Répertoire créé")
except Exception as e:
    print(f"✗ Erreur: {e}")

print("\n4️⃣ Vérification que le répertoire existe...")
try:
    result = ipython.run_line_magic('hdfs', 'exists /test_webhdfs')
    print(f"✓ Répertoire existe: {result}")
except Exception as e:
    print(f"✗ Erreur: {e}")

print("\n5️⃣ Création d'un fichier local de test...")
test_file = "/tmp/webhdfs_test.txt"
with open(test_file, 'w') as f:
    f.write("Hello from webhdfsmagic!\nThis is a test file.\n")
print(f"✓ Fichier créé: {test_file}")

print("\n6️⃣ Upload du fichier vers HDFS...")
try:
    result = ipython.run_line_magic('hdfs', f'put {test_file} /test_webhdfs/test.txt')
    print("✓ Upload réussi")
except Exception as e:
    print(f"✗ Erreur: {e}")

print("\n7️⃣ Listing du répertoire de test...")
try:
    result = ipython.run_line_magic('hdfs', 'ls /test_webhdfs')
    print("✓ Listing réussi")
except Exception as e:
    print(f"✗ Erreur: {e}")

print("\n8️⃣ Lecture du contenu du fichier...")
try:
    result = ipython.run_line_magic('hdfs', 'cat /test_webhdfs/test.txt')
    print("✓ Lecture réussie")
    print(f"Contenu: {result}")
except Exception as e:
    print(f"✗ Erreur: {e}")

print("\n9️⃣ Download du fichier depuis HDFS...")
try:
    download_file = "/tmp/downloaded_test.txt"
    result = ipython.run_line_magic('hdfs', f'get /test_webhdfs/test.txt {download_file}')
    print("✓ Download réussi")
    if os.path.exists(download_file):
        with open(download_file) as f:
            print(f"Contenu du fichier téléchargé:\n{f.read()}")
except Exception as e:
    print(f"✗ Erreur: {e}")

print("\n🔟 Statistiques du fichier...")
try:
    result = ipython.run_line_magic('hdfs', 'stat /test_webhdfs/test.txt')
    print("✓ Stat réussi")
except Exception as e:
    print(f"✗ Erreur: {e}")

print("\n" + "=" * 60)
print("Tests terminés!")
print("=" * 60)
