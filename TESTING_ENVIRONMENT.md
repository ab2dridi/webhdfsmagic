# Environnement de Test HDFS pour webhdfsmagic

## 🐳 Configuration Docker

Cet environnement fournit un cluster HDFS local avec WebHDFS activé et un gateway (simulant Knox) pour tester webhdfsmagic.

### Composants

- **namenode**: NameNode Hadoop avec WebHDFS activé (port 9870)
- **datanode**: DataNode Hadoop
- **webhdfs-gateway**: Nginx agissant comme proxy (simulant Knox Gateway) (port 8080)

## 🚀 Démarrage

```bash
# Démarrer le cluster
docker-compose up -d

# Vérifier que les conteneurs fonctionnent
docker ps

# Attendre ~30 secondes que HDFS s'initialise complètement
```

## 🔧 Configuration webhdfsmagic

Le fichier de configuration est créé automatiquement dans `~/.webhdfsmagic/config.json`:

```json
{
  "knox_url": "http://localhost:8080/gateway/default",
  "webhdfs_api": "/webhdfs/v1",
  "username": "testuser",
  "password": "testpass",
  "verify_ssl": false
}
```

## ✅ Tests

### Test via curl

```bash
# Lister le contenu racine
curl "http://localhost:8080/gateway/default/webhdfs/v1/?op=LISTSTATUS&user.name=testuser"

# Créer un répertoire
curl -X PUT "http://localhost:8080/gateway/default/webhdfs/v1/test?op=MKDIRS&user.name=testuser"

# Lister un répertoire spécifique
curl "http://localhost:8080/gateway/default/webhdfs/v1/test?op=LISTSTATUS&user.name=testuser"
```

### Test avec webhdfsmagic

```python
# Dans un notebook ou IPython
%load_ext webhdfsmagic

# Lister
%hdfs ls /

# Créer un répertoire
%hdfs mkdir /test

# Upload
%hdfs put local_file.txt /test/remote_file.txt

# Download
%hdfs get /test/remote_file.txt ./downloaded.txt
```

## 📓 Utilisation du notebook de démonstration

The notebook `examples/demo.ipynb` contains a complete demonstration of all features with user stories.

Pour l'utiliser avec cet environnement local :

1. Démarrez le cluster : `docker-compose up -d`
2. Ouvrez le notebook dans Jupyter
3. Exécutez les cellules séquentiellement

Note: Certaines commandes du notebook peuvent nécessiter des ajustements de chemins selon votre environnement.

## 🛑 Arrêt

```bash
# Arrêter les conteneurs
docker-compose down

# Arrêter et supprimer les volumes (⚠️ efface toutes les données HDFS)
docker-compose down -v
```

## 🔍 Débogage

### Logs des conteneurs

```bash
# Logs du namenode
docker logs namenode

# Logs du datanode  
docker logs datanode

# Logs du gateway
docker logs webhdfs-gateway
```

### Interface Web HDFS

Accédez à l'interface web du NameNode : http://localhost:9870

### Test de connectivité

```bash
# Test direct vers le namenode (sans gateway)
curl "http://localhost:9870/webhdfs/v1/?op=LISTSTATUS"

# Test via le gateway
curl "http://localhost:8080/gateway/default/webhdfs/v1/?op=LISTSTATUS&user.name=testuser"
```

## 📝 Notes

- Cet environnement est destiné au **développement et aux tests uniquement**
- Les permissions HDFS sont désactivées (`dfs.permissions.enabled=false`) pour simplifier les tests
- Aucune authentification réelle n'est configurée (Knox est simulé par nginx)
- Les données sont stockées dans des volumes Docker et persistent entre les redémarrages

## 🔐 Pour tester avec SSL

Pour tester avec SSL/TLS :

1. Générez un certificat auto-signé
2. Décommentez la section HTTPS dans `nginx.conf`
3. Modifiez `config.json` :
   ```json
   {
     "knox_url": "https://localhost:8443/gateway/default",
     "webhdfs_api": "/webhdfs/v1",
     "username": "testuser",
     "password": "testpass",
     "verify_ssl": "/path/to/certificate.pem"
   }
   ```
