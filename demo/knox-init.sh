#!/bin/bash

set -e

echo "Initialisation de Knox Gateway..."

# Attendre que le NameNode soit prêt
echo "Attente du NameNode..."
while ! nc -z namenode 9870; do
    sleep 2
done
echo "NameNode est prêt!"

# Créer les keystores si nécessaire
if [ ! -f "${KNOX_HOME}/data/security/keystores/gateway.jks" ]; then
    echo "Création du keystore..."
    ${KNOX_HOME}/bin/knoxcli.sh create-master --master secret1234
    ${KNOX_HOME}/bin/knoxcli.sh create-cert --hostname localhost
fi

# Copier la configuration de la topologie
if [ -f /knox-topology/topology.xml ]; then
    echo "Installation de la topologie..."
    cp /knox-topology/topology.xml ${KNOX_HOME}/conf/topologies/default.xml
fi

# Copier gateway-site.xml
if [ -f /knox-topology/gateway-site.xml ]; then
    echo "Installation de gateway-site.xml..."
    cp /knox-topology/gateway-site.xml ${KNOX_HOME}/conf/gateway-site.xml
fi

echo "Démarrage de Knox Gateway..."
${KNOX_HOME}/bin/gateway.sh start

# Garder le container actif et afficher les logs
tail -f ${KNOX_HOME}/logs/gateway.log
