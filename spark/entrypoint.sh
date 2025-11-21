#!/bin/bash

# Fail fast
set -e

if [ "$SPARK_MODE" = "master" ]; then
    echo "Inicializando Spark MASTER..."
    /opt/spark/sbin/start-master.sh
elif [ "$SPARK_MODE" = "worker" ]; then
    echo "Inicializando Spark WORKER..."
    /opt/spark/sbin/start-worker.sh $SPARK_MASTER_URL
else
    echo "ERRO: SPARK_MODE deve ser 'master' ou 'worker'"
    exit 1
fi

# Mantém o container rodando
tail -f /dev/null
