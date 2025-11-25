#!/bin/bash

export PYTHONPATH="/opt/spark/app:/opt/spark/app/src:$PYTHONPATH"

# Iniciar 
if [ "$SPARK_MODE" = "master" ]; then
    /opt/spark/sbin/start-master.sh
elif [ "$SPARK_MODE" = "worker" ]; then
    /opt/spark/sbin/start-worker.sh $SPARK_MASTER_URL
fi

# Manter  container funcionando
tail -f /dev/null
