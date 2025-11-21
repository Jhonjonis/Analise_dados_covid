#!/bin/bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/app/jobs/wordcount.py
