#!/usr/bin/env bash
# Uso: submit.sh <nombre_del_trabajo> [argumentos]
#   ej.: submit.sh parte_a      ->  /opt/jobs/parte_a.py
set -euo pipefail
JOB="/opt/jobs/$1.py"; shift
[ -f "$JOB" ] || { echo "No existe el trabajo $JOB"; ls /opt/jobs/*.py; exit 1; }
DRIVER_IP="$(hostname -i | awk '{print $1}')"
exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER:-spark://spark-master:7077}" \
  --conf spark.driver.host="${DRIVER_IP}" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.memory="${SPARK_DRIVER_MEMORY:-1g}" \
  --conf spark.executor.memory="${SPARK_EXECUTOR_MEMORY:-1500m}" \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.ui.port=4040 \
  --conf spark.jars.ivy=/tmp/ivy \
  --py-files /opt/jobs/comun.py \
  "$JOB" "$@"
