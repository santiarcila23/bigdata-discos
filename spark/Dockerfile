# Spark 3.5.3 + conector de Kafka (Structured Streaming) + conector de MongoDB.
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu
USER root
ARG PAQUETES=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0
# Los paquetes se resuelven al construir la imagen y quedan en /opt/spark/jars:
# los trabajos no descargan nada al ejecutarse.
RUN echo "" > /tmp/noop.py \
 && /opt/spark/bin/spark-submit --master "local[1]" --conf spark.jars.ivy=/tmp/ivy \
      --packages "${PAQUETES}" /tmp/noop.py \
 && cp /tmp/ivy/jars/*.jar /opt/spark/jars/ \
 && ls /opt/spark/jars | grep -i -E "kafka|mongo|bson" \
 && rm -rf /tmp/ivy /tmp/noop.py
COPY jobs /opt/jobs
RUN chmod +x /opt/jobs/*.sh
WORKDIR /opt/jobs
