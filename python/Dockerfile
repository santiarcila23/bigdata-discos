# Imagen de Python para: descarga de datos, producer de Kafka, consultas en MongoDB,
# Kaplan-Meier y gráficos.
FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 MPLBACKEND=Agg
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
