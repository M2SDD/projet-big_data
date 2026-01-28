FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Dépendances
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jre-headless \
    python3 python3-pip \
    curl wget vim ssh net-tools ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Python par défaut
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1

# Installer PySpark
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install pyspark==3.4.0 --no-cache-dir

ENV SPARK_VERSION=3.4.0 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    PYTHONHASHSEED=1

RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && mkdir -p $SPARK_HOME \
    && tar -xf apache-spark.tgz -C $SPARK_HOME --strip-components=1 \
    && rm apache-spark.tgz
# Variables d'environnement Spark
ENV SPARK_MASTER_PORT=7077 \
    SPARK_MASTER_WEBUI_PORT=8080 \
    SPARK_WORKER_PORT=7000 \
    SPARK_WORKER_WEBUI_PORT=8081 \
    SPARK_LOG_DIR=/opt/spark/logs \
    SPARK_MASTER_LOG=/opt/spark/logs/spark-master.out \
    SPARK_WORKER_LOG=/opt/spark/logs/spark-worker.out \
    SPARK_MASTER="spark://spark-master:7077" \
    SPARK_WORKLOAD="master"

# Installer Spark
RUN mkdir -p $SPARK_LOG_DIR && \
    touch $SPARK_MASTER_LOG $SPARK_WORKER_LOG && \
    ln -sf /dev/stdout $SPARK_MASTER_LOG && \
    ln -sf /dev/stdout $SPARK_WORKER_LOG

# Ports Spark
# Exposer les ports nécessaires pour Spark
EXPOSE 8080 8081 7077 6066

# Script d'entrée unique
COPY /start-spark.sh /start-spark.sh
RUN chmod +x /start-spark.sh

CMD ["/bin/bash", "/start-spark.sh"]
