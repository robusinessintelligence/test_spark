FROM apache/spark:latest

USER root

RUN apt-get update && apt-get install -y libnss-wrapper && \
    pip install --no-cache-dir requests pandas pyarrow && \
    rm -rf /var/lib/apt/lists/*

RUN chown -R 1001:0 /opt/spark && \
    chmod -R g+rwX /opt/spark

ENV SPARK_USER=spark
ENV HADOOP_USER_NAME=spark

USER 1001