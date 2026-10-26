FROM apache/airflow:3.3.2

USER root
# Spark runs on the JAV 
# procps provides ps, which sparks launch scripts call
RUN apt-get update \
	&& apt-get install -y --no-install-recommends default-jre-headless procps \
	&& apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements-airflow.txt