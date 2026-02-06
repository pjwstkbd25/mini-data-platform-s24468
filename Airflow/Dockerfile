FROM apache/airflow:2.10.2-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jdk-headless procps && apt-get autoremove -yqq --purge && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt