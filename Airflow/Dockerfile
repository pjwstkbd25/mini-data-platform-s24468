## Bazujemy na oficjalnym obrazie Airflow (tym samym co w docker-compose)
#FROM apache/airflow:2.10.2-python3.11
#
## Przełączamy się na roota, żeby zainstalować pakiety systemowe (Javę)
#USER root
#
## Instalacja OpenJDK 17 (wymagane przez Sparka) oraz procps (przydatne narzędzia)
#RUN apt-get update \
#  && apt-get install -y --no-install-recommends \
#         openjdk-17-jdk-headless \
#         procps \
#  && apt-get autoremove -yqq --purge \
#  && apt-get clean \
#  && rm -rf /var/lib/apt/lists/*
#
## Ustawienie zmiennej środowiskowej JAVA_HOME
#ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
#
## Przełączamy się z powrotem na użytkownika airflow
#USER airflow
#
## Kopiujemy requirements.txt do obrazu
#COPY requirements.txt /requirements.txt
#
## Instalujemy biblioteki Pythonowe (w tym PySpark) podczas budowania obrazu
#RUN pip install --no-cache-dir -r /requirements.txt



#FROM apache/airflow:2.10.2-python3.11
#
#USER root
#
## Instalacja Javy (wymagane przez Sparka)
#RUN apt-get update \
#  && apt-get install -y --no-install-recommends \
#         openjdk-17-jdk-headless \
#         procps \
#  && apt-get autoremove -yqq --purge \
#  && apt-get clean \
#  && rm -rf /var/lib/apt/lists/*
#
## Ustawienie JAVA_HOME
#ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
#
#USER airflow
#
## Kopiowanie i instalacja bibliotek
#COPY requirements.txt /requirements.txt
#RUN pip install --no-cache-dir -r /requirements.txt


FROM apache/airflow:2.10.2-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jdk-headless procps && apt-get autoremove -yqq --purge && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt