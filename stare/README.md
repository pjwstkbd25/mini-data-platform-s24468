[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/ano0EjUK)


# Szybki Start – Przewodnik krok po kroku
## 1. Uruchomienie całego środowiska

 ```
# (Tylko raz, żeby usunąć ewentualne stare kontenery i wolumeny)
docker compose down -v        

# Zbuduj obrazy i włącz serwisy w tle
docker compose up -d --build
 ```

Uruchomione zostaną:

- PostgreSQL z włączonym logical replication
- Debezium Connect (CDC → Kafka w formacie Avro)
- Kafka + Schema Registry
- Spark ze streamingiem
- MinIO do przechowywania plików Delta Lake
- Flask app do załadowania CSV

## 2. Weryfikacja podstaw
Tematy i dane w Kafka:
Otwórz Kafdrop: http://localhost:9000

Spark UI (monitor jobów):
http://localhost:4040/jobs/

MinIO Browser (utwórz bucket, jeśli nie istnieje):
http://localhost:9101 (login: minioadmin/minioadmin)

Przejdź do Buckets

Kliknij Create bucket, nadaj nazwę spark-output


## 3. Przepchnięcie zmiany przez CDC
1. Połącz się do Postgresa (np. przez pgAdmin, psql) na porcie 5433.

2. Wykonaj w tabeli public.educational_data:

 ```
INSERT INTO educational_data
  ("Full Name","Age","Education Level","Major",
   "Year Started Education","Year Completed Education",
   "Type of Educational Institution","Average Grade")
VALUES
  ('John Doe',22,'Tertiary','Computer Science',
   2018,2022,'Public',3.5);
 ```
3. W Kafdrop zobaczysz nową wiadomość na topicu
data_source.public.educational_data.

## 4. Sprawdzenie danych w MinIO
Po przetworzeniu rekordu Spark zapisze go w postaci pliku Parquet w ścieżce:
s3a://spark-output/educational_data/part-*.parquet
W MinIO Browser przejdź do spark-output → educational_data i zobacz nowy plik.

## 5. Odczyt w notebooku Python
 ```
import pandas as pd

df = pd.read_parquet(
    "s3://spark-output/educational_data/",
    engine="pyarrow",
    storage_options={
        "key": "minioadmin",
        "secret": "minioadmin",
        "client_kwargs": {"endpoint_url": "http://localhost:9100"},
        "use_ssl": False
    }
)

df.head()
 ```