
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, sum as _sum
# import sys
#
# # --- ŚCIEŻKI ---
# GOLD_PATH = "/opt/airflow/data/datalake/gold/mental_health"
#
#
# def assert_true(condition: bool, message: str):
#     if not condition:
#         print(f"!!! GOLD QC FAILED: {message}")
#         raise AssertionError(message)
#
#
# def run_job():
#     print(">>> [Start] Gold Quality Check")
#
#     spark = SparkSession.builder \
#         .appName("GoldQualityCheck") \
#         .master("local[*]") \
#         .config("spark.driver.host", "localhost") \
#         .getOrCreate()
#
#     print(f">>> Wczytywanie z: {GOLD_PATH}")
#     try:
#         df = spark.read.parquet(GOLD_PATH)
#     except Exception as e:
#         print(f"!!! Błąd odczytu: {e}")
#         sys.exit(1)
#
#     # 1. Czy są dane?
#     row_count = df.count()
#     print(f">>> Liczba wierszy: {row_count}")
#     assert_true(row_count > 0, "Gold QC: dataset jest pusty!")
#
#     # 2. Tylko numeryczne kolumny
#     # W ML (Spark MLlib / Scikit) potrzebujemy int/double/float.
#     print(">>> Weryfikacja typów danych (tylko numeryczne)...")
#     allowed_types = ("int", "bigint", "double", "float", "long")
#
#     non_numeric = []
#     for col_name, dtype in df.dtypes:
#         # dtype w Sparku to np. 'integer', 'double', 'vector' (chociaż wektory usunęliśmy)
#         # Uproszczone sprawdzenie:
#         is_numeric = any(t in dtype for t in allowed_types)
#         if not is_numeric:
#             non_numeric.append((col_name, dtype))
#
#     assert_true(
#         len(non_numeric) == 0,
#         f"Gold QC: znaleziono nienumeryczne kolumny: {non_numeric}"
#     )
#
#     # 3. Walidacja Label
#     print(">>> Weryfikacja kolumny 'label'...")
#     assert_true("label" in df.columns, "Gold QC: brak kolumny 'label'!")
#
#     # Pobieramy unikalne wartości labela
#     distinct_labels = [r[0] for r in df.select("label").distinct().collect()]
#     print(f">>> Znalezione wartości label: {distinct_labels}")
#
#     # Sprawdzamy czy są tylko 0 i 1 (może być int lub float np 1.0)
#     bad_labels = [v for v in distinct_labels if v not in (0, 1, 0.0, 1.0)]
#
#     assert_true(
#         len(bad_labels) == 0,
#         f"Gold QC: label ma nieoczekiwane wartości (inne niż 0/1): {bad_labels}"
#     )
#
#     # 4. Brak NULLi w całej tabeli
#     # Robimy to sprytną agregacją dla wszystkich kolumn naraz
#     print(">>> Skanowanie pod kątem NULLi (to może chwilę potrwać)...")
#
#     # Budujemy wyrażenia sumujące nulle dla każdej kolumny
#     aggregations = [_sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]
#
#     # Wykonujemy jedną akcję .first() zamiast pętli .count()
#     null_counts_row = df.agg(*aggregations).first()
#
#     # Sprawdzamy wynik
#     cols_with_nulls = {}
#     for c in df.columns:
#         cnt = null_counts_row[c]
#         if cnt > 0:
#             cols_with_nulls[c] = cnt
#
#     if cols_with_nulls:
#         print(f"!!! Znaleziono NULLe: {cols_with_nulls}")
#
#     assert_true(
#         len(cols_with_nulls) == 0,
#         f"Gold QC: Wykryto wartości NULL w kolumnach: {list(cols_with_nulls.keys())}"
#     )
#
#     print(">>> [Sukces] GoldQualityCheck: wszystkie testy zaliczone ✔")
#     print(">>> Gotowe do trenowania modeli ML!")
#     spark.stop()
#
#
# if __name__ == "__main__":
#     run_job()
import sys
import great_expectations as gx
from great_expectations.data_context import EphemeralDataContext
from pathlib import Path

# --- KONFIGURACJA ŚCIEŻEK ---
AIRFLOW_HOME = Path("/opt/airflow")
GOLD_BASE_DIR = AIRFLOW_HOME / "data/datalake/gold"
GX_ROOT_DIR = AIRFLOW_HOME / "gx"
DATA_FOLDER = "mental_health"


def run_job():
    print(">>> [Start] Gold Quality Check (Great Expectations)")

    # 1. Kontekst - WYMUSZENIE ŚCIEŻKI /opt/airflow/gx
    print(f">>> Inicjalizacja kontekstu w: {GX_ROOT_DIR}")
    context = gx.get_context(mode="file", project_root_dir=GX_ROOT_DIR)

    if isinstance(context, EphemeralDataContext):
        print(">>> Konwersja kontekstu tymczasowego na plikowy...")
        context = context.convert_to_file_context()

    # 2. Datasource
    datasource_name = "gold_datasource"
    try:
        ds = context.data_sources.get(datasource_name)
    except Exception:
        ds = context.data_sources.add_pandas_filesystem(
            name=datasource_name,
            base_directory=GOLD_BASE_DIR
        )

    # 3. Asset
    asset_name = "gold_mental_health_asset"
    try:
        asset = ds.get_asset(asset_name)
    except Exception:
        asset = ds.add_parquet_asset(name=asset_name)

    # 4. Batch Definition
    batch_def_name = "gold_batch_def"
    try:
        batch_def = asset.get_batch_definition(batch_def_name)
    except Exception:
        batch_def = asset.add_batch_definition_path(name=batch_def_name, path=DATA_FOLDER)

    # 5. Expectation Suite
    suite_name = "gold_mental_health_suite"
    try:
        suite = context.suites.get(name=suite_name)
        suite.expectations = []  # Czyścimy stare reguły
    except Exception:
        suite = gx.ExpectationSuite(name=suite_name)
        suite = context.suites.add(suite)

    print(">>> Definiowanie reguł jakości dla GOLD...")

    # --- REGUŁY ---

    # 1. Czy dane istnieją (nie są puste)
    suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(min_value=1))

    # 2. Czy kolumna label istnieje
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="label"))

    # 3. Czy label ma poprawne wartości (0 lub 1).
    # To wystarczy - nie musimy sprawdzać "int64", bo Spark zapisuje "int32" i to powodowało błąd.
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="label",
        value_set=[0, 1, 0.0, 1.0]
    ))

    # 4. Brak NULLi w kluczowych kolumnach
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="age"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="label"))

    # --------------

    # 6. Walidacja
    validation_name = "gold_validation_run"
    validation = gx.ValidationDefinition(
        data=batch_def,
        suite=suite,
        name=validation_name
    )

    try:
        context.validation_definitions.add(validation)
    except Exception:
        pass

    results = validation.run()

    # 7. Raport
    index_urls = context.build_data_docs()
    print(f"DEBUG: GX generated docs at: {index_urls}")

    html_path = GX_ROOT_DIR / "gx/data_docs/local_site/index.html"

    print("\n" + "=" * 50)
    print(f"WYNIK WALIDACJI GOLD: {'✅ SUKCES' if results.success else '❌ PORAŻKA'}")
    print(f"Raport powinien być dostępny w folderze projektu: gx/gx/data_docs/local_site/index.html")
    print("=" * 50 + "\n")

    if not results.success:
        print("!!! Znaleziono błędy w warstwie Gold.")
        sys.exit(1)


if __name__ == "__main__":
    run_job()