import sys
import great_expectations as gx
from great_expectations.data_context import EphemeralDataContext
from pathlib import Path

# --- KONFIGURACJA ŚCIEŻEK ---
AIRFLOW_HOME = Path("/opt/airflow")
SILVER_BASE_DIR = AIRFLOW_HOME / "data/datalake/silver"
GX_ROOT_DIR = AIRFLOW_HOME / "gx"  # <--- WSKAZUJEMY KONKRETNY FOLDER
DATA_FOLDER = "mental_health"


def run_job():
    print(">>> [Start] Silver Quality Check (Great Expectations)")

    # 1. Kontekst GX - WYMUSZENIE ŚCIEŻKI
    # Mówimy: "Twój dom jest w /opt/airflow/gx".
    print(f">>> Inicjalizacja kontekstu w: {GX_ROOT_DIR}")
    context = gx.get_context(mode="file", project_root_dir=GX_ROOT_DIR)

    # Jeśli folder był pusty, GX zwróci kontekst Ephemeral (tymczasowy).
    # Musimy go wtedy "zakotwiczyć" na dysku.
    if isinstance(context, EphemeralDataContext):
        print(">>> Konwersja kontekstu tymczasowego na plikowy...")
        context = context.convert_to_file_context()

    # 2. Datasource
    datasource_name = "silver_datasource"
    try:
        ds = context.data_sources.get(datasource_name)
    except Exception:
        ds = context.data_sources.add_pandas_filesystem(
            name=datasource_name,
            base_directory=SILVER_BASE_DIR
        )

    # 3. Asset
    asset_name = "silver_mental_health_asset"
    try:
        asset = ds.get_asset(asset_name)
    except Exception:
        asset = ds.add_parquet_asset(name=asset_name)

    # 4. Batch Definition
    batch_def_name = "silver_batch_def"
    try:
        batch_def = asset.get_batch_definition(batch_def_name)
    except Exception:
        batch_def = asset.add_batch_definition_path(name=batch_def_name, path=DATA_FOLDER)

    # 5. Expectation Suite
    suite_name = "silver_mental_health_suite"
    try:
        suite = context.suites.get(name=suite_name)
        suite.expectations = []
    except Exception:
        suite = gx.ExpectationSuite(name=suite_name)
        suite = context.suites.add(suite)

    print(">>> Definiowanie reguł jakości...")

    # --- REGUŁY ---
    suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(min_value=1))

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="age", min_value=15, max_value=100
    ))

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="depression",
        value_set=["Yes", "No", "yes", "no", "0", "1", 0, 1]
    ))

    required_cols = ["gender", "age", "sleep duration", "dietary habits", "financial stress", "depression"]
    for col in required_cols:
        suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=col))

    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="gender_vec"))
    # --------------

    # 6. Walidacja
    validation_name = "silver_validation_run"
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
    print(">>> Generowanie raportu HTML...")
    index_urls = context.build_data_docs()
    print(f"DEBUG: GX generated docs at: {index_urls}")

    html_path = GX_ROOT_DIR / "gx/data_docs/local_site/index.html"

    print("\n" + "=" * 50)
    print(f"WYNIK WALIDACJI: {'✅ SUKCES' if results.success else '❌ PORAŻKA'}")
    print(f"Raport powinien być tutaj: gx/gx/data_docs/local_site/index.html (lub podobnie)")
    print("=" * 50 + "\n")

    if not results.success:
        print("!!! Znaleziono błędy w danych.")
        sys.exit(1)


if __name__ == "__main__":
    run_job()