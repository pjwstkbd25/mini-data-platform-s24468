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

    print(f">>> Inicjalizacja kontekstu w: {GX_ROOT_DIR}")
    context = gx.get_context(mode="file", project_root_dir=GX_ROOT_DIR)

    if isinstance(context, EphemeralDataContext):
        print(">>> Konwersja kontekstu tymczasowego na plikowy...")
        context = context.convert_to_file_context()

    datasource_name = "gold_datasource"
    try:
        ds = context.data_sources.get(datasource_name)
    except Exception:
        ds = context.data_sources.add_pandas_filesystem(
            name=datasource_name,
            base_directory=GOLD_BASE_DIR
        )

    asset_name = "gold_mental_health_asset"
    try:
        asset = ds.get_asset(asset_name)
    except Exception:
        asset = ds.add_parquet_asset(name=asset_name)

    batch_def_name = "gold_batch_def"
    try:
        batch_def = asset.get_batch_definition(batch_def_name)
    except Exception:
        batch_def = asset.add_batch_definition_path(name=batch_def_name, path=DATA_FOLDER)

    suite_name = "gold_mental_health_suite"
    try:
        suite = context.suites.get(name=suite_name)
        suite.expectations = []
    except Exception:
        suite = gx.ExpectationSuite(name=suite_name)
        suite = context.suites.add(suite)

    print(">>> Definiowanie reguł jakości dla GOLD...")

    # --- REGUŁY ---

    # Czy dane istnieją (nie są puste)
    suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(min_value=1))

    # Czy kolumna label istnieje
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="label"))

    # Czy label ma poprawne wartości (0 lub 1).
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="label",
        value_set=[0, 1, 0.0, 1.0]
    ))

    # Brak NULLi w kluczowych kolumnach
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="age"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="label"))

    # --------------

    # # Walidacja
    # validation_name = "gold_validation_run"
    # validation = gx.ValidationDefinition(
    #     data=batch_def,
    #     suite=suite,
    #     name=validation_name
    # )
    print(">>> Zapisywanie zestawu reguł (Suite Save)...")
    suite.save()
    validation_name = "gold_validation_run"
    try:
        context.validation_definitions.delete(validation_name)
        print(f">>> Usunięto stare: {validation_name}")
    except Exception:
        pass
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

    # Raport
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