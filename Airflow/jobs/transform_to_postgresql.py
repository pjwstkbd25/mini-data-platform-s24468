import os
import time
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, Integer, Float, Text, DateTime, Boolean, text
from dotenv import load_dotenv

# -------------------- Env / Paths --------------------
# BASE_DIR -> folder "Airflow"
BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH)


def resolve_env_path(var_name: str, default_subpath: str) -> str:
    raw = os.getenv(var_name)
    if raw:
        p = Path(raw)
        if not p.is_absolute():
            p = BASE_DIR / p
    else:
        p = BASE_DIR / default_subpath
    p = p.resolve()
    os.environ[var_name] = str(p)
    return str(p)


# Ujednolić ścieżki z .env (względne -> absolutne)
resolve_env_path("KAGGLE_CONFIG_DIR", "Airflow/secrets")
resolve_env_path("KAGGLE_DEST_DIR", "Airflow/data/datasets")


# -------------------- Helpers --------------------
def env(name: str, default: str | None = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and (val is None or val == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val


def infer_sqlalchemy_type(series: pd.Series):
    if pd.api.types.is_bool_dtype(series):
        return Boolean()
    if pd.api.types.is_integer_dtype(series):
        return Integer()
    if pd.api.types.is_float_dtype(series):
        return Float()
    if pd.api.types.is_datetime64_any_dtype(series):
        return DateTime()
    return Text()


def make_pg_engine():
    host = env("PG_HOST", required=True)
    port = int(env("PG_PORT", "5432"))
    db = env("PG_DB", required=True)
    user = env("PG_USER", required=True)
    pwd = env("PG_PASSWORD", required=True)

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    engine = create_engine(url, pool_pre_ping=True, future=True)
    return engine


def wait_for_db(engine, seconds: int = 60):
    for _ in range(seconds):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("PostgreSQL not available after waiting.")


def load_csv_to_postgres(csv_path: Path, table_name: str, engine, schema: str | None = None):
    df = pd.read_csv(csv_path)

    for col in df.columns:
        name = col.lower()
        if any(name.endswith(suf) for suf in ("_date", "_dt", "_timestamp")):
            df[col] = pd.to_datetime(df[col], errors="ignore")

    dtype_map = {col: infer_sqlalchemy_type(df[col]) for col in df.columns}

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False,
        dtype=dtype_map,
        schema=schema
    )

    with engine.connect() as conn:
        fq = f'"{schema}".{table_name}' if schema else table_name
        conn.execute(text(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = '{table_name}'
                      AND table_schema = COALESCE('{schema}','public')
                      AND column_name = 'id'
                ) THEN
                    ALTER TABLE {fq} ADD COLUMN id SERIAL PRIMARY KEY;
                END IF;
            END $$;
        """))
        conn.commit()


def ensure_schema(engine, schema: str):
    if not schema:
        return
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        conn.commit()


def run_datagen():
    dest_dir = Path(env("KAGGLE_DEST_DIR", str(BASE_DIR / "data" / "datasets")))
    csv_files = list(dest_dir.rglob("*.csv"))
    engine = make_pg_engine()
    wait_for_db(engine, seconds=60)
    schema = env("PG_SCHEMA", "public")
    ensure_schema(engine, schema)
    prefix = env("PG_TABLE_PREFIX", "kaggle_")
    for csv in csv_files:
        table_base = csv.stem.lower().replace(" ", "_")
        table_name = f"{prefix}{table_base}"
        load_csv_to_postgres(csv, table_name, engine, schema=schema)
    print(f"✨ Załadowano {len(csv_files)} plików CSV do Postgresa (schema={schema}).")


if __name__ == "__main__":
    run_datagen()
