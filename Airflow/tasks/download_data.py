import os
from pathlib import Path
from typing import List

from kaggle.api.kaggle_api_extended import KaggleApi
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

# -------------------- Kaggle download --------------------
def download_kaggle_dataset(dataset_slug: str, dest_dir: str) -> List[Path]:
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)

    cfg_dir = Path(env("KAGGLE_CONFIG_DIR", required=True))
    if not (cfg_dir / "kaggle.json").exists():
        raise FileNotFoundError(f"Brak pliku kaggle.json w {cfg_dir}")

    api = KaggleApi()
    api.authenticate()


    api.dataset_download_files(dataset_slug, path=dest, unzip=False)

    for z in dest.glob("*.zip"):
        import zipfile
        with zipfile.ZipFile(z, "r") as zip_ref:
            zip_ref.extractall(dest)
        z.unlink(missing_ok=True)

    # csvs = list(dest.rglob("*.csv"))
    # if not csvs:
    #     raise RuntimeError(f"Brak plików CSV po pobraniu datasetu {dataset_slug} do {dest}")

def run_datagen():
    dataset_slug = env("KAGGLE_DATASET", required=True)
    dest_dir     = env("KAGGLE_DEST_DIR", str(BASE_DIR / "data" / "datasets"))

    download_kaggle_dataset(dataset_slug, dest_dir)


if __name__ == "__main__":
    run_datagen()
