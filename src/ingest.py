"""
ingest.py — Coleta dados de filmes via TMDB + OMDB e salva em data/raw.

Uso:
    uv run src/ingest.py

Requisitos de ambiente (.env na raiz do projeto):
    TMDB_API_KEY=xxxx
    OMDB_API_KEY=xxxx
    TMDB_PAGES=3          # opcional, default 3 (20 filmes por página)

Garantias:
    - Idempotência: mesmos parâmetros de execução não geram chamadas
      redundantes à API nem duplicam dados — o script calcula um hash
      da configuração e verifica se já existe um snapshot idêntico
      salvo hoje antes de coletar de novo.
    - Reprodutibilidade: cada arquivo de dados é acompanhado de um
      arquivo .meta.json com todos os parâmetros, timestamps e
      versões usadas na coleta.
    - Escrita atômica: os dados são gravados em um arquivo temporário
      e só então renomeados, evitando arquivos corrompidos caso a
      execução seja interrompida no meio.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --------------------------------------------------------------------------
# Configuração
# --------------------------------------------------------------------------

ROOT_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT_DIR / "data" / "raw"
LOG_DIR = ROOT_DIR / "logs"
RAW_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

load_dotenv(ROOT_DIR / ".env")

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
OMDB_API_KEY = os.getenv("OMDB_API_KEY")
TMDB_PAGES = int(os.getenv("TMDB_PAGES", "3"))
TMDB_SORT_BY = "popularity.desc"

if not TMDB_API_KEY or not OMDB_API_KEY:
    print("ERRO: defina TMDB_API_KEY e OMDB_API_KEY no arquivo .env")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "ingest.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("ingest")

TMDB_BASE = "https://api.themoviedb.org/3"
OMDB_BASE = "http://www.omdbapi.com/"


def build_session() -> requests.Session:
    """Sessão HTTP com retry/backoff automático para falhas transitórias."""
    session = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    return session


SESSION = build_session()


# --------------------------------------------------------------------------
# Coleta
# --------------------------------------------------------------------------

def fetch_tmdb_discover_page(page: int) -> list[dict]:
    resp = SESSION.get(
        f"{TMDB_BASE}/discover/movie",
        params={
            "api_key": TMDB_API_KEY,
            "language": "pt-BR",
            "sort_by": TMDB_SORT_BY,
            "page": page,
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json().get("results", [])


def fetch_tmdb_movie_details(movie_id: int) -> dict:
    resp = SESSION.get(
        f"{TMDB_BASE}/movie/{movie_id}",
        params={
            "api_key": TMDB_API_KEY,
            "language": "pt-BR",
            "append_to_response": "credits,keywords",
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_omdb_by_imdb_id(imdb_id: str) -> dict | None:
    if not imdb_id:
        return None
    resp = SESSION.get(
        OMDB_BASE,
        params={"apikey": OMDB_API_KEY, "i": imdb_id, "plot": "full"},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("Response") == "False":
        log.warning("OMDB sem dados para %s: %s", imdb_id, data.get("Error"))
        return None
    return data


# --------------------------------------------------------------------------
# Idempotência / reprodutibilidade
# --------------------------------------------------------------------------

def config_hash(config: dict) -> str:
    payload = json.dumps(config, sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:12]


def already_collected_today(chash: str) -> Path | None:
    """Verifica se já existe um snapshot com a mesma config hoje."""
    today = datetime.now().strftime("%Y%m%d")
    for meta_path in RAW_DIR.glob(f"dados-tmdb_omdb-{today}*.meta.json"):
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        if meta.get("config_hash") == chash:
            data_path = meta_path.with_suffix("").with_suffix(".json")
            if data_path.exists():
                return data_path
    return None


def atomic_write_json(path: Path, data) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    tmp_path.replace(path)  # rename é atômico no mesmo filesystem


# --------------------------------------------------------------------------
# Execução principal
# --------------------------------------------------------------------------

def main() -> None:
    config = {
        "tmdb_pages": TMDB_PAGES,
        "tmdb_sort_by": TMDB_SORT_BY,
        "source": "discover/movie + movie details + omdb",
    }
    chash = config_hash(config)

    existing = already_collected_today(chash)
    if existing:
        log.info(
            "Config idêntica já coletada hoje em %s — pulando nova coleta "
            "(idempotência). Apague o arquivo ou mude os parâmetros para forçar.",
            existing.name,
        )
        return

    run_start = datetime.now()
    timestamp = run_start.strftime("%Y%m%d-%H%M%S")

    log.info("Buscando %d página(s) de filmes populares no TMDB...", TMDB_PAGES)
    stub_movies = []
    for page in range(1, TMDB_PAGES + 1):
        stub_movies.extend(fetch_tmdb_discover_page(page))
        time.sleep(0.3)  # não martelar a API

    log.info("%d filmes encontrados. Buscando detalhes + OMDB...", len(stub_movies))

    collected = []
    errors = []
    for stub in stub_movies:
        movie_id = stub["id"]
        try:
            details = fetch_tmdb_movie_details(movie_id)
            imdb_id = details.get("imdb_id")
            omdb_data = fetch_omdb_by_imdb_id(imdb_id) if imdb_id else None
            collected.append(
                {
                    "tmdb_id": movie_id,
                    "tmdb_details": details,
                    "omdb_data": omdb_data,
                }
            )
        except requests.RequestException as exc:
            log.error("Falha ao coletar filme %s: %s", movie_id, exc)
            errors.append({"tmdb_id": movie_id, "error": str(exc)})
        time.sleep(0.25)

    data_filename = RAW_DIR / f"dados-tmdb_omdb-{timestamp}.json"
    meta_filename = RAW_DIR / f"dados-tmdb_omdb-{timestamp}.meta.json"

    atomic_write_json(data_filename, collected)
    atomic_write_json(
        meta_filename,
        {
            "config": config,
            "config_hash": chash,
            "run_started_at": run_start.isoformat(),
            "run_finished_at": datetime.now().isoformat(),
            "n_movies_collected": len(collected),
            "n_errors": len(errors),
            "errors": errors,
            "python_version": sys.version,
            "requests_version": requests.__version__,
        },
    )

    log.info(
        "Concluído: %d filmes salvos em %s (metadados em %s)",
        len(collected),
        data_filename.name,
        meta_filename.name,
    )


if __name__ == "__main__":
    main()
