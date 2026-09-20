"""
ingest.py — Coleta incremental de dados de filmes via TMDB + OMDB.

Uso:
    uv run src/ingest.py

Requisitos de ambiente (.env na raiz do projeto):
    TMDB_API_KEY=xxxx
    OMDB_API_KEY=xxxx
    MAX_MOVIES_PER_RUN=50       # opcional, quantos filmes NOVOS coletar por execução
    TMDB_REQUESTS_PER_SEC=4     # opcional, limite de requisições/segundo ao TMDB
    OMDB_MIN_INTERVAL_SEC=1.0   # opcional, intervalo mínimo entre requisições ao OMDB
    OMDB_DAILY_LIMIT=900        # opcional, margem de segurança sob o limite de 1000/dia do free tier

Como funciona a coleta incremental:
    - Cada execução continua de onde a anterior parou (página do TMDB
      discover + IDs já coletados), em vez de recomeçar do zero.
    - Um arquivo de estado (state/ingest_state.json) guarda: cursor de
      paginação, IDs de filmes já coletados e contadores de requisição
      do dia atual (reiniciados automaticamente à meia-noite).
    - Antes de cada chamada, um rate limiter garante o intervalo mínimo
      exigido por segundo; antes de cada chamada ao OMDB, o contador
      diário é conferido contra o limite configurado.

Garantias de idempotência e reprodutibilidade:
    - Idempotência: um filme já coletado (ID salvo no estado) nunca é
      buscado de novo — rodar o script várias vezes não duplica
      trabalho nem estoura os limites de requisição, mesmo após uma
      queda no meio de uma execução (o estado só é regravado de forma
      atômica ao final de cada filme processado).
    - Reprodutibilidade: cada execução grava um arquivo de dados
      timestamped em data/raw acompanhado de um .meta.json com os
      parâmetros, contadores de requisição e versões usadas.
    - Escrita atômica: todo arquivo (dados, metadados, estado) é
      gravado em um .tmp e só então renomeado, evitando corrupção se a
      execução for interrompida no meio.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import date, datetime
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
STATE_DIR = ROOT_DIR / "state"
STATE_PATH = STATE_DIR / "ingest_state.json"

for d in (RAW_DIR, LOG_DIR, STATE_DIR):
    d.mkdir(parents=True, exist_ok=True)

load_dotenv(ROOT_DIR / ".env")

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
OMDB_API_KEY = os.getenv("OMDB_API_KEY")
TMDB_SORT_BY = "popularity.desc"
MAX_MOVIES_PER_RUN = int(os.getenv("MAX_MOVIES_PER_RUN", "50"))
TMDB_REQUESTS_PER_SEC = float(os.getenv("TMDB_REQUESTS_PER_SEC", "4"))
OMDB_MIN_INTERVAL_SEC = float(os.getenv("OMDB_MIN_INTERVAL_SEC", "1.0"))
OMDB_DAILY_LIMIT = int(os.getenv("OMDB_DAILY_LIMIT", "900"))

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
TMDB_MAX_PAGE = 500  # limite da própria API do TMDB para discover


def build_session() -> requests.Session:
    # Retries aqui cobrem só falhas de conexão (timeout, DNS, conexão
    # recusada) — erros HTTP (401/429/5xx) são tratados explicitamente
    # em safe_get(), porque cada um exige uma reação diferente.
    session = requests.Session()
    retry = Retry(total=2, backoff_factor=1.0, allowed_methods=["GET"])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    return session


SESSION = build_session()


class CredentialError(RuntimeError):
    """401 — a chave está errada/expirada. Não adianta tentar de novo."""


class TransientAPIError(RuntimeError):
    """5xx ou 429 esgotado — falha do lado do servidor ou limite de taxa."""


def safe_get(url: str, params: dict, api_name: str, max_retries: int = 4) -> requests.Response:
    """GET com reação diferente para cada classe de erro HTTP.

    - 401 (credencial recusada): não retry — a chave está errada, tentar
      de novo só desperdiça requisições do orçamento diário.
    - 429 (limite de requisições): espera o tempo indicado em
      Retry-After (ou um backoff exponencial, se o cabeçalho não vier) e
      tenta de novo.
    - 5xx (falha do servidor): trata como falha transitória, com
      backoff exponencial.
    - Qualquer outro erro HTTP (404, 400, etc.): propaga normalmente,
      não é um caso que retry resolve.
    """
    for attempt in range(1, max_retries + 1):
        resp = SESSION.get(url, params=params, timeout=15)

        if resp.status_code == 401:
            raise CredentialError(
                f"{api_name}: credencial recusada (401) — verifique a chave no .env"
            )

        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 2**attempt))
            log.warning(
                "%s: limite de requisições (429), aguardando %ss "
                "(tentativa %d/%d)",
                api_name,
                wait,
                attempt,
                max_retries,
            )
            time.sleep(wait)
            continue

        if 500 <= resp.status_code < 600:
            wait = 2**attempt
            log.warning(
                "%s: falha do servidor (%d), tentando de novo em %ss "
                "(tentativa %d/%d)",
                api_name,
                resp.status_code,
                wait,
                attempt,
                max_retries,
            )
            time.sleep(wait)
            continue

        resp.raise_for_status()  # outros 4xx viram exceção normal, sem retry
        return resp

    raise TransientAPIError(
        f"{api_name}: excedeu {max_retries} tentativas (429/5xx persistente)"
    )


# --------------------------------------------------------------------------
# Rate limiting
# --------------------------------------------------------------------------

class RateLimiter:
    """Garante um intervalo mínimo entre chamadas consecutivas."""

    def __init__(self, min_interval_sec: float):
        self.min_interval = min_interval_sec
        self._last_call = 0.0

    def wait(self) -> None:
        elapsed = time.monotonic() - self._last_call
        remaining = self.min_interval - elapsed
        if remaining > 0:
            time.sleep(remaining)
        self._last_call = time.monotonic()


tmdb_limiter = RateLimiter(1.0 / TMDB_REQUESTS_PER_SEC)
omdb_limiter = RateLimiter(OMDB_MIN_INTERVAL_SEC)


# --------------------------------------------------------------------------
# Estado persistido (cursor de paginação, IDs coletados, contadores diários)
# --------------------------------------------------------------------------

def default_state() -> dict:
    return {
        "date": date.today().isoformat(),
        "next_discover_page": 1,
        "collected_tmdb_ids": [],
        "tmdb_requests_today": 0,
        "omdb_requests_today": 0,
        "pendentes_omdb": [],
    }


def load_state() -> dict:
    if not STATE_PATH.exists():
        return default_state()
    state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    state.setdefault("pendentes_omdb", [])  # compatibilidade com state.json antigo
    if state.get("date") != date.today().isoformat():
        log.info("Novo dia detectado — zerando contadores de requisição diários.")
        state["date"] = date.today().isoformat()
        state["tmdb_requests_today"] = 0
        state["omdb_requests_today"] = 0
    return state


def atomic_write_json(path: Path, data) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    tmp_path.replace(path)


def save_state(state: dict) -> None:
    atomic_write_json(STATE_PATH, state)


# --------------------------------------------------------------------------
# Coleta
# --------------------------------------------------------------------------

def fetch_tmdb_discover_page(page: int) -> list[dict]:
    tmdb_limiter.wait()
    resp = safe_get(
        f"{TMDB_BASE}/discover/movie",
        params={
            "api_key": TMDB_API_KEY,
            "language": "pt-BR",
            "sort_by": TMDB_SORT_BY,
            "page": page,
        },
        api_name="TMDB",
    )
    return resp.json().get("results", [])


def fetch_tmdb_movie_details(movie_id: int) -> dict:
    tmdb_limiter.wait()
    resp = safe_get(
        f"{TMDB_BASE}/movie/{movie_id}",
        params={
            "api_key": TMDB_API_KEY,
            "language": "pt-BR",
            "append_to_response": "credits,keywords",
        },
        api_name="TMDB",
    )
    return resp.json()


def fetch_omdb_by_imdb_id(imdb_id: str) -> dict | None:
    if not imdb_id:
        return None
    omdb_limiter.wait()
    resp = safe_get(
        OMDB_BASE,
        params={"apikey": OMDB_API_KEY, "i": imdb_id, "plot": "full"},
        api_name="OMDB",
    )
    data = resp.json()
    if data.get("Response") == "False":
        log.warning("OMDB sem dados para %s: %s", imdb_id, data.get("Error"))
        return None
    return data


def backfill_omdb(state: dict) -> list[dict]:
    """Reprocessa filmes que já foram coletados (tmdb_id em
    collected_tmdb_ids) mas ficaram sem enriquecimento OMDB por
    orçamento diário esgotado na época — sem isso, esses filmes
    ficariam incompletos para sempre, já que o cursor de paginação
    nunca mais passa por eles de novo. Roda ANTES da coleta de filmes
    novos em cada execução, enquanto houver orçamento OMDB do dia."""
    pendentes = list(state.get("pendentes_omdb", []))
    if not pendentes:
        return []

    log.info("%d filme(s) pendente(s) de enriquecimento OMDB — tentando recuperar.", len(pendentes))
    recovered: list[dict] = []
    still_pending: list[int] = []

    for movie_id in pendentes:
        if state["omdb_requests_today"] >= OMDB_DAILY_LIMIT:
            still_pending.append(movie_id)  # orçamento acabou de novo, tenta no próximo dia
            continue
        try:
            details = fetch_tmdb_movie_details(movie_id)
            state["tmdb_requests_today"] += 1
            imdb_id = details.get("imdb_id")
            omdb_data = fetch_omdb_by_imdb_id(imdb_id) if imdb_id else None
            if imdb_id:
                state["omdb_requests_today"] += 1
            if omdb_data is not None:
                recovered.append(
                    {
                        "tmdb_id": movie_id,
                        "tmdb_details": details,
                        "omdb_data": omdb_data,
                        "omdb_skipped_daily_budget": False,
                    }
                )
            # Se ainda vier None mesmo com orçamento disponível, o motivo
            # não é mais orçamento (ex.: imdb_id inválido) — não adianta
            # colocar de volta na fila, então simplesmente não re-adiciona.
        except CredentialError as exc:
            state["pendentes_omdb"] = still_pending + [
                m for m in pendentes if m not in still_pending and m >= movie_id
            ]
            save_state(state)
            log.critical(str(exc))
            sys.exit(1)
        except (requests.RequestException, TransientAPIError) as exc:
            log.error("Falha ao reprocessar OMDB do filme %s: %s", movie_id, exc)
            still_pending.append(movie_id)

    state["pendentes_omdb"] = still_pending
    log.info(
        "Backfill OMDB: %d recuperado(s), %d ainda pendente(s) para o próximo dia.",
        len(recovered), len(still_pending),
    )
    return recovered


# --------------------------------------------------------------------------
# Execução principal
# --------------------------------------------------------------------------

def main() -> None:
    state = load_state()
    collected_ids = set(state["collected_tmdb_ids"])
    page = state["next_discover_page"]

    run_start = datetime.now()
    timestamp = run_start.strftime("%Y%m%d-%H%M%S")

    new_records: list[dict] = list(backfill_omdb(state))
    save_state(state)
    n_recovered = len(new_records)

    errors: list[dict] = []
    omdb_budget_hit = False
    n_new = 0

    log.info(
        "Início da coleta: %d filmes já coletados até agora, retomando a partir "
        "da página %d do TMDB discover.",
        len(collected_ids),
        page,
    )

    while n_new < MAX_MOVIES_PER_RUN and page <= TMDB_MAX_PAGE:
        try:
            stubs = fetch_tmdb_discover_page(page)
            state["tmdb_requests_today"] += 1
        except CredentialError as exc:
            save_state(state)
            log.critical(str(exc))
            sys.exit(1)
        except (requests.RequestException, TransientAPIError) as exc:
            log.error("Falha ao buscar página %d do discover: %s", page, exc)
            break

        if not stubs:
            log.info("TMDB não retornou mais resultados na página %d.", page)
            break

        for stub in stubs:
            if n_new >= MAX_MOVIES_PER_RUN:
                break

            movie_id = stub["id"]
            if movie_id in collected_ids:
                continue  # já coletado em execução anterior — idempotência

            try:
                details = fetch_tmdb_movie_details(movie_id)
                state["tmdb_requests_today"] += 1

                imdb_id = details.get("imdb_id")
                omdb_data = None
                omdb_skipped_this_movie = False
                if imdb_id and state["omdb_requests_today"] < OMDB_DAILY_LIMIT:
                    omdb_data = fetch_omdb_by_imdb_id(imdb_id)
                    state["omdb_requests_today"] += 1
                elif imdb_id:
                    omdb_skipped_this_movie = True
                    if not omdb_budget_hit:
                        omdb_budget_hit = True
                        log.warning(
                            "Limite diário do OMDB (%d) atingido — filmes restantes "
                            "deste run ficam sem enriquecimento OMDB até o backfill "
                            "de uma próxima execução.",
                            OMDB_DAILY_LIMIT,
                        )

                new_records.append(
                    {
                        "tmdb_id": movie_id,
                        "tmdb_details": details,
                        "omdb_data": omdb_data,
                        "omdb_skipped_daily_budget": omdb_skipped_this_movie,
                    }
                )
                if omdb_skipped_this_movie:
                    state.setdefault("pendentes_omdb", []).append(movie_id)
                collected_ids.add(movie_id)
                n_new += 1

                # Estado salvo a cada filme: se o processo cair no meio,
                # a próxima execução não refaz nem perde trabalho já feito.
                state["collected_tmdb_ids"] = sorted(collected_ids)
                save_state(state)

            except CredentialError as exc:
                state["collected_tmdb_ids"] = sorted(collected_ids)
                save_state(state)
                log.critical(str(exc))
                sys.exit(1)
            except (requests.RequestException, TransientAPIError) as exc:
                log.error("Falha ao coletar filme %s: %s", movie_id, exc)
                errors.append({"tmdb_id": movie_id, "error": str(exc)})

        page += 1

    state["next_discover_page"] = page
    save_state(state)

    if not new_records:
        log.info("Nenhum filme novo coletado nesta execução.")
        return

    data_filename = RAW_DIR / f"dados-tmdb_omdb-{timestamp}.json"
    meta_filename = RAW_DIR / f"dados-tmdb_omdb-{timestamp}.meta.json"

    atomic_write_json(data_filename, new_records)
    atomic_write_json(
        meta_filename,
        {
            "fonte": {
                "tmdb": f"{TMDB_BASE}/discover/movie + /movie/{{id}}",
                "omdb": OMDB_BASE,
            },
            "run_started_at": run_start.isoformat(),
            "run_finished_at": datetime.now().isoformat(),
            "n_movies_collected_this_run": len(new_records),
            "n_movies_recovered_backfill_omdb": n_recovered,
            "n_movies_collected_total": len(collected_ids),
            "n_pendentes_omdb_restantes": len(state.get("pendentes_omdb", [])),
            "n_errors": len(errors),
            "errors": errors,
            "omdb_daily_budget_hit": omdb_budget_hit,
            "tmdb_requests_today": state["tmdb_requests_today"],
            "omdb_requests_today": state["omdb_requests_today"],
            "tmdb_requests_per_sec_limit": TMDB_REQUESTS_PER_SEC,
            "omdb_min_interval_sec": OMDB_MIN_INTERVAL_SEC,
            "omdb_daily_limit": OMDB_DAILY_LIMIT,
            "next_discover_page": page,
            "python_version": sys.version,
            "requests_version": requests.__version__,
        },
    )

    summary = (
        f"Concluído: {len(new_records)} filmes gravados nesta execução "
        f"({n_recovered} recuperados via backfill OMDB, {len(new_records) - n_recovered} novos; "
        f"total acumulado: {len(collected_ids)}). Gravado em {data_filename}. "
        f"Pendentes de OMDB para próxima execução: {len(state.get('pendentes_omdb', []))}. "
        f"Requisições hoje — TMDB: {state['tmdb_requests_today']}, "
        f"OMDB: {state['omdb_requests_today']}/{OMDB_DAILY_LIMIT}."
    )
    log.info(summary)
    print(summary)


if __name__ == "__main__":
    main()
