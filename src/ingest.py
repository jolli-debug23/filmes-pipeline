"""
ingest.py: coleta incremental de filmes via TMDB + OMDB, por mês de lançamento.

Uso:
    uv run python src/ingest.py

Requisitos de ambiente (.env na raiz do projeto):
    TMDB_API_KEY=xxxx
    OMDB_API_KEY=xxxx
    MAX_MOVIES_PER_RUN=100       # opcional, filmes NOVOS por execução
    TMDB_REQUESTS_PER_SEC=4      # opcional, limite de requisições por segundo ao TMDB
    OMDB_MIN_INTERVAL_SEC=1.0    # opcional, intervalo mínimo entre chamadas ao OMDB
    OMDB_DAILY_LIMIT=900         # opcional, margem sob o limite de 1000 por dia

O universo (período, mínimo de votos, cota por mês) está em src/parametros.py.

Como a coleta funciona:
    1. O período do universo é dividido em meses. Cada mês tem uma cota de
       FILMES_POR_MES filmes e o seu próprio cursor de página no TMDB.
    2. A coleta anda pelos meses em rodadas, pegando no máximo um filme novo
       por visita. Assim, parar no meio não deixa a amostra concentrada nos
       primeiros meses. Um ponteiro no estado faz a próxima execução
       continuar do mês onde esta parou.
    3. Quem já está em data/raw/ conta para a cota do seu mês e nunca é
       buscado de novo. Isso permite completar um raw que já existe, sem
       apagar nada: a coleta nova só acrescenta arquivos.
    4. Cada filme novo é gravado com origem_coleta = "universo_por_mes". Os
       filmes de coletas anteriores, feitas por popularidade, ficam como
       "legado_popularidade" no transform.py. Isso permite medir se a origem
       da coleta muda o resultado.

Limites das APIs:
    - Por segundo: um RateLimiter espera o intervalo mínimo entre chamadas.
    - Por dia (OMDB gratuito, 1000): o script para de chamar o OMDB em
      OMDB_DAILY_LIMIT. O filme é gravado só com os dados do TMDB e entra na
      lista de pendentes. No começo da execução seguinte, o script busca o
      OMDB desses filmes antes de coletar filmes novos.
    - Erros diferentes pedem reações diferentes (função safe_get): 401 para
      tudo, 429 espera e tenta de novo, 5xx tenta de novo com espera
      crescente.

Idempotência e reprodutibilidade:
    - O data/raw/ é a fonte da verdade. A cada execução o script lê o que já
      existe lá: filme que já está no raw não é buscado de novo, e conta para
      a cota do seu mês. O state/ingest_state.json guarda só o que o raw não
      sabe (chamadas feitas hoje, filmes esperando o OMDB, o ponteiro de
      mês). Se o estado for apagado, nada se perde e nada se repete.
    - Rodar duas vezes seguidas não duplica dado. Se o script cair ou for
      interrompido no meio, o que já foi coletado é gravado antes de sair.
    - Cada execução grava um JSON com data e hora em data/raw/ e um
      .meta.json ao lado, com a fonte, o universo, as contagens e os limites.
      Se dois arquivos caírem no mesmo segundo, o segundo ganha um sufixo, e
      nenhum é sobrescrito.
    - Todo arquivo é gravado num .tmp e depois renomeado.
"""

from __future__ import annotations

import calendar
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

sys.path.insert(0, str(Path(__file__).resolve().parent))
from parametros import (  # noqa: E402
    DATA_FINAL,
    DATA_INICIAL,
    FILMES_POR_MES,
    MIN_VOTOS,
    ORDEM_TMDB,
)

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
MAX_MOVIES_PER_RUN = int(os.getenv("MAX_MOVIES_PER_RUN", "100"))
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
ORIGEM_NOVA = "universo_por_mes"


def build_session() -> requests.Session:
    # Estes retries cobrem só falha de conexão (timeout, DNS). Os erros HTTP
    # (401, 429, 5xx) são tratados em safe_get(), um por um.
    session = requests.Session()
    retry = Retry(total=2, backoff_factor=1.0, allowed_methods=["GET"])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    return session


SESSION = build_session()


class CredentialError(RuntimeError):
    """401: a chave está errada ou expirou. Tentar de novo não adianta."""


class TransientAPIError(RuntimeError):
    """5xx ou 429 que não passou depois de todas as tentativas."""


def safe_get(url: str, params: dict, api_name: str, max_retries: int = 4) -> requests.Response:
    """GET com uma reação diferente para cada classe de erro HTTP.

    - 401: para na hora. A chave está errada, e cada nova tentativa gastaria
      chamada do orçamento diário.
    - 429: espera o tempo do cabeçalho Retry-After (ou um tempo crescente) e
      tenta de novo.
    - 5xx: falha do servidor, tenta de novo com espera crescente.
    - Outros erros HTTP (404, 400): viram exceção normal, sem repetir.
    """
    for attempt in range(1, max_retries + 1):
        resp = SESSION.get(url, params=params, timeout=15)

        if resp.status_code == 401:
            raise CredentialError(
                f"{api_name}: credencial recusada (401), verifique a chave no .env"
            )

        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 2**attempt))
            log.warning(
                "%s: limite de requisições (429), aguardando %ss (tentativa %d/%d)",
                api_name, wait, attempt, max_retries,
            )
            time.sleep(wait)
            continue

        if 500 <= resp.status_code < 600:
            wait = 2**attempt
            log.warning(
                "%s: falha do servidor (%d), nova tentativa em %ss (tentativa %d/%d)",
                api_name, resp.status_code, wait, attempt, max_retries,
            )
            time.sleep(wait)
            continue

        resp.raise_for_status()
        return resp

    raise TransientAPIError(f"{api_name}: {max_retries} tentativas sem sucesso (429 ou 5xx)")


# --------------------------------------------------------------------------
# Limite por segundo
# --------------------------------------------------------------------------

class RateLimiter:
    """Garante um intervalo mínimo entre chamadas seguidas."""

    def __init__(self, min_interval_sec: float):
        self.min_interval = min_interval_sec
        self._last_call = 0.0

    def wait(self) -> None:
        restante = self.min_interval - (time.monotonic() - self._last_call)
        if restante > 0:
            time.sleep(restante)
        self._last_call = time.monotonic()


tmdb_limiter = RateLimiter(1.0 / TMDB_REQUESTS_PER_SEC)
omdb_limiter = RateLimiter(OMDB_MIN_INTERVAL_SEC)


# --------------------------------------------------------------------------
# Meses do universo
# --------------------------------------------------------------------------

def lista_de_meses() -> list[str]:
    ano, mes = int(DATA_INICIAL[:4]), int(DATA_INICIAL[5:7])
    ano_fim, mes_fim = int(DATA_FINAL[:4]), int(DATA_FINAL[5:7])
    meses = []
    while (ano, mes) <= (ano_fim, mes_fim):
        meses.append(f"{ano:04d}-{mes:02d}")
        mes += 1
        if mes == 13:
            ano, mes = ano + 1, 1
    return meses


def limites_do_mes(mes: str) -> tuple[str, str]:
    ano, n = int(mes[:4]), int(mes[5:7])
    primeiro = f"{mes}-01"
    ultimo = f"{mes}-{calendar.monthrange(ano, n)[1]:02d}"
    return max(primeiro, DATA_INICIAL), min(ultimo, DATA_FINAL)


def universo_atual() -> dict:
    return {
        "data_inicial": DATA_INICIAL,
        "data_final": DATA_FINAL,
        "min_votos": MIN_VOTOS,
        "ordem": ORDEM_TMDB,
        "filmes_por_mes": FILMES_POR_MES,
    }


# --------------------------------------------------------------------------
# O que já existe em data/raw/ (a fonte da verdade)
# --------------------------------------------------------------------------

def arquivos_raw() -> list[Path]:
    return sorted(
        f for f in RAW_DIR.glob("dados-tmdb_omdb-*.json") if not f.name.endswith(".meta.json")
    )


def ler_raw_existente() -> tuple[set[int], dict[str, int]]:
    """Devolve os tmdb_id já coletados e, por mês do universo, quantos filmes
    diferentes já existem. Só lê, nunca altera o raw.

    Um filme coletado por esta versão carrega o mês para o qual foi buscado
    (mes_universo), e esse mês vale. Os filmes de coletas anteriores não têm
    essa marca, então vale o mês da data de lançamento, se estiver dentro do
    universo (período e mínimo de votos)."""
    ids: set[int] = set()
    mes_do_filme: dict[int, str] = {}
    for arquivo in arquivos_raw():
        try:
            registros = json.loads(arquivo.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            log.error("Arquivo bruto ilegível, ignorado na contagem: %s (%s)", arquivo.name, exc)
            continue
        for r in registros:
            movie_id = r.get("tmdb_id")
            if movie_id is None:
                continue
            ids.add(movie_id)
            if r.get("mes_universo"):
                mes_do_filme[movie_id] = r["mes_universo"]
                continue
            detalhes = r.get("tmdb_details") or {}
            data = detalhes.get("release_date") or ""
            if (
                movie_id not in mes_do_filme
                and len(data) >= 10
                and DATA_INICIAL <= data <= DATA_FINAL
                and (detalhes.get("vote_count") or 0) >= MIN_VOTOS
            ):
                mes_do_filme[movie_id] = data[:7]
    contagem: dict[str, int] = {}
    for mes in mes_do_filme.values():
        contagem[mes] = contagem.get(mes, 0) + 1
    return ids, contagem


# --------------------------------------------------------------------------
# Estado persistido (só o que o raw não sabe)
# --------------------------------------------------------------------------

def default_state() -> dict:
    return {
        "date": date.today().isoformat(),
        "universo": universo_atual(),
        "ponteiro": 0,
        "tmdb_requests_today": 0,
        "omdb_requests_today": 0,
        "pendentes_omdb": [],
    }


def load_state() -> dict:
    if not STATE_PATH.exists():
        return default_state()

    state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    for chave in ("collected_tmdb_ids", "cursores", "cursores_por_ano", "next_discover_page"):
        state.pop(chave, None)  # de versões anteriores: o raw é quem sabe isso agora
    for chave, valor in default_state().items():
        state.setdefault(chave, valor)

    if state.get("universo") != universo_atual():
        log.info("O universo mudou desde a última execução. O ponteiro de mês volta ao início.")
        state["universo"] = universo_atual()
        state["ponteiro"] = 0

    if state.get("date") != date.today().isoformat():
        log.info("Novo dia: contadores de requisição do dia zerados.")
        state["date"] = date.today().isoformat()
        state["tmdb_requests_today"] = 0
        state["omdb_requests_today"] = 0
    return state


def atomic_write_json(path: Path, data) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def save_state(state: dict) -> None:
    atomic_write_json(STATE_PATH, state)


def caminhos_da_execucao(carimbo: str) -> tuple[Path, Path]:
    """Nome com data e hora. Se já existir (duas execuções no mesmo segundo),
    acrescenta um sufixo em vez de sobrescrever."""
    sufixo = ""
    k = 1
    while (RAW_DIR / f"dados-tmdb_omdb-{carimbo}{sufixo}.json").exists():
        k += 1
        sufixo = f"-{k}"
    base = f"dados-tmdb_omdb-{carimbo}{sufixo}"
    return RAW_DIR / f"{base}.json", RAW_DIR / f"{base}.meta.json"


# --------------------------------------------------------------------------
# Chamadas às APIs
# --------------------------------------------------------------------------

def fetch_tmdb_discover_page(mes: str, page: int) -> list[dict]:
    inicio, fim = limites_do_mes(mes)
    tmdb_limiter.wait()
    resp = safe_get(
        f"{TMDB_BASE}/discover/movie",
        params={
            "api_key": TMDB_API_KEY,
            "language": "pt-BR",
            "sort_by": ORDEM_TMDB,
            "primary_release_date.gte": inicio,
            "primary_release_date.lte": fim,
            "vote_count.gte": MIN_VOTOS,
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


# --------------------------------------------------------------------------
# Backfill do OMDB
# --------------------------------------------------------------------------

def backfill_omdb(state: dict, recuperados: list[dict]) -> None:
    """Busca o OMDB dos filmes que foram gravados sem ele porque o limite
    diário tinha acabado. Roda antes da coleta nova, enquanto houver
    orçamento do dia. O filme recuperado é gravado de novo, completo, e o
    transform.py fica com a cópia mais completa. Os registros vão sendo
    acrescentados em `recuperados`, para não se perderem se o script parar."""
    pendentes = list(state.get("pendentes_omdb", []))
    if not pendentes:
        return

    log.info("%d filme(s) esperando o OMDB. Tentando recuperar.", len(pendentes))
    ainda_pendentes: list[int] = []

    try:
        for movie_id in pendentes:
            if state["omdb_requests_today"] >= OMDB_DAILY_LIMIT:
                ainda_pendentes.append(movie_id)
                continue
            try:
                detalhes = fetch_tmdb_movie_details(movie_id)
                state["tmdb_requests_today"] += 1
                imdb_id = detalhes.get("imdb_id")
                omdb = fetch_omdb_by_imdb_id(imdb_id) if imdb_id else None
                if imdb_id:
                    state["omdb_requests_today"] += 1
                if omdb is not None:
                    recuperados.append(
                        {
                            "tmdb_id": movie_id,
                            "tmdb_details": detalhes,
                            "omdb_data": omdb,
                            "omdb_skipped_daily_budget": False,
                        }
                    )
            except CredentialError:
                ainda_pendentes.append(movie_id)
                raise
            except (requests.RequestException, TransientAPIError) as exc:
                log.error("Falha ao refazer o OMDB do filme %s: %s", movie_id, exc)
                ainda_pendentes.append(movie_id)
    finally:
        feitos = {r["tmdb_id"] for r in recuperados}
        restantes = [i for i in pendentes if i not in feitos and i not in ainda_pendentes]
        state["pendentes_omdb"] = ainda_pendentes + restantes
        save_state(state)

    log.info(
        "Backfill do OMDB: %d recuperado(s), %d ainda esperando.",
        len(recuperados), len(state["pendentes_omdb"]),
    )


# --------------------------------------------------------------------------
# Coleta por mês
# --------------------------------------------------------------------------

def proximo_candidato(mes, cursor, paginas, ja_coletados, falhos, state):
    """Procura o próximo filme novo do mês. Devolve (status, tmdb_id).

    Status: "ok", "esgotado" (o TMDB não tem mais filmes neste mês) ou "falha"
    (os filmes novos que sobraram falharam nesta execução, então o mês é
    pulado e eles são tentados de novo na próxima execução).
    As páginas já buscadas ficam em `paginas`, para não chamar o TMDB duas
    vezes pela mesma página dentro da execução.
    """
    while True:
        if cursor["page"] > TMDB_MAX_PAGE:
            return "esgotado", None
        chave = (mes, cursor["page"])
        if chave not in paginas:
            paginas[chave] = fetch_tmdb_discover_page(mes, cursor["page"])
            state["tmdb_requests_today"] += 1
        pagina = paginas[chave]
        if not pagina:
            return "esgotado", None
        novos = [f["id"] for f in pagina if f["id"] not in ja_coletados]
        livres = [i for i in novos if i not in falhos]
        if livres:
            return "ok", livres[0]
        if novos:
            return "falha", None
        cursor["page"] += 1


def coletar_filmes_novos(
    state: dict,
    ja_coletados: set[int],
    contagem: dict[str, int],
    novos_registros: list[dict],
    erros: list[dict],
) -> bool:
    """Coleta filmes novos, um por visita a cada mês, em rodadas. Os registros
    são acrescentados em `novos_registros` (a lista é do chamador, para que o
    que já foi coletado possa ser gravado mesmo se o script parar no meio).
    Devolve True se o limite diário do OMDB foi atingido nesta execução."""
    meses = lista_de_meses()
    total = len(meses)
    cursores = {m: {"page": 1, "coletados": contagem.get(m, 0)} for m in meses}
    esgotados: set[str] = set()
    meses_com_erro: set[str] = set()
    falhos: set[int] = set()
    paginas: dict[tuple[str, int], list[dict]] = {}
    orcamento_omdb_acabou = False
    n_novos = 0

    def ativo(mes: str) -> bool:
        return (
            cursores[mes]["coletados"] < FILMES_POR_MES
            and mes not in esgotados
            and mes not in meses_com_erro
        )

    ponteiro = state.get("ponteiro", 0) % total

    while n_novos < MAX_MOVIES_PER_RUN and any(ativo(m) for m in meses):
        inicio = ponteiro
        for passo in range(total):
            if n_novos >= MAX_MOVIES_PER_RUN:
                break
            indice = (inicio + passo) % total
            mes = meses[indice]
            if not ativo(mes):
                continue
            ponteiro = (indice + 1) % total

            try:
                status, movie_id = proximo_candidato(
                    mes, cursores[mes], paginas, ja_coletados, falhos, state
                )
            except (requests.RequestException, TransientAPIError) as exc:
                log.error("Falha ao listar o mês %s: %s", mes, exc)
                meses_com_erro.add(mes)
                continue

            if status == "esgotado":
                esgotados.add(mes)
                log.info("Mês %s esgotado (%d filmes do universo neste mês).", mes, cursores[mes]["coletados"])
                continue
            if status == "falha":
                meses_com_erro.add(mes)
                continue

            try:
                detalhes = fetch_tmdb_movie_details(movie_id)
                state["tmdb_requests_today"] += 1

                imdb_id = detalhes.get("imdb_id")
                omdb = None
                sem_orcamento = False
                if imdb_id and state["omdb_requests_today"] < OMDB_DAILY_LIMIT:
                    omdb = fetch_omdb_by_imdb_id(imdb_id)
                    state["omdb_requests_today"] += 1
                elif imdb_id:
                    sem_orcamento = True
                    if not orcamento_omdb_acabou:
                        orcamento_omdb_acabou = True
                        log.warning(
                            "Limite diário do OMDB (%d) atingido. Os próximos filmes ficam sem "
                            "OMDB até o backfill da próxima execução.",
                            OMDB_DAILY_LIMIT,
                        )

                novos_registros.append(
                    {
                        "tmdb_id": movie_id,
                        "tmdb_details": detalhes,
                        "omdb_data": omdb,
                        "omdb_skipped_daily_budget": sem_orcamento,
                        "origem_coleta": ORIGEM_NOVA,
                        "mes_universo": mes,
                    }
                )
                if sem_orcamento:
                    state.setdefault("pendentes_omdb", []).append(movie_id)

                ja_coletados.add(movie_id)
                cursores[mes]["coletados"] += 1
                n_novos += 1
                state["ponteiro"] = ponteiro
                save_state(state)

            except (requests.RequestException, TransientAPIError) as exc:
                log.error("Falha ao coletar o filme %s: %s", movie_id, exc)
                erros.append({"tmdb_id": movie_id, "error": str(exc)})
                falhos.add(movie_id)

    state["ponteiro"] = ponteiro
    contagem.update({m: c["coletados"] for m, c in cursores.items()})
    return orcamento_omdb_acabou


# --------------------------------------------------------------------------
# Execução principal
# --------------------------------------------------------------------------

def gravar_execucao(registros, erros, state, ja_coletados, contagem, inicio, carimbo, n_recuperados, n_novos, omdb_acabou):
    arquivo_dados, arquivo_meta = caminhos_da_execucao(carimbo)
    meses = lista_de_meses()
    completos = sum(1 for m in meses if contagem.get(m, 0) >= FILMES_POR_MES)
    atomic_write_json(arquivo_dados, registros)
    atomic_write_json(
        arquivo_meta,
        {
            "fonte": {
                "tmdb": f"{TMDB_BASE}/discover/movie + /movie/{{id}}",
                "omdb": OMDB_BASE,
            },
            "universo": universo_atual(),
            "origem_coleta": ORIGEM_NOVA,
            "run_started_at": inicio.isoformat(),
            "run_finished_at": datetime.now().isoformat(),
            "n_movies_collected_this_run": n_novos,
            "n_movies_recovered_backfill_omdb": n_recuperados,
            "n_movies_collected_total": len(ja_coletados),
            "n_filmes_do_universo_contados": sum(contagem.values()),
            "meses_completos": completos,
            "meses_total": len(meses),
            "n_pendentes_omdb_restantes": len(state.get("pendentes_omdb", [])),
            "n_errors": len(erros),
            "errors": erros,
            "omdb_daily_budget_hit": omdb_acabou,
            "tmdb_requests_today": state["tmdb_requests_today"],
            "omdb_requests_today": state["omdb_requests_today"],
            "tmdb_requests_per_sec_limit": TMDB_REQUESTS_PER_SEC,
            "omdb_min_interval_sec": OMDB_MIN_INTERVAL_SEC,
            "omdb_daily_limit": OMDB_DAILY_LIMIT,
            "python_version": sys.version,
            "requests_version": requests.__version__,
        },
    )
    return arquivo_dados, completos, len(meses)


def main() -> None:
    state = load_state()
    ja_coletados, contagem = ler_raw_existente()
    antes = len(ja_coletados)

    inicio = datetime.now()
    carimbo = inicio.strftime("%Y%m%d-%H%M%S")

    recuperados: list[dict] = []
    novos: list[dict] = []
    erros: list[dict] = []
    omdb_acabou = False

    try:
        backfill_omdb(state, recuperados)
        omdb_acabou = coletar_filmes_novos(state, ja_coletados, contagem, novos, erros)
    finally:
        # Mesmo se o script parar no meio (chave recusada, Ctrl+C), o que já
        # foi coletado é gravado antes de sair.
        save_state(state)
        registros = recuperados + novos
        if registros:
            arquivo, completos, n_meses = gravar_execucao(
                registros, erros, state, ja_coletados, contagem, inicio, carimbo,
                len(recuperados), len(novos), omdb_acabou,
            )

    no_universo = sum(contagem.values())
    if not registros:
        meses = lista_de_meses()
        completos = sum(1 for m in meses if contagem.get(m, 0) >= FILMES_POR_MES)
        print(
            f"Nenhum filme novo nesta execução. {completos} de {len(meses)} meses completos, "
            f"{no_universo} filmes do universo no raw."
        )
        return

    resumo = (
        f"Concluído: {len(registros)} filmes gravados nesta execução "
        f"({len(recuperados)} recuperados via backfill do OMDB, {len(novos)} novos). "
        f"Total já coletado: {len(ja_coletados)} (antes: {antes}). Gravado em {arquivo}. "
        f"Meses completos: {completos}/{n_meses}. Filmes do universo no raw: {no_universo}. "
        f"Filmes esperando o OMDB: {len(state.get('pendentes_omdb', []))}. "
        f"Requisições hoje: TMDB {state['tmdb_requests_today']}, "
        f"OMDB {state['omdb_requests_today']}/{OMDB_DAILY_LIMIT}."
    )
    log.info(resumo)
    print(resumo)


if __name__ == "__main__":
    try:
        main()
    except CredentialError as exc:
        log.critical(str(exc))
        sys.exit(1)
