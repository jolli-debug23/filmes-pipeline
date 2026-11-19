"""
transform.py — Camada Trusted: valida e trata os dados brutos de filmes
(TMDB + OMDB) e grava em data/trusted/. O que não passa no CONTRATO vai
para data/quarentena/ com o motivo exato registrado.

Uso:
    uv run src/transform.py

Dependências (uv add pandas pandera pyarrow):
    pandas, pandera, pyarrow

Política de qualidade adotada (documentar no relatório):
    - Falhas ESTRUTURAIS (nenhum arquivo em data/raw/, JSON corrompido,
      nada pra processar) são Fail-Stop — o pipeline já está quebrado,
      não adianta seguir. Ver load_raw_records().
    - Falhas DE REGISTRO (um filme individual viola uma regra do
      CONTRATO) vão para Quarentena, não travam o run inteiro: um
      registro ruim não deveria impedir os outros de virarem dado
      confiável. Ver validate_and_split().
    Os dois mecanismos coexistem porque respondem a falhas de natureza
    diferente — isso é o que os torna consistentes, não contraditórios.

Tratamento de ausência ("N/A" e zeros-como-ausência):
    - No TMDB, budget/revenue == 0 é convenção de "desconhecido", não
      "filme sem custo" — viram nulo + coluna indicadora
      (orcamento_desconhecido / receita_desconhecida), em vez de serem
      tratados como zero de verdade.
    - "N/A" do OMDB (imdbRating, Metascore ausentes) vira nulo antes de
      qualquer validação de tipo — coagir "N/A" direto pra float
      quebraria a validação por um motivo errado (formato, não
      ausência).
    - Título ou tmdb_id ausentes são críticos (não dá pra identificar o
      filme) e vão para quarentena, não para um valor default silencioso.

Outliers: propositalmente NÃO removidos. Orçamento, bilheteria e
duração muito altos ou baixos em filmes costumam ser reais (um curta
famoso, um blockbuster), não erro de coleta — descartar isso jogaria
fora sinal genuíno para a pergunta de premiação. Só é tratado como
inválido o que é logicamente impossível (valor negativo, nota fora da
escala 0-10), nunca o que é raro mas plausível.

Idempotência e reprodutibilidade:
    - Lê TODOS os arquivos de data/raw/*.json, sempre — não há estado
      externo dependente de execuções anteriores deste script.
    - data/trusted/ e data/quarentena/ são sempre REESCRITOS por
      inteiro (nunca incrementados), com as linhas ordenadas por
      tmdb_id: apagar as pastas e rodar de novo dá o mesmo resultado
      byte a byte, enquanto data/raw/ não mudar.
    - Escrita atômica (.tmp + rename) evita arquivo parcial se o
      processo cair no meio.
"""

from __future__ import annotations

import json
import logging
import re
import sys
from pathlib import Path

import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema

sys.path.insert(0, str(Path(__file__).resolve().parent))
from parametros import DATA_FINAL, DATA_INICIAL, MIN_VOTOS  # noqa: E402

ROOT_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT_DIR / "data" / "raw"
TRUSTED_DIR = ROOT_DIR / "data" / "trusted"
QUARANTINE_DIR = ROOT_DIR / "data" / "quarentena"
LOG_DIR = ROOT_DIR / "logs"

for d in (TRUSTED_DIR, QUARANTINE_DIR, LOG_DIR):
    d.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "transform.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("transform")


# --------------------------------------------------------------------------
# Helpers de limpeza de valor
# --------------------------------------------------------------------------

def na_if_placeholder(value):
    """Converte os placeholders de 'ausente' ('N/A', string vazia, None)
    num None de verdade, pra não virarem valor de tipo errado."""
    if value is None:
        return None
    if isinstance(value, str) and value.strip().upper() in ("", "N/A"):
        return None
    return value


def to_float_or_none(value) -> float | None:
    value = na_if_placeholder(value)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


AWARD_WIN_PATTERN = re.compile(r"\b(win|wins|won|winner|winners)\b", re.IGNORECASE)
AWARD_NOMINATION_PATTERN = re.compile(
    r"\b(nominated|nomination|nominations)\b", re.IGNORECASE
)
# Oscar: o texto do OMDB começa com "Won N Oscar(s)" ou "Nominated for N
# Oscar(s)" quando existe. Quem venceu também foi indicado.
OSCAR_WIN_PATTERN = re.compile(r"\bWon\s+(?:\d+\s+)?Oscars?\b", re.IGNORECASE)
OSCAR_NOMINATION_PATTERN = re.compile(
    r"\bNominated\s+for\s+(?:\d+\s+)?Oscars?\b", re.IGNORECASE
)
COUNT_WINS_PATTERN = re.compile(r"(\d+)\s+wins?\b", re.IGNORECASE)
COUNT_NOMINATIONS_PATTERN = re.compile(r"(\d+)\s+nominations?\b", re.IGNORECASE)


def parse_awards(raw: str | None) -> dict[str, bool | int | None]:
    """Separa o texto Awards do OMDB em colunas.

    Regras (decisão da equipe, registrada no relatório):
    - Texto ausente ou "N/A": o OMDB não traz informação de prêmios.
      awards_sem_informacao = True e as colunas gerais ficam nulas.
    - teve_indicacao, teve_vitoria, teve_reconhecimento: qualquer prêmio
      ou indicação (de qualquer festival). Quase todo filme que tem texto
      tem alguma coisa, então estas colunas quase não variam entre os
      filmes com informação. Servem como controle, não como alvo.
    - n_vitorias e n_indicacoes: o número que o OMDB informa
      ("58 wins & 86 nominations total"). Nulo quando não há texto.
    - teve_oscar_vitoria e teve_oscar: Oscar. Sem texto, o valor é False,
      porque uma indicação ao Oscar sempre aparece no texto do OMDB. É
      suposição declarada e foi conferida com filmes conhecidos.
    """
    raw = na_if_placeholder(raw)
    if raw is None:
        return {
            "teve_indicacao": None,
            "teve_vitoria": None,
            "teve_reconhecimento": None,
            "awards_sem_informacao": True,
            "n_vitorias": None,
            "n_indicacoes": None,
            "teve_oscar_vitoria": False,
            "teve_oscar": False,
        }

    teve_indicacao = bool(AWARD_NOMINATION_PATTERN.search(raw))
    teve_vitoria = bool(AWARD_WIN_PATTERN.search(raw))

    vitorias = sum(int(n) for n in COUNT_WINS_PATTERN.findall(raw))
    if vitorias == 0:
        m = re.match(r"\s*Won\s+(\d+)", raw, re.IGNORECASE)
        vitorias = int(m.group(1)) if m else 0
    indicacoes = sum(int(n) for n in COUNT_NOMINATIONS_PATTERN.findall(raw))
    if indicacoes == 0:
        m = re.match(r"\s*Nominated\s+for\s+(\d+)", raw, re.IGNORECASE)
        indicacoes = int(m.group(1)) if m else 0

    oscar_vitoria = bool(OSCAR_WIN_PATTERN.search(raw))
    oscar = oscar_vitoria or bool(OSCAR_NOMINATION_PATTERN.search(raw))

    return {
        "teve_indicacao": teve_indicacao,
        "teve_vitoria": teve_vitoria,
        "teve_reconhecimento": teve_indicacao or teve_vitoria,
        "awards_sem_informacao": False,
        "n_vitorias": vitorias,
        "n_indicacoes": indicacoes,
        "teve_oscar_vitoria": oscar_vitoria,
        "teve_oscar": oscar,
    }


# --------------------------------------------------------------------------
# Leitura e achatamento do bruto
# --------------------------------------------------------------------------

def load_raw_records() -> list[dict]:
    files = sorted(
        f for f in RAW_DIR.glob("dados-tmdb_omdb-*.json") if not f.name.endswith(".meta.json")
    )
    if not files:
        log.critical("Nenhum arquivo dados-tmdb_omdb-*.json em %s — rode o ingest.py primeiro.", RAW_DIR)
        sys.exit(1)  # Fail-Stop: falha estrutural, não há bruto pra tratar

    records: list[dict] = []
    for f in files:
        try:
            records.extend(json.loads(f.read_text(encoding="utf-8")))
        except json.JSONDecodeError as exc:
            log.critical("Arquivo bruto corrompido: %s (%s)", f, exc)
            sys.exit(1)  # Fail-Stop: o bruto nem pode ser lido
    return records


def flatten_record(record: dict) -> dict:
    details = record.get("tmdb_details") or {}
    omdb = record.get("omdb_data") or {}
    award_flags = parse_awards(omdb.get("Awards"))

    genres = ";".join(g["name"] for g in details.get("genres") or [])
    countries = details.get("production_countries") or []
    country = countries[0]["iso_3166_1"] if countries else None

    crew = (details.get("credits") or {}).get("crew") or []
    directors = ";".join(p["name"] for p in crew if p.get("job") == "Director")

    cast = (details.get("credits") or {}).get("cast") or []
    top_cast = ";".join(p["name"] for p in sorted(cast, key=lambda p: p.get("order", 999))[:3])

    budget = details.get("budget") or 0
    revenue = details.get("revenue") or 0
    vote_count = details.get("vote_count") or 0
    # Sem nenhum voto, o TMDB mostra vote_average = 0.0 — isso é "ainda
    # sem nota", não "nota mínima". Mesma armadilha do orçamento/receita
    # zerados, tratada da mesma forma: vira nulo, não um zero real.
    vote_average = details.get("vote_average") if vote_count > 0 else None

    return {
        "tmdb_id": details.get("id") or record.get("tmdb_id"),
        "titulo": na_if_placeholder(details.get("title")),
        "data_lancamento": na_if_placeholder(details.get("release_date")),
        "status": na_if_placeholder(details.get("status")),
        "generos": genres or None,
        "duracao_min": details.get("runtime"),
        "orcamento": budget if budget > 0 else None,
        "orcamento_desconhecido": budget == 0,
        "receita": revenue if revenue > 0 else None,
        "receita_desconhecida": revenue == 0,
        "idioma_original": na_if_placeholder(details.get("original_language")),
        "pais_producao": country,
        "diretor": directors or None,
        "elenco_principal": top_cast or None,
        "tem_franquia": details.get("belongs_to_collection") is not None,
        "popularidade_tmdb": details.get("popularity"),
        "nota_tmdb": vote_average,
        "n_votos_tmdb": vote_count,
        "imdb_id": na_if_placeholder(details.get("imdb_id")),
        "tem_dado_omdb": bool(omdb),
        "nota_imdb": to_float_or_none(omdb.get("imdbRating")),
        "metascore": to_float_or_none(omdb.get("Metascore")),
        "premios_texto": na_if_placeholder(omdb.get("Awards")),
        **award_flags,
    }


ORIGEM_NOVA = "universo_por_mes"
ORIGEM_LEGADO = "legado_popularidade"


def build_dataframe(records: list[dict]) -> pd.DataFrame:
    """Uma linha por registro bruto, mais a origem da coleta.

    O ingest.py novo marca cada filme que busca com origem_coleta =
    "universo_por_mes". Os filmes de coletas anteriores, feitas pelos mais
    populares, não têm a marca e ficam como "legado_popularidade". Um filme
    é "universo_por_mes" se qualquer registro dele tiver a marca. Isso
    permite medir se a origem da coleta muda o resultado."""
    marcados = {r.get("tmdb_id") for r in records if r.get("origem_coleta") == ORIGEM_NOVA}
    df = pd.DataFrame(flatten_record(r) for r in records)
    df["origem_coleta"] = [
        ORIGEM_NOVA if i in marcados else ORIGEM_LEGADO for i in df["tmdb_id"]
    ]
    return df


def aplicar_universo(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Mantém só os filmes do universo definido em src/parametros.py: lançados
    entre DATA_INICIAL e DATA_FINAL e com pelo menos MIN_VOTOS votos.

    Os filmes de fora continuam no data/raw/, só não entram na trusted. Isso
    deixa usar um raw coletado antes de o universo existir, sem apagar nada,
    e garante que o mesmo critério vale para todo filme, venha de onde vier.
    Não é quarentena: o dado destes filmes não está errado, só está fora do
    recorte da pergunta."""
    data = pd.to_datetime(df["data_lancamento"], errors="coerce")
    sem_data = data.isna()
    fora_do_periodo = ~sem_data & ((data < DATA_INICIAL) | (data > DATA_FINAL))
    votos = pd.to_numeric(df["n_votos_tmdb"], errors="coerce").fillna(0)
    poucos_votos = ~sem_data & ~fora_do_periodo & (votos < MIN_VOTOS)
    dentro = ~(sem_data | fora_do_periodo | poucos_votos)
    resumo = {
        "antes": len(df),
        "sem_data": int(sem_data.sum()),
        "fora_do_periodo": int(fora_do_periodo.sum()),
        "poucos_votos": int(poucos_votos.sum()),
        "dentro": int(dentro.sum()),
    }
    log.info(
        "Universo (%s a %s, no mínimo %d votos): %d de %d filmes dentro. Fora: %d sem data, "
        "%d fora do período, %d com poucos votos.",
        DATA_INICIAL, DATA_FINAL, MIN_VOTOS, resumo["dentro"], resumo["antes"],
        resumo["sem_data"], resumo["fora_do_periodo"], resumo["poucos_votos"],
    )
    return df[dentro].copy(), resumo


# --------------------------------------------------------------------------
# Duplicatas — removidas antes de qualquer outra estatística/validação
# --------------------------------------------------------------------------

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Quando duas coletas trazem o mesmo tmdb_id, fica a cópia mais
    COMPLETA (mais colunas preenchidas), não simplesmente a mais
    recente — uma coleta que bateu no limite diário do OMDB e ficou
    sem enriquecimento não deveria apagar uma versão já enriquecida do
    mesmo filme coletada antes."""
    n_before = len(df)
    completude = df.notna().sum(axis=1)
    df = (
        df.assign(_completude=completude)
        .sort_values("_completude", ascending=False)
        .drop_duplicates(subset=["tmdb_id"], keep="first")
        .drop(columns="_completude")
    )
    n_after = len(df)
    log.info(
        "Duplicatas: %d registros antes, %d depois (%d removidos, mantendo sempre a cópia mais completa).",
        n_before, n_after, n_before - n_after,
    )
    return df


# --------------------------------------------------------------------------
# CONTRATO
# --------------------------------------------------------------------------

_num_ok = lambda s: s.isna() | (s >= 0)  # noqa: E731 — checks de não-negatividade, ignorando nulo

CONTRACT = DataFrameSchema(
    {
        "tmdb_id": Column(int, nullable=False),
        "titulo": Column(str, nullable=False),
        "data_lancamento": Column(str, nullable=True),
        "status": Column(
            str,
            Check.isin(
                ["Released", "Post Production", "In Production", "Planned", "Rumored", "Canceled"],
                error="status_fora_do_dominio_conhecido",
            ),
            nullable=True,
        ),
        "generos": Column(str, nullable=True),
        "duracao_min": Column(float, Check(_num_ok, error="duracao_negativa"), nullable=True),
        "orcamento": Column(float, Check(_num_ok, error="orcamento_negativo"), nullable=True),
        "orcamento_desconhecido": Column(bool, nullable=False),
        "receita": Column(float, Check(_num_ok, error="receita_negativa"), nullable=True),
        "receita_desconhecida": Column(bool, nullable=False),
        "idioma_original": Column(str, nullable=True),
        "pais_producao": Column(str, nullable=True),
        "diretor": Column(str, nullable=True),
        "elenco_principal": Column(str, nullable=True),
        "tem_franquia": Column(bool, nullable=False),
        "popularidade_tmdb": Column(float, Check(_num_ok, error="popularidade_negativa"), nullable=True),
        "nota_tmdb": Column(float, Check.in_range(0, 10, error="nota_tmdb_fora_do_intervalo"), nullable=True),
        "n_votos_tmdb": Column(float, Check(_num_ok, error="votos_negativos"), nullable=True),
        "imdb_id": Column(str, nullable=True),
        "tem_dado_omdb": Column(bool, nullable=False),
        "nota_imdb": Column(float, Check.in_range(0, 10, error="nota_imdb_fora_do_intervalo"), nullable=True),
        "metascore": Column(float, Check.in_range(0, 100, error="metascore_fora_do_intervalo"), nullable=True),
        "premios_texto": Column(str, nullable=True),
        # "boolean" (com B minúsculo) é o tipo que aceita nulo. Com bool, o
        # coerce=True trocava o nulo de "sem informação" por False sem avisar.
        "teve_indicacao": Column("boolean", nullable=True),
        "teve_vitoria": Column("boolean", nullable=True),
        "teve_reconhecimento": Column("boolean", nullable=True),
        "awards_sem_informacao": Column(bool, nullable=False),
        "n_vitorias": Column(float, Check(_num_ok, error="vitorias_negativas"), nullable=True),
        "n_indicacoes": Column(float, Check(_num_ok, error="indicacoes_negativas"), nullable=True),
        "teve_oscar_vitoria": Column(bool, nullable=False),
        "teve_oscar": Column(bool, nullable=False),
        "origem_coleta": Column(
            str,
            Check.isin([ORIGEM_NOVA, ORIGEM_LEGADO], error="origem_coleta_desconhecida"),
            nullable=False,
        ),
    },
    checks=[
        # Regra de negócio de tabela, envolvendo duas colunas: um filme
        # já LANÇADO não pode ter duração zerada ou ausente.
        Check(
            lambda df: ~((df["status"] == "Released") & (df["duracao_min"].fillna(0) <= 0)),
            error="filme_lancado_sem_duracao",
        ),
    ],
    strict=True,
    coerce=True,
)


def validate_and_split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Valida linha a linha: quem passa vai para Trusted, quem falha vai
    para Quarentena com o motivo exato do pandera. Linha a linha (não em
    lote) para que o motivo registrado seja sempre específico daquela
    linha — a uniqueness de tmdb_id já foi garantida por
    remove_duplicates(), então não depende de validar em lote."""
    good_rows: list[pd.Series] = []
    bad_rows: list[dict] = []

    for _, row in df.iterrows():
        row_df = row.to_frame().T
        try:
            validated_row = CONTRACT.validate(row_df, lazy=True)
            good_rows.append(validated_row.iloc[0])
        except (pa.errors.SchemaErrors, pa.errors.SchemaError, TypeError, ValueError) as exc:
            reasons = (
                ";".join(sorted(set(exc.failure_cases["check"].astype(str))))
                if hasattr(exc, "failure_cases")
                else str(exc)
            )
            bad_row = row.to_dict()
            bad_row["motivo_quarentena"] = reasons
            bad_rows.append(bad_row)

    good_df = (
        pd.DataFrame(good_rows).reset_index(drop=True)
        if good_rows
        else pd.DataFrame(columns=df.columns)
    )
    quarantine_df = (
        pd.DataFrame(bad_rows).reset_index(drop=True)
        if bad_rows
        else pd.DataFrame(columns=list(df.columns) + ["motivo_quarentena"])
    )
    return good_df, quarantine_df


# --------------------------------------------------------------------------
# Escrita atômica e determinística
# --------------------------------------------------------------------------

def write_outputs(trusted_df: pd.DataFrame, quarantine_df: pd.DataFrame) -> None:
    trusted_df = trusted_df.sort_values("tmdb_id").reset_index(drop=True)
    if len(quarantine_df) and "tmdb_id" in quarantine_df.columns:
        quarantine_df = quarantine_df.sort_values("tmdb_id").reset_index(drop=True)

    trusted_final = TRUSTED_DIR / "filmes.parquet"
    trusted_tmp = trusted_final.with_suffix(".parquet.tmp")
    trusted_df.to_parquet(trusted_tmp, index=False)
    trusted_tmp.replace(trusted_final)

    quarantine_final = QUARANTINE_DIR / "filmes_quarentena.csv"
    quarantine_tmp = quarantine_final.with_suffix(".csv.tmp")
    quarantine_df.to_csv(quarantine_tmp, index=False)
    quarantine_tmp.replace(quarantine_final)


# --------------------------------------------------------------------------
# Execução principal
# --------------------------------------------------------------------------

def main() -> None:
    records = load_raw_records()
    log.info("%d registros brutos lidos de data/raw/.", len(records))

    df = build_dataframe(records)
    df = remove_duplicates(df)
    n_unicos = len(df)
    df, universo = aplicar_universo(df)

    trusted_df, quarantine_df = validate_and_split(df)
    write_outputs(trusted_df, quarantine_df)

    # Resumo por motivo, exigido pelo desafio da aula-5
    print(f"Filmes diferentes no raw: {n_unicos}")
    print(
        f"Fora do universo ({DATA_INICIAL} a {DATA_FINAL}, no mínimo {MIN_VOTOS} votos): "
        f"{universo['antes'] - universo['dentro']} "
        f"(sem data {universo['sem_data']}, fora do período {universo['fora_do_periodo']}, "
        f"poucos votos {universo['poucos_votos']}). Continuam no raw."
    )
    print(f"Total processado: {len(df)}")
    print(f"Foram para data/trusted/filmes.parquet: {len(trusted_df)}")
    print(f"Foram para data/quarentena/filmes_quarentena.csv: {len(quarantine_df)}")
    if len(quarantine_df):
        print("Motivos da quarentena:")
        print(quarantine_df["motivo_quarentena"].value_counts().to_string())

    if len(trusted_df):
        print("Origem da coleta na trusted:")
        print(trusted_df["origem_coleta"].value_counts().to_string())

    log.info(
        "Concluído: %d em trusted, %d em quarentena.",
        len(trusted_df), len(quarantine_df),
    )


if __name__ == "__main__":
    main()
