"""
model.py: constrói a camada gold (modelo estrela) a partir de data/trusted/.

Uso:
    uv run python src/model.py
    uv run python src/model.py --teste-falha     (ver "Teste de parada")

O desenho (resumo, o texto completo está no relatório):
    Processo de negócio: Um filme estreia e recebe avaliações e
        reconhecimentos.
    Granularidade da fato_filme: uma linha por filme (um tmdb_id).
    Dimensões: dim_data, dim_origem, dim_perfil, dim_genero.
    Ponte: ponte_filme_genero (um filme tem vários gêneros).
    Fato agregada: fato_oscar_genero_decada (uma linha por
        década e gênero), para a pergunta "qual a proporção de filmes
        indicados ou vencedores do Oscar por gênero e década?".

Como o script funciona:
    1. Lê data/trusted/filmes.parquet numa base DuckDB em memória.
    2. Monta todas as tabelas em memória: dimensoes(), fatos(), agregada().
    3. Confere tudo ANTES de gravar: chaves únicas, chaves estrangeiras
       (cada chave da fato existe na dimensão), dimensão de data sem
       buraco, contagem de linhas e soma de cada medida contra a trusted,
       e a fato agregada contra a fato de onde vem.
    4. Se qualquer conferência falhar, o script mostra a chave ou a
       soma que não bate e para. Nada é gravado.
    5. Se tudo bater, grava um arquivo por tabela em data/gold/.

Idempotência: o gold é recriado por inteiro a cada execução, com as
linhas ordenadas pelas chaves. Apagar data/gold/ e rodar de novo dá o
mesmo resultado. Cada arquivo é gravado num .tmp e renomeado, para nunca
sobrar um arquivo pela metade.

Importante: o gold é conferido contra a trusted, não contra a fonte.
Uma coleta incompleta ou um filme duplicado na trusted passaria com todas
as conferências batendo. Por isso o transform.py da Etapa 1 precisa estar
certo antes deste script.

Teste de parada (--teste-falha): troca o generate_series por range na
dimensão de data. O range não inclui o último dia, então falta uma chave
que a fato usa. O script precisa parar antes de gravar e apontar a chave
que falta. É o teste que o enunciado pede, e a flag só evita editar o
código para fazê-lo.
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from parametros import DATA_CORTE  # noqa: E402

ROOT_DIR = Path(__file__).resolve().parent.parent
TRUSTED_DIR = ROOT_DIR / "data" / "trusted"
GOLD_DIR = ROOT_DIR / "data" / "gold"

# --------------------------------------------------------------------------
# O desenho em listas (é daqui que o script confere e grava)
# --------------------------------------------------------------------------

TABELAS_TRUSTED = {"filmes": TRUSTED_DIR / "filmes.parquet"}

# tabela do gold -> ordenação usada ao gravar (deixa a saída determinística)
TABELAS_GOLD = {
    "dim_data": "data",
    "dim_origem": "origem_sk",
    "dim_perfil": "perfil_sk",
    "dim_genero": "genero_sk",
    "ponte_filme_genero": "tmdb_id, genero_sk",
    "fato_filme": "tmdb_id",
    "fato_oscar_genero_decada": "decada NULLS LAST, genero_sk",
}

# (tabela filha, coluna filha, tabela pai, coluna pai)
CHAVES = [
    ("fato_filme", "data_lancamento", "dim_data", "data"),
    ("fato_filme", "origem_sk", "dim_origem", "origem_sk"),
    ("fato_filme", "perfil_sk", "dim_perfil", "perfil_sk"),
    ("ponte_filme_genero", "tmdb_id", "fato_filme", "tmdb_id"),
    ("ponte_filme_genero", "genero_sk", "dim_genero", "genero_sk"),
    ("fato_oscar_genero_decada", "genero_sk", "dim_genero", "genero_sk"),
]

# (tabela, coluna que não pode repetir)
CHAVES_UNICAS = [
    ("dim_data", "data"),
    ("dim_origem", "origem_sk"),
    ("dim_perfil", "perfil_sk"),
    ("dim_genero", "genero_sk"),
    ("fato_filme", "tmdb_id"),
]

# (nome na fato, expressão na trusted, classe da medida)
#   aditiva: a soma entre filmes faz sentido.
#   nao_aditiva: nota, popularidade. Nunca somar para responder nada;
#       a conta certa é soma dividida pela contagem de filmes COM valor.
# As colunas teve_* e tem_dado_omdb são 0/1 e somam como contagem
# (teve_indicacao, teve_vitoria e teve_reconhecimento ficam nulas quando
# o OMDB não traz o campo Awards). A proporção é soma dividida pela
# contagem de filmes do grupo, nunca média de proporções. n_vitorias e
# n_indicacoes são contagens de prêmios de cada filme e também somam.
MEDIDAS = [
    ("duracao_min", "duracao_min", "aditiva"),
    ("orcamento", "orcamento", "aditiva"),
    ("receita", "receita", "aditiva"),
    ("n_votos_tmdb", "n_votos_tmdb", "aditiva"),
    ("popularidade_tmdb", "popularidade_tmdb", "nao_aditiva"),
    ("nota_tmdb", "nota_tmdb", "nao_aditiva"),
    ("nota_imdb", "nota_imdb", "nao_aditiva"),
    ("metascore", "metascore", "nao_aditiva"),
    ("n_vitorias", "n_vitorias", "aditiva"),
    ("n_indicacoes", "n_indicacoes", "aditiva"),
    ("teve_oscar", "CAST(teve_oscar AS INTEGER)", "aditiva"),
    ("teve_oscar_vitoria", "CAST(teve_oscar_vitoria AS INTEGER)", "aditiva"),
    ("teve_indicacao", "CAST(teve_indicacao AS INTEGER)", "aditiva"),
    ("teve_vitoria", "CAST(teve_vitoria AS INTEGER)", "aditiva"),
    ("teve_reconhecimento", "CAST(teve_reconhecimento AS INTEGER)", "aditiva"),
    ("awards_sem_informacao", "CAST(awards_sem_informacao AS INTEGER)", "aditiva"),
    ("tem_dado_omdb", "CAST(tem_dado_omdb AS INTEGER)", "aditiva"),
]

CONFERENCIA = [("linhas", "SELECT COUNT(*) FROM filmes", "SELECT COUNT(*) FROM fato_filme")]
for _nome, _expr, _classe in MEDIDAS:
    CONFERENCIA.append(
        (
            f"soma {_nome}",
            f"SELECT SUM({_expr}) FROM filmes",
            f"SELECT SUM({_nome}) FROM fato_filme",
        )
    )

# A fato agregada confere contra a fato de onde vem: a mesma conta, sem
# o GROUP BY. O filtro é o mesmo da agregada (só filmes com registro no
# OMDB), e a junção com a ponte repete o filme em cada gênero dele.
_ORIGEM_AGREGADA = (
    "FROM fato_filme f JOIN ponte_filme_genero p ON p.tmdb_id = f.tmdb_id "
    "WHERE COALESCE(f.tem_dado_omdb, 0) = 1"
)
CONFERENCIA_AGREGADA = [
    ("n_filmes (par filme-gênero)", f"SELECT COUNT(*) {_ORIGEM_AGREGADA}", "n_filmes"),
    ("n_oscar", f"SELECT SUM(f.teve_oscar) {_ORIGEM_AGREGADA}", "n_oscar"),
    ("n_oscar_vitoria", f"SELECT SUM(f.teve_oscar_vitoria) {_ORIGEM_AGREGADA}", "n_oscar_vitoria"),
    ("n_com_orcamento", f"SELECT COUNT(f.orcamento) {_ORIGEM_AGREGADA}", "n_com_orcamento"),
    ("soma_orcamento", f"SELECT SUM(f.orcamento) {_ORIGEM_AGREGADA}", "soma_orcamento"),
    ("n_com_nota_imdb", f"SELECT COUNT(f.nota_imdb) {_ORIGEM_AGREGADA}", "n_com_nota_imdb"),
    ("soma_nota_imdb", f"SELECT SUM(f.nota_imdb) {_ORIGEM_AGREGADA}", "soma_nota_imdb"),
    ("n_com_metascore", f"SELECT COUNT(f.metascore) {_ORIGEM_AGREGADA}", "n_com_metascore"),
    ("soma_metascore", f"SELECT SUM(f.metascore) {_ORIGEM_AGREGADA}", "soma_metascore"),
]

# Agrupamento dos gêneros do TMDB em grupos maiores, para comparar com
# volume suficiente. É uma decisão da equipe (ver relatório). Gênero que
# não aparece aqui cai em "Outros".
GRUPOS_GENERO = {
    "Ação": "Ação e aventura",
    "Aventura": "Ação e aventura",
    "Guerra": "Ação e aventura",
    "Faroeste": "Ação e aventura",
    "Thriller": "Suspense e crime",
    "Crime": "Suspense e crime",
    "Mistério": "Suspense e crime",
    "Terror": "Suspense e crime",
    "Drama": "Drama e romance",
    "Romance": "Drama e romance",
    "História": "Drama e romance",
    "Música": "Drama e romance",
    "Comédia": "Comédia e família",
    "Família": "Comédia e família",
    "Animação": "Comédia e família",
    "Fantasia": "Fantasia e ficção",
    "Ficção científica": "Fantasia e ficção",
}


# --------------------------------------------------------------------------
# Construção das tabelas
# --------------------------------------------------------------------------

def _sql_grupo_genero() -> str:
    casos = " ".join(
        f"WHEN '{g}' THEN '{grupo}'" for g, grupo in sorted(GRUPOS_GENERO.items())
    )
    return f"CASE genero_nome {casos} ELSE 'Outros' END"


def dimensoes(con: duckdb.DuckDBPyConnection, teste_falha: bool = False) -> None:
    # A dimensão de data cobre o período inteiro, inclusive os dias sem
    # estreia. A chave é a própria data.
    serie = "range" if teste_falha else "generate_series"
    con.execute(
        f"""
        CREATE TABLE dim_data AS
        WITH limites AS (
            SELECT MIN(TRY_CAST(data_lancamento AS DATE)) AS inicio,
                   MAX(TRY_CAST(data_lancamento AS DATE)) AS fim
            FROM filmes
        ), dias AS (
            SELECT CAST(d AS DATE) AS data
            FROM limites, {serie}(inicio, fim, INTERVAL 1 DAY) AS s(d)
        )
        SELECT data,
               year(data) AS ano,
               month(data) AS mes,
               (year(data) // 10) * 10 AS decada,
               data >= DATE '{DATA_CORTE}' AS parte_guardada
        FROM dias
        """
    )

    con.execute(
        """
        CREATE TABLE dim_origem AS
        SELECT ROW_NUMBER() OVER (ORDER BY pais_producao, idioma_original) AS origem_sk,
               pais_producao, idioma_original
        FROM (
            SELECT DISTINCT COALESCE(pais_producao, 'desconhecido') AS pais_producao,
                            COALESCE(idioma_original, 'desconhecido') AS idioma_original
            FROM filmes
        )
        """
    )

    con.execute(
        """
        CREATE TABLE dim_perfil AS
        SELECT ROW_NUMBER() OVER (ORDER BY status, tem_franquia, origem_coleta) AS perfil_sk,
               status, tem_franquia, origem_coleta
        FROM (
            SELECT DISTINCT COALESCE(status, 'desconhecido') AS status,
                            COALESCE(tem_franquia, FALSE) AS tem_franquia,
                            COALESCE(origem_coleta, 'desconhecida') AS origem_coleta
            FROM filmes
        )
        """
    )

    # Um filme tem vários gêneros, separados por ponto e vírgula na trusted.
    con.execute(
        """
        CREATE TEMP TABLE genero_filme AS
        SELECT DISTINCT tmdb_id, TRIM(genero) AS genero_nome
        FROM (
            SELECT tmdb_id,
                   UNNEST(string_split(
                       COALESCE(NULLIF(TRIM(generos), ''), 'Sem gênero informado'), ';'
                   )) AS genero
            FROM filmes
        )
        """
    )
    con.execute(
        f"""
        CREATE TABLE dim_genero AS
        SELECT ROW_NUMBER() OVER (ORDER BY genero_nome) AS genero_sk,
               genero_nome,
               {_sql_grupo_genero()} AS grupo_genero
        FROM (SELECT DISTINCT genero_nome FROM genero_filme)
        """
    )


def fatos(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE ponte_filme_genero AS
        SELECT gf.tmdb_id, g.genero_sk
        FROM genero_filme gf
        JOIN dim_genero g ON g.genero_nome = gf.genero_nome
        """
    )

    colunas_medidas = ",\n               ".join(
        f"{expr} AS {nome}" for nome, expr, _ in MEDIDAS
    )
    con.execute(
        f"""
        CREATE TABLE fato_filme AS
        SELECT f.tmdb_id,
               TRY_CAST(f.data_lancamento AS DATE) AS data_lancamento,
               o.origem_sk,
               p.perfil_sk,
               {colunas_medidas}
        FROM filmes f
        LEFT JOIN dim_origem o
               ON COALESCE(f.pais_producao, 'desconhecido') = o.pais_producao
              AND COALESCE(f.idioma_original, 'desconhecido') = o.idioma_original
        LEFT JOIN dim_perfil p
               ON COALESCE(f.status, 'desconhecido') = p.status
              AND COALESCE(f.tem_franquia, FALSE) = p.tem_franquia
              AND COALESCE(f.origem_coleta, 'desconhecida') = p.origem_coleta
        """
    )


def agregada(con: duckdb.DuckDBPyConnection) -> None:
    # Pergunta: qual a proporção de filmes indicados ou vencedores do Oscar
    # por gênero e década?
    # Granularidade: uma linha por década e gênero. Um filme com três
    # gêneros entra em três linhas, então n_filmes NÃO soma o total de
    # filmes quando se juntam gêneros.
    # Filtro que fica: só filmes com registro no OMDB (sem registro, a
    # ausência de Oscar não quer dizer nada).
    # Chave que se perde: o tmdb_id (da linha agregada não se volta ao filme).
    con.execute(
        """
        CREATE TABLE fato_oscar_genero_decada AS
        SELECT d.decada,
               p.genero_sk,
               COUNT(*) AS n_filmes,
               SUM(f.teve_oscar) AS n_oscar,
               SUM(f.teve_oscar_vitoria) AS n_oscar_vitoria,
               COUNT(f.orcamento) AS n_com_orcamento,
               SUM(f.orcamento) AS soma_orcamento,
               COUNT(f.nota_imdb) AS n_com_nota_imdb,
               ROUND(SUM(f.nota_imdb), 2) AS soma_nota_imdb,
               COUNT(f.metascore) AS n_com_metascore,
               ROUND(SUM(f.metascore), 2) AS soma_metascore
        FROM fato_filme f
        JOIN ponte_filme_genero p ON p.tmdb_id = f.tmdb_id
        LEFT JOIN dim_data d ON d.data = f.data_lancamento
        WHERE COALESCE(f.tem_dado_omdb, 0) = 1
        GROUP BY d.decada, p.genero_sk
        """
    )


# --------------------------------------------------------------------------
# Conferência (roda antes de gravar qualquer coisa)
# --------------------------------------------------------------------------

def _iguais(a, b) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-6)


def _valor(con: duckdb.DuckDBPyConnection, sql: str):
    return con.execute(sql).fetchall()[0][0]


def _fmt(v) -> str:
    if v is None:
        return "nulo"
    if float(v).is_integer():
        return f"{int(v):,}".replace(",", ".")
    return f"{float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def conferir(con: duckdb.DuckDBPyConnection) -> tuple[list[str], list[str]]:
    """Devolve (problemas, linhas impressas). Problema vazio = tudo bate."""
    problemas: list[str] = []
    saida: list[str] = []

    existentes = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    for tabela in TABELAS_GOLD:
        if tabela not in existentes:
            problemas.append(f"tabela do desenho que não foi construída: {tabela}")

    for tabela, coluna in CHAVES_UNICAS:
        repetidas = con.execute(
            f"SELECT {coluna}, COUNT(*) FROM {tabela} GROUP BY {coluna} HAVING COUNT(*) > 1 LIMIT 5"
        ).fetchall()
        if repetidas:
            problemas.append(
                f"chave repetida em {tabela}.{coluna}: " + ", ".join(str(r[0]) for r in repetidas)
            )

    for filha, col_filha, pai, col_pai in CHAVES:
        faltando = con.execute(
            f"""
            SELECT DISTINCT {col_filha} FROM {filha}
            WHERE {col_filha} IS NOT NULL
              AND {col_filha} NOT IN (SELECT {col_pai} FROM {pai})
            ORDER BY 1 LIMIT 10
            """
        ).fetchall()
        if faltando:
            chaves = ", ".join(str(r[0]) for r in faltando)
            problemas.append(
                f"falta em {pai}.{col_pai} a chave usada por {filha}.{col_filha}: {chaves}"
            )

    esperado, presentes = con.execute(
        "SELECT DATEDIFF('day', MIN(data), MAX(data)) + 1, COUNT(*) FROM dim_data"
    ).fetchall()[0]
    if esperado is not None and esperado != presentes:
        problemas.append(
            f"dim_data tem buraco: {presentes} datas para um período de {esperado} dias"
        )

    sem_data = _valor(con, "SELECT COUNT(*) FROM fato_filme WHERE data_lancamento IS NULL")
    saida.append(f"filmes sem data de lançamento (chave de data nula na fato): {sem_data}")
    saida.append("")

    saida.append("CONFERÊNCIA trusted x gold")
    saida.append(f"{'item':<34}{'trusted':>20}{'gold':>20}  bate")
    for rotulo, sql_trusted, sql_gold in CONFERENCIA:
        a, b = _valor(con, sql_trusted), _valor(con, sql_gold)
        ok = _iguais(a, b)
        saida.append(f"{rotulo:<34}{_fmt(a):>20}{_fmt(b):>20}  {'sim' if ok else 'NÃO'}")
        if not ok:
            problemas.append(f"{rotulo}: trusted {_fmt(a)} contra gold {_fmt(b)}")

    saida.append("")
    saida.append("CONFERÊNCIA fato agregada x fato_filme")
    saida.append(f"{'item':<34}{'fato_filme':>20}{'agregada':>20}  bate")
    for rotulo, sql_fato, coluna in CONFERENCIA_AGREGADA:
        a = _valor(con, sql_fato)
        b = _valor(con, f"SELECT SUM({coluna}) FROM fato_oscar_genero_decada")
        ok = _iguais(a, b)
        saida.append(f"{rotulo:<34}{_fmt(a):>20}{_fmt(b):>20}  {'sim' if ok else 'NÃO'}")
        if not ok:
            problemas.append(f"agregada, {rotulo}: fato {_fmt(a)} contra agregada {_fmt(b)}")

    return problemas, saida


# --------------------------------------------------------------------------
# Gravação
# --------------------------------------------------------------------------

def gravar(con: duckdb.DuckDBPyConnection) -> list[Path]:
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    gravados: list[Path] = []
    for tabela, ordem in TABELAS_GOLD.items():
        destino = GOLD_DIR / f"{tabela}.parquet"
        temporario = GOLD_DIR / f"{tabela}.parquet.tmp"
        con.execute(
            f"COPY (SELECT * FROM {tabela} ORDER BY {ordem}) TO '{temporario.as_posix()}' (FORMAT PARQUET)"
        )
        os.replace(temporario, destino)
        gravados.append(destino)
    return gravados


# --------------------------------------------------------------------------
# Execução principal
# --------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Constrói o gold a partir da trusted.")
    parser.add_argument(
        "--teste-falha",
        action="store_true",
        help="usa range no lugar de generate_series na dim_data, para provar que o script para",
    )
    args = parser.parse_args()

    caminho = TABELAS_TRUSTED["filmes"]
    if not caminho.exists():
        raise SystemExit(f"Não encontrei {caminho}. Rode o transform.py antes.")

    con = duckdb.connect()
    con.execute(f"CREATE VIEW filmes AS SELECT * FROM read_parquet('{caminho.as_posix()}')")

    dimensoes(con, teste_falha=args.teste_falha)
    fatos(con)
    agregada(con)

    problemas, saida = conferir(con)
    print("\n".join(saida))

    if problemas:
        print("\nPROBLEMAS ENCONTRADOS")
        for p in problemas:
            print(f" - {p}")
        raise SystemExit("\nA conferência falhou. Nada foi gravado em data/gold/.")

    gravados = gravar(con)
    print(f"\nTudo bate. {len(gravados)} tabelas gravadas em {GOLD_DIR}:")
    for caminho_gold in gravados:
        linhas = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{caminho_gold.as_posix()}')"
        ).fetchall()[0][0]
        print(f" - {caminho_gold.name}: {_fmt(linhas)} linhas")


if __name__ == "__main__":
    main()
