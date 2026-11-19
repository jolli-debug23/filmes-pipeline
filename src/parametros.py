"""
parametros.py: valores que precisam ser iguais em mais de um arquivo.

Ficam aqui, e não no .env, porque não são segredo e fazem parte do método:
quem clona o repositório coleta e trata o mesmo universo.

UNIVERSO DE FILMES
    Filmes lançados entre DATA_INICIAL e DATA_FINAL, com pelo menos
    MIN_VOTOS votos no TMDB.
    - O período começa em 2000 para ter muitos anos de exposição a
      premiações e termina em 2024 para que os filmes mais novos já tenham
      passado por uma temporada de prêmios na data da coleta.
    - O mínimo de votos tira filmes com nota baseada em poucas avaliações.
      Não deixa de fora filmes pouco conhecidos, porque 100 votos é pouco.
    O src/ingest.py usa estes valores para consultar o TMDB. O
    src/transform.py usa os mesmos valores para garantir que só filmes do
    universo cheguem à trusted, venham de onde vierem no data/raw/.

COTA POR MÊS (FILMES_POR_MES)
    Coletar só os filmes mais populares puxa a amostra para filmes
    premiados. Para evitar isso, o ingest.py divide o período em meses e
    busca até FILMES_POR_MES filmes em cada mês. Quem já está no data/raw/
    conta para a cota, então a coleta nova só completa os meses que estão
    mais vazios. Por mês, e não por ano, porque os filmes que disputam o
    Oscar estreiam no fim do ano e uma cota por ano, com a ordem por data,
    pegaria só os filmes de janeiro.

PARTE GUARDADA (DATA_CORTE)
    Filmes lançados nesta data ou depois ficam fora da exploração
    (analises/exploracao.qmd) e só voltam a ser usados na Etapa 3, para
    confirmar ou derrubar as previsões da tabela de hipóteses. O
    src/model.py usa o mesmo valor para marcar a parte guardada na
    dimensão de data. O corte é por tempo porque o dado tem data de
    lançamento: prever o presente com o passado é o uso real do que a
    equipe descobre, e um sorteio misturaria filmes do mesmo ano nas duas
    partes.
"""

DATA_INICIAL = "2000-01-01"
DATA_FINAL = "2024-12-31"
MIN_VOTOS = 100
FILMES_POR_MES = 4
ORDEM_TMDB = "primary_release_date.asc"

DATA_CORTE = "2023-01-01"
