# O desenho do modelo (texto para a seção 3 do relatório)

Este texto segue a ordem do enunciado e descreve exatamente o que o
`src/model.py` grava em `data/gold/`. Se mudar o código, mude aqui.

## 1. O processo de negócio

Um filme **estreia**. A partir da estreia, ele passa a ser avaliado pelo público
e pela crítica, e pode receber indicações e prêmios. Os registros da fonte
descrevem esse acontecimento: cada filme com a sua data de estreia, as notas e o
texto de prêmios.

## 2. A granularidade

**Uma linha por filme** (um `tmdb_id`), como ele estava no dia da coleta.

Uma linha mais grossa, por década e gênero, esconde o filme. A pergunta da
equipe é "o que distingue um filme com Oscar dos outros", e isso só se responde
olhando o filme. Por isso a linha por década e gênero existe só como fato
agregada (item 6), e a fato principal fica no filme.

Uma linha mais fina, por filme e gênero, repetiria o orçamento e a receita do
filme em cada gênero dele, e a soma do orçamento contaria o mesmo dinheiro duas
ou três vezes. Uma linha por filme e prêmio nem seria possível, porque o OMDB
só informa a quantidade de prêmios, num texto.

## 3. As dimensões

Cada dimensão vale para a granularidade da frase 2: um filme tem uma data de
estreia, uma origem, um perfil, e gêneros.

| Dimensão | Dois atributos para filtrar ou agrupar | Por que vale para "uma linha por filme" |
|---|---|---|
| `dim_data` | `ano` e `decada` (também `mes` e `parte_guardada`) | O filme estreia em um dia. A chave é a própria data, e a dimensão cobre todos os dias do período, inclusive os sem estreia. |
| `dim_origem` | `pais_producao` e `idioma_original` | Cada filme tem um país de produção (o primeiro que o TMDB lista) e um idioma original. |
| `dim_perfil` | `tem_franquia` e `status` (também `origem_coleta`) | O filme pertence ou não a uma franquia, tem um status de lançamento e foi coletado de um jeito (por mês, ou pelos mais populares numa coleta antiga). |
| `dim_genero` | `genero_nome` e `grupo_genero` | O filme tem gêneros. Como são vários, ficam ligados pela ponte (item 5). |

O `grupo_genero` junta os gêneros do TMDB em cinco grupos mais um grupo
"Outros" (Ação e aventura, Suspense e crime, Drama e romance, Comédia e
família, Fantasia e ficção). É uma decisão da equipe, para comparar grupos com
volume suficiente. A tabela do agrupamento está em `GRUPOS_GENERO` no
`src/model.py`.

Todas as dimensões, menos a de data, têm chave substituta (`origem_sk`,
`perfil_sk`, `genero_sk`).

## 4. As medidas

| Medida | Classe |
|---|---|
| `duracao_min`, `orcamento`, `receita`, `n_votos_tmdb` | Aditiva. A soma entre filmes faz sentido. Orçamento e receita só existem para parte dos filmes (zero do TMDB virou ausente), então a média é soma dividida pela contagem de filmes com valor. |
| `n_vitorias`, `n_indicacoes` | Aditiva. São contagens de prêmios de cada filme. Nulas quando o OMDB não traz texto de prêmios. |
| `teve_oscar`, `teve_oscar_vitoria`, `teve_indicacao`, `teve_vitoria`, `teve_reconhecimento`, `awards_sem_informacao`, `tem_dado_omdb` | Aditiva como contagem de 0 e 1. A **proporção** é soma dividida pela contagem de filmes do grupo, nunca média de proporções nem soma de proporções. |
| `nota_tmdb`, `nota_imdb`, `metascore` | Não aditiva. Nunca somar notas para responder nada. A conta certa é a soma dividida pela contagem de filmes com nota. |
| `popularidade_tmdb` | Não aditiva. É um índice de um momento. Só serve para média ou comparação. |

## 5. O que é de outra granularidade

- **Gêneros.** Um filme tem vários, então o gênero é de uma granularidade mais
  fina (filme e gênero). Ficou na `ponte_filme_genero`, com uma linha por filme
  e gênero, ligada à `dim_genero` e à fato pelo `tmdb_id`. Ficou fora da fato
  para não repetir as medidas do filme em cada gênero. Quem consulta por gênero
  precisa saber que um filme aparece em todos os gêneros dele.
- **Diretor e elenco principal.** Também são vários por filme. Ficaram **fora
  do gold** nesta etapa, e continuam na trusted. Nenhuma pergunta da exploração
  usa essas colunas. Se a Etapa 3 pedir, entram como uma dimensão e uma ponte,
  do mesmo jeito que o gênero.

## 6. A fato agregada

- **Pergunta:** qual a proporção de filmes indicados ou vencedores do Oscar, por
  gênero e por década?
- **Granularidade:** uma linha por década e gênero (`fato_oscar_genero_decada`).
  Um filme com três gêneros entra em três linhas, então `n_filmes` não soma o
  total de filmes quando se juntam gêneros.
- **Filtro que fica:** só filmes com registro no OMDB (`tem_dado_omdb`). Sem
  registro, a ausência de texto de prêmios não quer dizer ausência de Oscar.
- **Chave da linha atômica que se perde:** o `tmdb_id`. Da linha agregada não se
  volta ao filme.
- **Medidas guardadas como soma e contagem**, nunca como proporção pronta:
  `n_filmes`, `n_oscar`, `n_oscar_vitoria`, e para orçamento, nota do IMDb e
  Metascore a contagem de filmes com valor e a soma. A proporção é feita na
  consulta: `n_oscar / n_filmes`.
- Conferida contra a `fato_filme`: o `model.py` refaz cada soma sem o
  `GROUP BY`, a partir da fato e da ponte, e compara.

## O que o desenho tem de decisão da equipe a confirmar

1. O Oscar como medida de premiação (as colunas gerais quase não variam).
2. O país de produção como o primeiro país listado.
3. Os cinco grupos de gênero.
4. Deixar diretor e elenco fora do gold.
5. O universo (2000 a 2024, no mínimo 100 votos) e a cota de 4 filmes por mês, em `src/parametros.py`.
