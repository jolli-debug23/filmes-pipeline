# Pipeline de Dados de Filmes — TMDB/OMDb

Este projeto implementa um pipeline acadêmico em Python para coletar, validar,
explorar e modelar dados de filmes obtidos das APIs TMDB e OMDb.

**Equipe:**

- João Victor Oliveira Silva
- Akilan Henderson Ribeiro Gomes

## Estrutura das camadas

O fluxo usa as seguintes camadas:

```text
TMDB/OMDb -> data/raw/ -> data/trusted/ -> data/gold/
```

- `data/raw/`: arquivos JSON recebidos da fonte;
- `data/trusted/`: filme válido depois da aplicação do contrato e das regras de qualidade;
- `data/quarentena/`: registros que não passaram no contrato;
- `data/gold/`: modelo estrela usado pelas análises;
- `analises/`: documentos Quarto de diagnóstico e exploração;
- `src/`: scripts de coleta, transformação e modelagem.

## Pré-requisitos

- Python `>=3.14`, conforme `pyproject.toml`;
- `uv` instalado;
- Quarto CLI disponível ao `uv` (`quarto-cli`);
- chaves válidas das APIs TMDB e OMDb;
- acesso à internet na execução da coleta.

## Configuração de credenciais

Crie o arquivo local de configuração a partir do modelo versionado:

```bash
cp .env.example .env
```

Edite apenas o `.env` local e substitua os placeholders pelas próprias chaves:

```env
TMDB_API_KEY=sua_chave_aqui
OMDB_API_KEY=sua_chave_aqui
```

O `.env` é ignorado pelo Git e não deve ser enviado ao repositório. O
`.env.example` contém apenas nomes de variáveis vazias e pode ser versionado.
Nunca coloque uma chave real no README, no `.env.example`, no código ou em
commits.

Para recriar as dependências definidas pelo projeto:

```bash
uv sync
```

## Sequência completa do clone ao Gold

Execute os comandos a partir da raiz do projeto e na ordem abaixo:

```bash
uv sync
uv run python src/ingest.py
uv run quarto render analises/diagnostico.qmd
uv run python src/transform.py
uv run quarto render analises/exploracao.qmd
uv run python src/model.py
```

### Observações sobre os scripts

- `src/ingest.py` lê o estado existente e os arquivos já presentes em
  `data/raw/`. Se não houver filme novo, ele informa que nenhum registro novo
  foi gravado e não é necessário apagar o diretório manualmente.
- `analises/diagnostico.qmd` roda a partir da raiz do projeto graças ao
  `_quarto.yml` com `execute-dir: project`.
- `src/transform.py` recria `data/trusted/` e `data/quarentena/`.
- `analises/exploracao.qmd` consulta a parte de exploração anterior ao corte
  temporal configurado em `src/parametros.py`.
- `src/model.py` recria a camada Gold a partir de
  `data/trusted/filmes.parquet`.

## Arquivos da camada Gold

`src/model.py` grava um arquivo Parquet por tabela do modelo estrela:

| Arquivo | Papel no modelo | Granularidade esperada |
|---|---|---|
| `data/gold/dim_data.parquet` | Dimensão de calendário | Uma linha por dia no período; 9.051 linhas nesta execução |
| `data/gold/dim_origem.parquet` | Origem por país e idioma | Uma linha por combinação; 38 linhas nesta execução |
| `data/gold/dim_perfil.parquet` | Status, franquia e origem da coleta | Uma linha por combinação; 2 linhas nesta execução |
| `data/gold/dim_genero.parquet` | Gênero e grupo de gênero | Uma linha por gênero; 18 linhas nesta execução |
| `data/gold/ponte_filme_genero.parquet` | Relação muitos-para-muitos entre filmes e gêneros | Um par por filme-gênero; 1.835 linhas nesta execução |
| `data/gold/fato_filme.parquet` | Medidas e chaves da unidade principal | Uma linha por `tmdb_id`; 625 linhas nesta execução |
| `data/gold/fato_oscar_genero_decada.parquet` | Oscar por gênero e década | Uma linha por `decada, genero_sk`; 51 linhas nesta execução |

As quantidades podem variar se a trusted for recolhida ou tratada novamente.
Os valores acima correspondem à trusted validada nesta entrega.

## Testes de integridade

Antes da gravação, `src/model.py` confere chaves únicas, chaves estrangeiras,
a continuidade da dimensão de data, contagens e somas entre trusted e Gold e os
valores da fato agregada.

Para provar o bloqueio por chave faltante:

```bash
uv run python src/model.py --teste-falha
```

O teste substitui `generate_series` por `range` na dimensão de data. Como
`range` não inclui o último dia da série, uma chave usada pela fato fica
ausente. O script deve:

- imprimir a relação pai/filha e a chave ausente;
- mostrar que a conferência falhou;
- terminar com código de saída diferente de zero;
- não criar nem sobrescrever tabelas em `data/gold/`.

Após o teste, execute novamente:

```bash
uv run python src/model.py
```

para restaurar o Gold válido.

## Artefatos gerados e ignorados

Os seguintes diretórios e arquivos não devem ser enviados ao Git:

- `.env`;
- `.venv/`;
- `data/raw/*`, mantendo `data/raw/.gitkeep`;
- `data/trusted/*`;
- `data/quarentena/*`;
- `data/gold/*`;
- `state/`;
- `logs/`;
- `.quarto/`;
- arquivos HTML e arquivos auxiliares gerados pelo Quarto.

O clone novo precisa recriar os dados brutos, trusted, quarentena e Gold usando
os scripts da sequência documentada.