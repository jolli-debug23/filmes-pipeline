"""
visualize.py — Gera visualizações exploratórias a partir de
data/trusted/filmes.parquet (e, se existir, data/quarentena/), salvando
PNGs em docs/figures/ para usar no relatório.

Uso:
    uv run src/visualize.py

Dependências (uv add matplotlib):
    matplotlib (pandas/pyarrow já vêm do transform.py)

Por que um script e não abrir num notebook: os gráficos viram arquivo
em docs/figures/, então entram no relatório em PDF sem depender de
rodar nada de interativo — e o mesmo script serve tanto pra gerar as
figuras do relatório quanto pra qualquer um da equipe conferir
rapidamente o estado do dado trusted.

Reprodutibilidade: os nomes dos arquivos são fixos (não têm timestamp)
e cada gráfico é determinístico a partir do mesmo data/trusted/ — rodar
de novo apenas sobrescreve as mesmas imagens, não acumula lixo.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # script sem tela — só salva arquivo
import matplotlib.pyplot as plt
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
TRUSTED_PATH = ROOT_DIR / "data" / "trusted" / "filmes.parquet"
QUARANTINE_PATH = ROOT_DIR / "data" / "quarentena" / "filmes_quarentena.csv"
FIGURES_DIR = ROOT_DIR / "docs" / "figures"
FIGURES_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("visualize")

plt.rcParams["figure.dpi"] = 120
plt.rcParams["axes.titlesize"] = 12


def load_trusted() -> pd.DataFrame:
    if not TRUSTED_PATH.exists():
        log.critical(
            "Não encontrei %s — rode o ingest.py e o transform.py antes.", TRUSTED_PATH
        )
        sys.exit(1)
    return pd.read_parquet(TRUSTED_PATH)


def load_quarantine() -> pd.DataFrame:
    if not QUARANTINE_PATH.exists():
        return pd.DataFrame()
    return pd.read_csv(QUARANTINE_PATH)


# --------------------------------------------------------------------------
# Gráficos
# --------------------------------------------------------------------------

def plot_missingness(df: pd.DataFrame) -> Path:
    """Completude por coluna — apoia a dimensão 'completude' de Batini
    exigida no relatório."""
    pct_presente = (1 - df.isna().mean()).sort_values() * 100
    fig, ax = plt.subplots(figsize=(8, max(4, 0.3 * len(pct_presente))))
    ax.barh(pct_presente.index, pct_presente.values, color="#4C72B0")
    ax.set_xlabel("% de registros com valor preenchido")
    ax.set_title("Completude por coluna — data/trusted/")
    ax.set_xlim(0, 100)
    fig.tight_layout()
    out = FIGURES_DIR / "completude_por_coluna.png"
    fig.savefig(out)
    plt.close(fig)
    return out


def plot_genre_frequency(df: pd.DataFrame, top_n: int = 15) -> Path | None:
    if "generos" not in df.columns or df["generos"].dropna().empty:
        return None
    counts = df["generos"].dropna().str.split(";").explode().value_counts().head(top_n)
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.barh(counts.index[::-1], counts.values[::-1], color="#55A868")
    ax.set_xlabel("Número de filmes")
    ax.set_title(f"Top {top_n} gêneros — data/trusted/")
    fig.tight_layout()
    out = FIGURES_DIR / "generos_frequencia.png"
    fig.savefig(out)
    plt.close(fig)
    return out


def plot_award_proportion(df: pd.DataFrame) -> Path | None:
    if "teve_premiacao" not in df.columns:
        return None
    counts = df["teve_premiacao"].value_counts().reindex([True, False]).fillna(0)
    fig, ax = plt.subplots(figsize=(4, 4))
    ax.pie(
        counts.values,
        labels=["Com indicação/prêmio", "Sem evidência"],
        autopct="%1.0f%%",
        colors=["#C44E52", "#8C8C8C"],
    )
    ax.set_title("Proporção com indicação/prêmio (OMDB Awards)")
    fig.tight_layout()
    out = FIGURES_DIR / "proporcao_premiacao.png"
    fig.savefig(out)
    plt.close(fig)
    return out


def plot_budget_vs_revenue(df: pd.DataFrame) -> Path | None:
    plot_df = df.dropna(subset=["orcamento", "receita"])
    if plot_df.empty:
        return None
    fig, ax = plt.subplots(figsize=(6, 6))
    colors = plot_df["teve_premiacao"].map({True: "#C44E52", False: "#4C72B0"})
    ax.scatter(plot_df["orcamento"], plot_df["receita"], c=colors, alpha=0.6, s=20)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Orçamento (log, US$)")
    ax.set_ylabel("Receita (log, US$)")
    ax.set_title("Orçamento vs. receita (vermelho = com indicação/prêmio)")
    fig.tight_layout()
    out = FIGURES_DIR / "orcamento_vs_receita.png"
    fig.savefig(out)
    plt.close(fig)
    return out


def plot_ratings_distribution(df: pd.DataFrame) -> Path | None:
    cols = [c for c in ("nota_tmdb", "nota_imdb") if c in df.columns and df[c].notna().any()]
    if not cols:
        return None
    fig, ax = plt.subplots(figsize=(7, 4))
    for col, cor in zip(cols, ["#4C72B0", "#DD8452"]):
        ax.hist(df[col].dropna(), bins=20, alpha=0.6, label=col, color=cor)
    ax.set_xlabel("Nota (0-10)")
    ax.set_ylabel("Número de filmes")
    ax.set_title("Distribuição das notas — TMDB vs. IMDb")
    ax.legend()
    fig.tight_layout()
    out = FIGURES_DIR / "distribuicao_notas.png"
    fig.savefig(out)
    plt.close(fig)
    return out


def plot_quarantine_reasons(quarantine_df: pd.DataFrame) -> Path | None:
    if quarantine_df.empty or "motivo_quarentena" not in quarantine_df.columns:
        return None
    counts = quarantine_df["motivo_quarentena"].value_counts()
    fig, ax = plt.subplots(figsize=(8, max(3, 0.4 * len(counts))))
    ax.barh(counts.index[::-1], counts.values[::-1], color="#C44E52")
    ax.set_xlabel("Número de registros")
    ax.set_title("Motivos de quarentena — data/quarentena/")
    fig.tight_layout()
    out = FIGURES_DIR / "motivos_quarentena.png"
    fig.savefig(out)
    plt.close(fig)
    return out


def plot_raw_vs_trusted_vs_quarentena(n_trusted: int, n_quarentena: int) -> Path:
    fig, ax = plt.subplots(figsize=(4, 4))
    labels = ["Trusted", "Quarentena"]
    values = [n_trusted, n_quarentena]
    ax.bar(labels, values, color=["#55A868", "#C44E52"])
    ax.set_ylabel("Número de registros")
    ax.set_title("Destino dos registros após o CONTRATO")
    for i, v in enumerate(values):
        ax.text(i, v, str(v), ha="center", va="bottom")
    fig.tight_layout()
    out = FIGURES_DIR / "trusted_vs_quarentena.png"
    fig.savefig(out)
    plt.close(fig)
    return out


# --------------------------------------------------------------------------
# Execução principal
# --------------------------------------------------------------------------

def main() -> None:
    trusted_df = load_trusted()
    quarantine_df = load_quarantine()
    log.info(
        "%d registros em trusted, %d em quarentena.", len(trusted_df), len(quarantine_df)
    )

    generated = [
        plot_missingness(trusted_df),
        plot_genre_frequency(trusted_df),
        plot_award_proportion(trusted_df),
        plot_budget_vs_revenue(trusted_df),
        plot_ratings_distribution(trusted_df),
        plot_quarantine_reasons(quarantine_df),
        plot_raw_vs_trusted_vs_quarentena(len(trusted_df), len(quarantine_df)),
    ]
    generated = [p for p in generated if p is not None]

    print(f"{len(generated)} gráfico(s) salvos em {FIGURES_DIR}:")
    for p in generated:
        print(f" - {p.name}")


if __name__ == "__main__":
    main()
