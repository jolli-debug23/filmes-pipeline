"""
migrar_pendentes_omdb.py — Roda UMA VEZ, antes da próxima execução do
ingest.py atualizado. Escaneia data/raw/ em busca de filmes que já foram
coletados sem OMDB por orçamento diário esgotado (marcados com
omdb_skipped_daily_budget=True pelo próprio ingest.py) e registra esses
tmdb_id em state/ingest_state.json, para que o backfill automático do
ingest.py os recupere na próxima execução.

Uso:
    uv run python migrar_pendentes_omdb.py

Depois de confirmar que funcionou (rodando o ingest.py e vendo a mensagem
de backfill no log), este arquivo pode ser apagado — é migração de
estado, não faz parte da pipeline documentada no README.
"""
import json
from pathlib import Path

RAW_DIR = Path("data/raw")
STATE_PATH = Path("state/ingest_state.json")


def main() -> None:
    if not STATE_PATH.exists():
        print(f"Não encontrei {STATE_PATH} — rode o ingest.py pelo menos uma vez antes.")
        return

    pendentes = set()
    for f in RAW_DIR.glob("dados-tmdb_omdb-*.json"):
        if f.name.endswith(".meta.json"):
            continue
        for r in json.loads(f.read_text(encoding="utf-8")):
            if r.get("omdb_skipped_daily_budget"):
                pendentes.add(r["tmdb_id"])

    state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    ja_pendentes = set(state.get("pendentes_omdb", []))
    state["pendentes_omdb"] = sorted(ja_pendentes | pendentes)
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"{len(pendentes)} filme(s) encontrados com omdb_skipped_daily_budget=True.")
    print(f"Total agora em pendentes_omdb: {len(state['pendentes_omdb'])}")
    print("Pronto — rode o ingest.py de novo para recuperá-los.")


if __name__ == "__main__":
    main()
