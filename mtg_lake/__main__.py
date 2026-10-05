"""Linha de comando do pipeline.

python -m mtg_lake diario                 # extrair + transformar + publicar + limpar
python -m mtg_lake extrair [--forcar]
python -m mtg_lake transformar [--snapshot AAAA-MM-DD] [--select dim_cards+]
python -m mtg_lake publicar [--snapshot AAAA-MM-DD]
python -m mtg_lake limpar [--dias 14]
python -m mtg_lake legado --arquivo ../mtg_prices_historic.csv --fato fct_card_prices
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from . import config, extrair, manutencao, publicar, transformar


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m mtg_lake", description="Pipeline MTG: Scryfall -> lake -> Power BI"
    )
    sub = parser.add_subparsers(dest="comando", required=True)

    p = sub.add_parser("diario", help="pipeline completo do dia")
    p.add_argument("--forcar", action="store_true", help="baixa de novo mesmo se o snapshot já estiver no lake")

    p = sub.add_parser("extrair", help="Scryfall -> bronze")
    p.add_argument("--forcar", action="store_true")

    p = sub.add_parser("transformar", help="dbt build (modelos + testes)")
    p.add_argument("--snapshot")
    p.add_argument("--select", help="seleção do dbt, ex.: dim_cards+")
    p.add_argument("--vars", default="{}", help="vars extras do dbt em JSON, ex.: '{\"min_sets\": 10}'")

    p = sub.add_parser("publicar", help="gold (DuckDB) -> Parquet no lake")
    p.add_argument("--snapshot")

    p = sub.add_parser("limpar", help="retenção da camada bronze")
    p.add_argument("--snapshot")
    p.add_argument("--dias", type=int)

    p = sub.add_parser("legado", help="importa um CSV de preços do notebook antigo")
    p.add_argument("--arquivo", type=Path, required=True)
    p.add_argument("--fato", choices=sorted(publicar.FATOS), required=True)

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cfg = config.carregar()
    logging.getLogger(__name__).info("Lake: %s", cfg.lake)

    if args.comando == "diario":
        snap = extrair.executar(cfg, forcar=args.forcar)
        transformar.executar(cfg, snap)
        publicar.executar(cfg, snap)
        manutencao.limpar_bronze(cfg, snap)
    elif args.comando == "extrair":
        extrair.executar(cfg, forcar=args.forcar)
    elif args.comando == "transformar":
        transformar.executar(
            cfg, config.snapshot_atual(cfg, args.snapshot), selecao=args.select, vars_extra=json.loads(args.vars)
        )
    elif args.comando == "publicar":
        publicar.executar(cfg, config.snapshot_atual(cfg, args.snapshot))
    elif args.comando == "limpar":
        manutencao.limpar_bronze(cfg, config.snapshot_atual(cfg, args.snapshot), args.dias)
    elif args.comando == "legado":
        manutencao.carregar_legado(cfg, args.arquivo, args.fato)
    return 0


if __name__ == "__main__":
    sys.exit(main())
