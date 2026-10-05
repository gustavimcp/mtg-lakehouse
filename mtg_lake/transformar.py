"""Etapa 2 - Transformação e testes: dbt + DuckDB lendo a camada bronze do lake."""

from __future__ import annotations

import json
import logging
import os

from .config import DBT_DIR, Config

log = logging.getLogger(__name__)


def executar(
    cfg: Config,
    snapshot: str,
    comando: str = "build",
    selecao: str | None = None,
    vars_extra: dict | None = None,
) -> None:
    """Roda `dbt build` (modelos + testes). Se qualquer teste falhar, levanta erro e nada é publicado."""
    from dbt.cli.main import dbtRunner

    dbt_vars = {"lake": cfg.lake, "snapshot_date": snapshot, **(vars_extra or {})}
    cfg.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    os.environ["MTG_DUCKDB_PATH"] = cfg.duckdb_path.as_posix()  # lido pelo profiles.yml

    args = [
        comando,
        "--project-dir",
        str(DBT_DIR),
        "--profiles-dir",
        str(DBT_DIR),
        "--vars",
        json.dumps(dbt_vars),
    ]
    if selecao:
        args += ["--select", selecao]

    log.info("dbt %s (snapshot %s)", comando, snapshot)
    resultado = dbtRunner().invoke(args)
    if not resultado.success:
        raise RuntimeError(f"dbt {comando} falhou; veja o log acima (dbt/logs/dbt.log)")
