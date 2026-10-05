"""Retenção da camada bronze e carga do histórico legado (CSVs do notebook antigo)."""

from __future__ import annotations

import logging
import re
from datetime import date, timedelta
from pathlib import Path

from .armazenamento import Lake, conectar
from .config import Config, ler_estado
from .publicar import FATOS, atualizar_manifesto, gravar_mes_fato

log = logging.getLogger(__name__)

_PARTICAO = re.compile(r"/dt=(\d{4}-\d{2}-\d{2})/")


def limpar_bronze(cfg: Config, referencia: str, dias: int | None = None) -> list[str]:
    """Apaga arquivos brutos com mais de N dias (o histórico de preços já está na camada gold)."""
    dias = cfg.retencao_bronze_dias if dias is None else dias
    limite = date.fromisoformat(referencia) - timedelta(days=dias)
    lake = Lake(cfg)
    apagados = []
    for rel in lake.listar("raw/scryfall/"):
        m = _PARTICAO.search(rel)
        if m and date.fromisoformat(m.group(1)) < limite:
            lake.apagar(rel)
            apagados.append(rel)
    log.info("Retenção bronze (%s dias): %s arquivos apagados", dias, len(apagados))
    return apagados


def carregar_legado(cfg: Config, arquivo: Path, fato: str) -> int:
    """Importa um CSV legado (id;usd;usd_foil;usd_etched;eur;eur_foil;tix;date) para o histórico mensal.

    Usa o modo "completar": se já houver preço para (id, data) no lake, o do lake prevalece.
    """
    chave = FATOS[fato]
    lake = Lake(cfg)
    con = conectar(cfg)
    caminho = arquivo.resolve().as_posix().replace("'", "''")
    try:
        con.execute(
            f"""
            CREATE TEMP TABLE legado AS
            SELECT
              id                                          AS {chave},
              TRY_CAST(LEFT("date", 10) AS DATE)          AS price_date,
              TRY_CAST(usd        AS DECIMAL(12,2))       AS usd,
              TRY_CAST(usd_foil   AS DECIMAL(12,2))       AS usd_foil,
              TRY_CAST(usd_etched AS DECIMAL(12,2))       AS usd_etched,
              TRY_CAST(eur        AS DECIMAL(12,2))       AS eur,
              TRY_CAST(eur_foil   AS DECIMAL(12,2))       AS eur_foil,
              TRY_CAST(tix        AS DECIMAL(12,2))       AS tix,
              CURRENT_TIMESTAMP                           AS loaded_at
            FROM read_csv('{caminho}', delim = ';', header = true, all_varchar = true)
            WHERE id IS NOT NULL AND TRY_CAST(LEFT("date", 10) AS DATE) IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY id, price_date ORDER BY usd NULLS LAST) = 1
            """
        )
        meses = [
            r[0] for r in con.execute("SELECT DISTINCT strftime(price_date, '%Y-%m') FROM legado ORDER BY 1").fetchall()
        ]
        log.info("%s: %s meses de histórico", arquivo.name, len(meses))
        total = 0
        for mes in meses:
            total += gravar_mes_fato(
                con,
                lake,
                fato,
                mes,
                f"SELECT * FROM legado WHERE strftime(price_date, '%Y-%m') = '{mes}'",
                modo="completar",
            )
        atualizar_manifesto(lake, ler_estado(cfg).get("snapshot_date"))
        return total
    finally:
        con.close()
