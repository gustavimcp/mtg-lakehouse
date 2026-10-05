"""Etapa 3 - Publicação: tabelas do schema gold (DuckDB) -> Parquet na camada gold do lake.

- Dimensões e agregados: 1 arquivo por tabela, sobrescrito a cada dia.
- Fatos de preço: histórico particionado por mês (gold/<fato>/mes=AAAA-MM.parquet).
  Só o arquivo do mês corrente é reescrito; meses fechados ficam imutáveis.
- manifest.json: índice de todos os arquivos, lido pelo Power BI.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import PurePosixPath

import duckdb

from .armazenamento import Lake, conectar
from .config import Config

log = logging.getLogger(__name__)

# fato -> coluna de identificação
FATOS = {"fct_card_prices": "card_id", "fct_print_prices": "print_id"}
COLUNAS_PRECO = ("usd", "usd_foil", "usd_etched", "eur", "eur_foil", "tix")


def caminho_fato(fato: str, mes: str) -> str:
    return f"gold/{fato}/mes={mes}.parquet"


def _select_padronizado(origem: str, chave: str) -> str:
    """Mesmas colunas e tipos em todos os arquivos mensais (o Power BI combina por nome e tipo)."""
    precos = ",\n  ".join(f"CAST({c} AS DECIMAL(12,2)) AS {c}" for c in COLUNAS_PRECO)
    return f"""
SELECT
  CAST({chave} AS VARCHAR) AS {chave},
  CAST(price_date AS DATE) AS price_date,
  {precos},
  CAST(loaded_at AS TIMESTAMP) AS loaded_at
FROM ({origem})"""


def gravar_mes_fato(
    con: duckdb.DuckDBPyConnection,
    lake: Lake,
    fato: str,
    mes: str,
    origem_sql: str,
    modo: str,
) -> int:
    """Mescla `origem_sql` no arquivo mensal do fato.

    modo="substituir": as datas presentes na origem substituem as do arquivo (carga diária, idempotente).
    modo="completar":  só entram pares (id, data) que ainda não existem (carga do histórico legado).
    """
    chave = FATOS[fato]
    rel = caminho_fato(fato, mes)
    uri = lake.uri(rel)

    con.execute(f"CREATE OR REPLACE TEMP TABLE _origem AS {_select_padronizado(origem_sql, chave)}")
    if lake.existe(rel):
        existente = _select_padronizado(f"SELECT * FROM read_parquet('{uri}')", chave)
        if modo == "substituir":
            sql = f"""
                SELECT * FROM ({existente}) e
                WHERE e.price_date NOT IN (SELECT DISTINCT price_date FROM _origem)
                UNION ALL SELECT * FROM _origem"""
        elif modo == "completar":
            sql = f"""
                SELECT * FROM ({existente})
                UNION ALL
                SELECT o.* FROM _origem o
                WHERE NOT EXISTS (
                    SELECT 1 FROM ({existente}) e
                    WHERE e.{chave} = o.{chave} AND e.price_date = o.price_date)"""
        else:
            raise ValueError(modo)
    else:
        sql = "SELECT * FROM _origem"
    # Materializa antes de gravar: o arquivo de destino é o mesmo que está sendo lido
    con.execute(f"CREATE OR REPLACE TEMP TABLE _mes AS {sql}")

    duplicados = con.execute(f"SELECT COUNT(*) - COUNT(DISTINCT ({chave}, price_date)) FROM _mes").fetchone()[0]
    if duplicados:
        raise RuntimeError(f"{rel}: {duplicados} linhas duplicadas por ({chave}, price_date); nada foi gravado")

    linhas = con.execute("SELECT COUNT(*) FROM _mes").fetchone()[0]
    lake.preparar_destino(rel)
    con.execute(f"COPY (SELECT * FROM _mes ORDER BY price_date, {chave}) TO '{uri}' (FORMAT parquet, COMPRESSION zstd)")
    log.info("%s: %s linhas", rel, f"{linhas:,}")
    return linhas


def atualizar_manifesto(lake: Lake, snapshot: str | None) -> dict:
    arquivos = lake.listar("gold/")
    tabelas = {PurePosixPath(a).stem: a for a in arquivos if a.endswith(".parquet") and a.count("/") == 1}
    fatos = {f: [a for a in arquivos if a.startswith(f"gold/{f}/") and a.endswith(".parquet")] for f in FATOS}
    manifesto = {
        "versao": 1,
        "snapshot_date": snapshot,
        "publicado_em": datetime.now(UTC).isoformat(timespec="seconds"),
        "tabelas": tabelas,
        "fatos": fatos,
    }
    lake.escrever_texto("gold/manifest.json", json.dumps(manifesto, indent=2))
    return manifesto


def executar(cfg: Config, snapshot: str) -> dict:
    lake = Lake(cfg)
    con = conectar(cfg, cfg.duckdb_path)
    try:
        tabelas = [
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'gold' ORDER BY 1"
            ).fetchall()
        ]
        if not tabelas:
            raise RuntimeError("Schema gold vazio: rode 'python -m mtg_lake transformar' antes")

        for tabela in tabelas:
            if tabela.endswith("_dia"):
                continue  # fatos do dia entram no histórico mensal abaixo
            rel = f"gold/{tabela}.parquet"
            lake.preparar_destino(rel)
            con.execute(f"COPY gold.{tabela} TO '{lake.uri(rel)}' (FORMAT parquet, COMPRESSION zstd)")
            log.info("%s publicado", rel)

        for fato in FATOS:
            gravar_mes_fato(con, lake, fato, snapshot[:7], f"SELECT * FROM gold.{fato}_dia", modo="substituir")

        return atualizar_manifesto(lake, snapshot)
    finally:
        con.close()
