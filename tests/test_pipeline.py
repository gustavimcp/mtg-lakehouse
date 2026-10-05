"""Testes de ponta a ponta com uma amostra real da Scryfall (tests/fixtures), sem internet e sem nuvem.

pytest -q
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import duckdb
import pytest

from mtg_lake import config, extrair, manutencao, publicar, transformar

FIXTURES = Path(__file__).parent / "fixtures" / "lake"
SNAP = "2026-10-05"
# A amostra tem poucas cartas: baixa os limites de volume dos testes do dbt
VARS_AMOSTRA = {"min_oracle_cards": 100, "min_default_cards": 500, "min_sets": 50}


@pytest.fixture(scope="module")
def cfg(tmp_path_factory):
    base = tmp_path_factory.mktemp("mtg")
    shutil.copytree(FIXTURES, base / "lake")
    mp = pytest.MonkeyPatch()
    mp.setenv("MTG_LAKE", (base / "lake").as_posix())
    mp.setenv("MTG_DUCKDB_PATH", (base / "mtg.duckdb").as_posix())
    mp.setenv("MTG_ESTADO_PATH", (base / "estado.json").as_posix())
    cfg = config.carregar()
    config.salvar_estado(cfg, snapshot_date=SNAP)
    yield cfg
    mp.undo()


@pytest.fixture(scope="module")
def publicado(cfg):
    transformar.executar(cfg, SNAP, vars_extra=VARS_AMOSTRA)
    return publicar.executar(cfg, SNAP)


def _sql(cfg, consulta: str):
    return duckdb.sql(consulta.replace("{lake}", cfg.lake)).fetchall()


def test_caminho_bronze():
    assert extrair.caminho_bronze("sets", SNAP) == f"raw/scryfall/sets/dt={SNAP}/sets.jsonl.gz"


def test_validar_jsonl_gz():
    arquivo = FIXTURES / "raw" / "scryfall" / "sets" / f"dt={SNAP}" / "sets.jsonl.gz"
    assert extrair.validar_jsonl_gz(arquivo) > 50


def test_manifesto_lista_todas_as_tabelas(publicado):
    assert publicado["snapshot_date"] == SNAP
    assert {"dim_cards", "dim_prints", "dim_sets", "agg_year_verbose"} <= set(publicado["tabelas"])
    assert not any(t.endswith("_dia") for t in publicado["tabelas"])
    assert publicado["fatos"]["fct_print_prices"] == [f"gold/fct_print_prices/mes={SNAP[:7]}.parquet"]


def test_extras_sao_removidos(cfg, publicado):
    layouts = {r[0] for r in _sql(cfg, "SELECT DISTINCT layout FROM '{lake}/gold/dim_cards.parquet'")}
    assert not layouts & {"token", "emblem", "art_series", "double_faced_token"}


def test_carta_de_duas_faces(cfg, publicado):
    [(back, frente_ok)] = _sql(
        cfg,
        "SELECT card_back_name, image_uri_small IS NOT NULL FROM '{lake}/gold/dim_cards.parquet' "
        "WHERE name = 'Delver of Secrets // Insectile Aberration'",
    )
    assert back == "Insectile Aberration" and frente_ok


def test_fato_completo_e_sem_duplicados(cfg, publicado):
    [(linhas, distintos)] = _sql(
        cfg, "SELECT COUNT(*), COUNT(DISTINCT print_id) FROM '{lake}/gold/fct_print_prices/*.parquet'"
    )
    [(prints,)] = _sql(cfg, "SELECT COUNT(*) FROM '{lake}/gold/dim_prints.parquet'")
    assert linhas == distintos == prints


def test_publicar_de_novo_e_idempotente(cfg, publicado):
    antes = _sql(cfg, "SELECT COUNT(*) FROM '{lake}/gold/fct_card_prices/*.parquet'")
    publicar.executar(cfg, SNAP)
    assert _sql(cfg, "SELECT COUNT(*) FROM '{lake}/gold/fct_card_prices/*.parquet'") == antes


def test_legado_nao_sobrescreve_precos_existentes(cfg, publicado, tmp_path):
    [(card_id, usd)] = _sql(
        cfg, "SELECT card_id, usd FROM '{lake}/gold/fct_card_prices/*.parquet' WHERE usd IS NOT NULL LIMIT 1"
    )
    csv = tmp_path / "legado.csv"
    csv.write_text(
        "id;usd;usd_foil;usd_etched;eur;eur_foil;tix;date\n"
        f"{card_id};999.99;;;;;;{SNAP}\n"  # mesmo dia: o preço do lake prevalece
        f"{card_id};1.23;;;;;;2024-02-07\n",  # dia antigo: entra no histórico
        encoding="utf-8",
    )
    manutencao.carregar_legado(cfg, csv, "fct_card_prices")

    consulta = (
        "SELECT CAST(price_date AS VARCHAR), usd FROM '{lake}/gold/fct_card_prices/*.parquet' "
        f"WHERE card_id = '{card_id}'"
    )
    precos = dict(_sql(cfg, consulta))
    assert precos[SNAP] == usd
    assert float(precos["2024-02-07"]) == 1.23
    manifesto = json.loads((Path(cfg.lake) / "gold" / "manifest.json").read_text(encoding="utf-8"))
    assert "gold/fct_card_prices/mes=2024-02.parquet" in manifesto["fatos"]["fct_card_prices"]


def test_retencao_bronze(cfg, publicado):
    antigo = Path(cfg.lake) / extrair.caminho_bronze("sets", "2026-09-01")
    antigo.parent.mkdir(parents=True)
    antigo.write_bytes(b"")
    apagados = manutencao.limpar_bronze(cfg, SNAP, dias=14)
    assert apagados == [extrair.caminho_bronze("sets", "2026-09-01")]
    assert (Path(cfg.lake) / extrair.caminho_bronze("sets", SNAP)).exists()
