"""Etapa 1 - Extração: Scryfall (bulk data + /sets) -> camada bronze do lake (JSONL.gz por dia)."""

from __future__ import annotations

import gzip
import json
import logging
import shutil
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .armazenamento import Lake
from .config import Config, salvar_estado

log = logging.getLogger(__name__)

SCRYFALL_API = "https://api.scryfall.com"
BULK_TYPES = ("oracle_cards", "default_cards")  # cartas únicas / todas as impressões
DATASETS = (*BULK_TYPES, "sets")


def caminho_bronze(dataset: str, snapshot_date: str) -> str:
    # Particionamento estilo Hive (dt=...): legível por DuckDB, Spark, Athena etc.
    return f"raw/scryfall/{dataset}/dt={snapshot_date}/{dataset}.jsonl.gz"


def criar_sessao(cfg: Config) -> requests.Session:
    sessao = requests.Session()
    # A Scryfall exige User-Agent e Accept em todas as requisições
    sessao.headers.update({"User-Agent": cfg.user_agent, "Accept": "application/json"})
    retry = Retry(total=5, backoff_factor=5, status_forcelist=(429, 500, 502, 503, 504))
    sessao.mount("https://", HTTPAdapter(max_retries=retry))
    return sessao


def obter_metadados(sessao: requests.Session) -> dict:
    """Consulta /bulk-data e define a data do snapshot (data UTC em que a Scryfall gerou o arquivo)."""
    resp = sessao.get(f"{SCRYFALL_API}/bulk-data", timeout=60)
    resp.raise_for_status()
    itens = {item["type"]: item for item in resp.json()["data"]}

    arquivos = {
        tipo: {"url": itens[tipo]["jsonl_download_uri"], "updated_at": itens[tipo]["updated_at"]} for tipo in BULK_TYPES
    }
    atualizado_em = datetime.fromisoformat(arquivos["default_cards"]["updated_at"]).astimezone(UTC)
    idade_horas = (datetime.now(UTC) - atualizado_em).total_seconds() / 3600
    if idade_horas > 30:
        log.warning("Bulk data da Scryfall tem %.1f horas; os preços podem estar desatualizados.", idade_horas)

    return {"snapshot_date": atualizado_em.date().isoformat(), "arquivos": arquivos}


def validar_jsonl_gz(caminho: Path) -> int:
    """Confere se o arquivo é um gzip íntegro com um JSON por linha. Retorna o nº de linhas."""
    linhas = 0
    with gzip.open(caminho, "rt", encoding="utf-8") as f:
        for linha in f:
            if linhas == 0:
                json.loads(linha)
            linhas += 1
    if linhas == 0:
        raise ValueError(f"Arquivo {caminho} está vazio")
    return linhas


def baixar_bulk(sessao: requests.Session, url: str, destino: Path) -> int:
    """Baixa o JSONL.gz em streaming (sem carregar tudo na memória) e valida."""
    with sessao.get(url, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        resp.raw.decode_content = False  # mantém o .gz exatamente como veio
        with open(destino, "wb") as f:
            shutil.copyfileobj(resp.raw, f, length=1024 * 1024)
    return validar_jsonl_gz(destino)


def baixar_sets(sessao: requests.Session, destino: Path) -> int:
    """Baixa a lista de sets (/sets, com paginação) e grava como JSONL.gz."""
    sets, url = [], f"{SCRYFALL_API}/sets"
    while url:
        resp = sessao.get(url, timeout=60)
        resp.raise_for_status()
        corpo = resp.json()
        sets.extend(corpo["data"])
        url = corpo.get("next_page") if corpo.get("has_more") else None
        time.sleep(0.1)  # regra da Scryfall: 50-100 ms entre requisições
    with gzip.open(destino, "wt", encoding="utf-8") as gz:
        for s in sets:
            gz.write(json.dumps(s, ensure_ascii=False) + "\n")
    return len(sets)


def executar(cfg: Config, forcar: bool = False) -> str:
    lake = Lake(cfg)
    sessao = criar_sessao(cfg)
    meta = obter_metadados(sessao)
    snap = meta["snapshot_date"]
    log.info("Snapshot da Scryfall: %s", snap)

    with tempfile.TemporaryDirectory() as tmp:
        for dataset in DATASETS:
            rel = caminho_bronze(dataset, snap)
            if lake.existe(rel) and not forcar:
                # Reexecução no mesmo dia: o arquivo do snapshot não muda, não precisa baixar de novo
                log.info("%s: já existe no lake (%s), pulando", dataset, rel)
                continue
            local = Path(tmp) / f"{dataset}.jsonl.gz"
            if dataset in BULK_TYPES:
                linhas = baixar_bulk(sessao, meta["arquivos"][dataset]["url"], local)
            else:
                linhas = baixar_sets(sessao, local)
            log.info("%s: %s registros, %.1f MB -> %s", dataset, f"{linhas:,}", local.stat().st_size / 1e6, rel)
            lake.enviar(local, rel)

    salvar_estado(cfg, snapshot_date=snap, extraido_em=datetime.now(UTC).isoformat(timespec="seconds"))
    return snap
