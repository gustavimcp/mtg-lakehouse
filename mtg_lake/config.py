"""Configuração do pipeline, lida de variáveis de ambiente (ou do arquivo .env na raiz)."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
DBT_DIR = RAIZ / "dbt"

try:  # .env só é usado no desenvolvimento local; no GitHub Actions vêm de secrets/variables
    from dotenv import load_dotenv

    load_dotenv(RAIZ / ".env")
except ImportError:  # pragma: no cover
    pass


def _env(nome: str, padrao: str) -> str:
    # Variável vazia conta como ausente (o GitHub Actions passa "" para variables não cadastradas)
    return os.environ.get(nome) or padrao


def _caminho_absoluto(valor: str) -> Path:
    p = Path(valor)
    return p if p.is_absolute() else (RAIZ / p).resolve()


@dataclass(frozen=True)
class Config:
    lake: str  # "s3://bucket[/prefixo]" ou caminho local (absoluto, com "/")
    duckdb_path: Path
    estado_path: Path
    user_agent: str
    retencao_bronze_dias: int
    s3_endpoint: str
    s3_region: str
    s3_url_style: str
    s3_access_key_id: str
    s3_secret_access_key: str
    s3_use_ssl: bool

    @property
    def lake_remoto(self) -> bool:
        return self.lake.startswith("s3://")


def carregar() -> Config:
    lake = _env("MTG_LAKE", "./data/lake").rstrip("/")
    if not lake.startswith("s3://"):
        lake = _caminho_absoluto(lake).as_posix()  # DuckDB aceita "C:/..." também no Windows

    return Config(
        lake=lake,
        duckdb_path=_caminho_absoluto(_env("MTG_DUCKDB_PATH", "./data/mtg.duckdb")),
        estado_path=_caminho_absoluto(_env("MTG_ESTADO_PATH", "./data/estado.json")),
        user_agent=_env("SCRYFALL_USER_AGENT", "MTGLakehouse/1.0"),
        retencao_bronze_dias=int(_env("MTG_RETENCAO_BRONZE_DIAS", "14")),
        # Cloudflare R2: <account_id>.r2.cloudflarestorage.com / auto / path
        # AWS S3:        s3.amazonaws.com                    / us-east-1 (ou sua região) / vhost
        s3_endpoint=_env("S3_ENDPOINT", "s3.amazonaws.com"),
        s3_region=_env("S3_REGION", "us-east-1"),
        s3_url_style=_env("S3_URL_STYLE", "vhost"),
        s3_access_key_id=_env("S3_ACCESS_KEY_ID", ""),
        s3_secret_access_key=_env("S3_SECRET_ACCESS_KEY", ""),
        s3_use_ssl=_env("S3_USE_SSL", "true").lower() != "false",  # false só para MinIO local
    )


def salvar_estado(cfg: Config, **valores) -> None:
    """Guarda informações entre etapas (ex.: snapshot_date gerado pela extração)."""
    estado = ler_estado(cfg)
    estado.update(valores)
    cfg.estado_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.estado_path.write_text(json.dumps(estado, indent=2), encoding="utf-8")


def ler_estado(cfg: Config) -> dict:
    if cfg.estado_path.exists():
        return json.loads(cfg.estado_path.read_text(encoding="utf-8"))
    return {}


def snapshot_atual(cfg: Config, informado: str | None = None) -> str:
    if informado:
        return informado
    snap = ler_estado(cfg).get("snapshot_date")
    if not snap:
        raise SystemExit(
            "Nenhum snapshot encontrado: rode 'python -m mtg_lake extrair' ou informe --snapshot AAAA-MM-DD"
        )
    return snap
