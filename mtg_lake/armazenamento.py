"""Acesso ao data lake: o mesmo código funciona com Cloudflare R2, AWS S3 ou uma pasta local."""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb

from .config import Config


class Lake:
    """Operações de arquivo no lake. Caminhos `rel` são relativos à raiz do lake (ex.: "gold/dim_cards.parquet")."""

    def __init__(self, cfg: Config):
        self.url = cfg.lake
        self.remoto = cfg.lake_remoto
        if self.remoto:
            import boto3

            self.bucket, _, prefixo = self.url[len("s3://") :].partition("/")
            self.prefixo = prefixo.strip("/")
            protocolo = "https" if cfg.s3_use_ssl else "http"
            endpoint = None if cfg.s3_endpoint == "s3.amazonaws.com" else f"{protocolo}://{cfg.s3_endpoint}"
            self._s3 = boto3.client(
                "s3",
                endpoint_url=endpoint,
                region_name=cfg.s3_region,
                aws_access_key_id=cfg.s3_access_key_id or None,
                aws_secret_access_key=cfg.s3_secret_access_key or None,
            )
        else:
            self.raiz = Path(self.url)

    # --- caminhos -------------------------------------------------------------------
    def uri(self, rel: str) -> str:
        """Caminho completo, no formato que o DuckDB entende."""
        return f"{self.url}/{rel}"

    def _key(self, rel: str) -> str:
        return f"{self.prefixo}/{rel}" if self.prefixo else rel

    def _rel(self, key: str) -> str:
        return key[len(self.prefixo) + 1 :] if self.prefixo else key

    def preparar_destino(self, rel: str) -> None:
        """O COPY do DuckDB não cria pastas locais; no S3/R2 não há pastas."""
        if not self.remoto:
            (self.raiz / rel).parent.mkdir(parents=True, exist_ok=True)

    # --- operações ------------------------------------------------------------------
    def enviar(self, arquivo_local: Path, rel: str) -> None:
        if self.remoto:
            self._s3.upload_file(str(arquivo_local), self.bucket, self._key(rel))
        else:
            self.preparar_destino(rel)
            shutil.copyfile(arquivo_local, self.raiz / rel)

    def existe(self, rel: str) -> bool:
        if self.remoto:
            resp = self._s3.list_objects_v2(Bucket=self.bucket, Prefix=self._key(rel), MaxKeys=1)
            return any(o["Key"] == self._key(rel) for o in resp.get("Contents", []))
        return (self.raiz / rel).is_file()

    def listar(self, prefixo: str) -> list[str]:
        if self.remoto:
            chaves = []
            for pagina in self._s3.get_paginator("list_objects_v2").paginate(
                Bucket=self.bucket, Prefix=self._key(prefixo)
            ):
                chaves += [self._rel(o["Key"]) for o in pagina.get("Contents", [])]
            return sorted(chaves)
        base = self.raiz / prefixo
        if not base.exists():
            return []
        return sorted(p.relative_to(self.raiz).as_posix() for p in base.rglob("*") if p.is_file())

    def apagar(self, rel: str) -> None:
        if self.remoto:
            self._s3.delete_object(Bucket=self.bucket, Key=self._key(rel))
        else:
            (self.raiz / rel).unlink(missing_ok=True)

    def escrever_texto(self, rel: str, texto: str, content_type: str = "application/json") -> None:
        if self.remoto:
            self._s3.put_object(
                Bucket=self.bucket,
                Key=self._key(rel),
                Body=texto.encode("utf-8"),
                ContentType=content_type,
                CacheControl="no-cache",  # o Power BI precisa sempre da versão mais recente
            )
        else:
            self.preparar_destino(rel)
            (self.raiz / rel).write_text(texto, encoding="utf-8")


def _sql_str(valor: str) -> str:
    return "'" + valor.replace("'", "''") + "'"


def conectar(cfg: Config, banco: str | Path = ":memory:") -> duckdb.DuckDBPyConnection:
    """Conexão DuckDB já configurada para ler/escrever no lake."""
    con = duckdb.connect(str(banco))
    if cfg.lake_remoto:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(
            f"""
            CREATE OR REPLACE SECRET lake (
                TYPE s3,
                KEY_ID {_sql_str(cfg.s3_access_key_id)},
                SECRET {_sql_str(cfg.s3_secret_access_key)},
                REGION {_sql_str(cfg.s3_region)},
                ENDPOINT {_sql_str(cfg.s3_endpoint)},
                URL_STYLE {_sql_str(cfg.s3_url_style)},
                USE_SSL {str(cfg.s3_use_ssl).lower()}
            )
            """
        )
    return con
