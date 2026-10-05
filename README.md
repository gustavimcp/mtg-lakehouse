# MTG Lakehouse

Pipeline diário de dados de Magic: The Gathering, sem servidor e sem custo fixo:
**Scryfall → GitHub Actions → Cloudflare R2 → dbt + DuckDB → Parquet → Power BI (MTG_2)**.

- **Extração** diária do *bulk data* da Scryfall, agendada no GitHub Actions (não depende do seu PC ligado)
- **Data lake** no Cloudflare R2 (compatível com S3, sem custo de saída): bronze em JSONL.gz, gold em Parquet
- **Transformação** com dbt sobre DuckDB, com 33 testes de qualidade; se algum falhar, nada é publicado
- **Histórico de preços** particionado por mês, idempotente, com importação dos CSVs antigos
- **Power BI** lê os Parquet por HTTPS, sem gateway

Passo a passo completo, incluindo a comparação com a versão Airflow + S3 + Snowflake: **[docs/GUIA.md](docs/GUIA.md)**.

## Começo rápido (local, sem nuvem)

```powershell
git clone https://github.com/gustavimcp/mtg-lakehouse.git C:\projetos\mtg_lakehouse
cd C:\projetos\mtg_lakehouse
py -3.12 -m venv .venv
.venv\Scripts\python -m pip install -r requirements-dev.txt
copy .env.example .env
.venv\Scripts\python -m mtg_lake diario     # resultado em data\lake\gold\
.venv\Scripts\python -m pytest
```

No Linux/macOS, use `python3 -m venv .venv` e `.venv/bin/python`.

## Comandos

| Comando | Etapa |
|---|---|
| `python -m mtg_lake diario` | tudo abaixo, em sequência |
| `python -m mtg_lake extrair [--forcar]` | Scryfall → bronze |
| `python -m mtg_lake transformar [--select ...]` | `dbt build` (modelos + testes) |
| `python -m mtg_lake publicar` | gold → Parquet + `manifest.json` |
| `python -m mtg_lake limpar [--dias 14]` | retenção do bronze |
| `python -m mtg_lake legado --arquivo X.csv --fato fct_card_prices` | importa histórico dos CSVs antigos |
