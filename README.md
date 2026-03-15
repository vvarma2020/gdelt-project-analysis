# GDELT Geopolitical Risk Graph

Local Python pipeline for building a 7-day trailing geopolitical interaction graph from the GDELT 2.0 feed.

## What it does
- Polls `masterfilelist.txt` for new 15-minute GDELT batches.
- Downloads raw `export`, `mentions`, and `gkg` zip files.
- Stores normalized bronze and filtered silver data locally.
- Aggregates daily country-to-country interactions and risk scores in DuckDB.
- Publishes country interaction projections into Neo4j.

## Commands
- `python -m gdelt_risk_graph backfill --days 7 --workers 4`
- `python -m gdelt_risk_graph poll-once`
- `python -m gdelt_risk_graph rebuild-day --date 2026-03-15`
- `python -m gdelt_risk_graph publish-neo4j --since 2026-03-15`

## Environment
- `GDELT_PROJECT_ROOT` optional project root override.
- `GDELT_DATA_ROOT` optional data directory override.
- `GDELT_MASTERFILE_URL` optional masterfile URL override. Defaults to the GDELT HTTP feed because the current HTTPS certificate for `data.gdeltproject.org` does not validate cleanly.
- `GDELT_DUCKDB_PATH` optional DuckDB path override.
- `GDELT_CHECKPOINT_PATH` optional checkpoint path override.
- `GDELT_CA_BUNDLE_PATH` optional CA bundle override for HTTPS downloads.
- `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`, `NEO4J_DATABASE` for Neo4j publication.

## Layout
- `data/raw/YYYY/MM/DD/*.zip`
- `data/bronze/.../*.parquet`
- `data/silver/.../*.parquet`
- `data/gdelt.duckdb`
- `state/checkpoint.json`
