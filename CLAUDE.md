# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview
Migration of a monthly margin report from SQL Server (T-SQL) to PySpark on Delta Lake, targeting AWS Glue 5.1.

## Tech Stack
- **Language:** Python 3.11
- **Engine:** PySpark 3.5.6
- **Storage:** Delta Lake 3.3.2
- **Environment:** AWS Glue 5.1 (local development via PySpark)

## Common Commands
- **Setup Environment:**
  ```bash
  python3.11 -m venv .venv && source .venv/bin/activate
  pip install pyspark==3.5.6 delta-spark==3.3.2
  ```
- **Run Pipeline:** (Once implemented) `python src/main.py --lote <1|2> --input_path <path> --output_path <path>`

## Architecture & Data Flow
- **Source:** CSV files in `datos/lote_1/` and `datos/lote_2/`.
- **Processing:** 
  - Initial load (Lote 1) $\rightarrow$ Delta Tables.
  - Incremental load (Lote 2) $\rightarrow$ `MERGE` into Delta Tables.
  - Business Logic: Re-implementation of `legado/usp_margen_mensual.sql` using Spark DataFrame API or Spark SQL.
- **Output:** CSV files in `salida/` matching the schema: `mes, id_tienda, categoria, unidades, tickets, unidades_por_ticket, venta_neta, costo, margen, margen_pct`.

## Key Constraints
- **Upsert Logic:** For each table, if a primary key is duplicated, the row with the most recent `fecha_actualizacion` prevails.
- **Parity:** The final report must match `legado/salida/rpt_margen_mensual_loteX.csv` exactly, or differences must be explained in `CONCILIACION.md`.
- **Idempotency:** Re-running the same lot should not alter the results.
