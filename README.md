# Retail Sales Warehouse

A modern data warehouse solution for retail sales analytics.

## Structure

- `architecture/` - System architecture diagrams
- `sql/` - SQL scripts for bronze, silver, gold layers
- `docs/` - Documentation

## Quick Start

```bash
# Run bronze layer
psql -f sql/01_bronze.sql

# Run silver layer
psql -f sql/02_silver.sql

# Run gold layer
psql -f sql/03_gold.sql
```
