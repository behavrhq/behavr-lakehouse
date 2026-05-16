-- Unity Catalog layout (run once per workspace / metastore with appropriate permissions).
-- Replace locations with your managed/external storage as required.

CREATE CATALOG IF NOT EXISTS behavr;

CREATE SCHEMA IF NOT EXISTS behavr.bronze;
CREATE SCHEMA IF NOT EXISTS behavr.silver;
CREATE SCHEMA IF NOT EXISTS behavr.gold;

-- Tables are created by pipelines on first run (bronze streaming, silver MERGE, gold MERGE).
-- Grant ownership and privileges to pipeline service principals in production.
