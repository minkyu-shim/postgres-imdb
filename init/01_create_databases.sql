-- Raw data (flat TEXT tables, loaded directly from TSV files)
SELECT 'CREATE DATABASE imdb_raw'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'imdb_raw') \gexec

-- 3NF schema (OLTP)
SELECT 'CREATE DATABASE imdb_3nf'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'imdb_3nf') \gexec

-- Star schema (OLAP)
SELECT 'CREATE DATABASE imdb_star'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'imdb_star') \gexec
