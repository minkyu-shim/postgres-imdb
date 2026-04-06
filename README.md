# IMDb PostgreSQL — Lab Setup

Loads the IMDb non-commercial dataset into three PostgreSQL databases:

| Database | Schema | Purpose |
|---|---|---|
| `imdb_raw` | Flat `stg_*` staging tables | Direct import from TSV files (Lab 1) |
| `imdb_3nf` | Normalised 3NF tables | OLTP — website/API queries (Lab 3) |
| `imdb_star` | Star schema | OLAP — analytics queries (Lab 3) |

---

## Prerequisites

- Docker and Docker Compose
- ~60 GB free disk space (raw data + three databases)

---

## 1. Download IMDb data

Create the data directory and download all six dataset files from [datasets.imdbws.com](https://datasets.imdbws.com/):

```bash
mkdir -p imdb_db
cd imdb_db
curl -O https://datasets.imdbws.com/title.basics.tsv.gz
curl -O https://datasets.imdbws.com/title.ratings.tsv.gz
curl -O https://datasets.imdbws.com/name.basics.tsv.gz
curl -O https://datasets.imdbws.com/title.principals.tsv.gz
curl -O https://datasets.imdbws.com/title.episode.tsv.gz
curl -O https://datasets.imdbws.com/title.crew.tsv.gz
cd ..
```

Keep the files **compressed** (`.tsv.gz`) — the loader reads them directly.

---

## 2. Configure credentials

```bash
cp .env.example .env
```

Edit `.env` and fill in your values:

```env
POSTGRES_USER=imdb
POSTGRES_PASSWORD=your_password_here
POSTGRES_DB=imdb_raw
```

---

## 3. Run

```bash
docker compose up --build
```

This will:
1. Start PostgreSQL and create the three databases (`imdb_raw`, `imdb_3nf`, `imdb_star`)
2. Wait for the database to be ready
3. Run the loader which sequentially:
   - Imports raw TSV data into `imdb_raw` (`stg_*` tables) — **~5–15 min** (parallel COPY)
   - Transforms into the 3NF schema in `imdb_3nf` — **~30–60 min**
   - Transforms into the star schema in `imdb_star` — **~20–40 min**

To run in the background and follow the loader logs:

```bash
docker compose up --build -d
docker compose logs -f loader
```

---

## 4. Connect to the databases

Once the stack is up, connect with `psql` directly from the host:

```bash
# Raw staging tables
psql -h localhost -U imdb -d imdb_raw

# 3NF schema (OLTP)
psql -h localhost -U imdb -d imdb_3nf

# Star schema (OLAP)
psql -h localhost -U imdb -d imdb_star
```

Or open a shell inside the running container:

```bash
docker exec -it imdb-db-1 bash
docker exec -it imdb-db-1 psql -U imdb -d imdb_3nf
```

---

## 5. Verify the load

Run in `imdb_raw`:

```sql
SELECT 'stg_title_basics'    AS table_name, COUNT(*) FROM stg_title_basics
UNION ALL SELECT 'stg_title_ratings',  COUNT(*) FROM stg_title_ratings
UNION ALL SELECT 'stg_name_basics',    COUNT(*) FROM stg_name_basics
UNION ALL SELECT 'stg_title_principals', COUNT(*) FROM stg_title_principals
UNION ALL SELECT 'stg_title_episode',  COUNT(*) FROM stg_title_episode
UNION ALL SELECT 'stg_title_crew',     COUNT(*) FROM stg_title_crew
ORDER BY table_name;
```

Run in `imdb_3nf`:

```sql
SELECT 'titles'       AS t, COUNT(*) FROM titles
UNION ALL SELECT 'people',         COUNT(*) FROM people
UNION ALL SELECT 'ratings',        COUNT(*) FROM ratings
UNION ALL SELECT 'title_cast',     COUNT(*) FROM title_cast
UNION ALL SELECT 'title_crew',     COUNT(*) FROM title_crew
UNION ALL SELECT 'title_genres',   COUNT(*) FROM title_genres
UNION ALL SELECT 'episodes',       COUNT(*) FROM episodes
UNION ALL SELECT 'title_display',  COUNT(*) FROM title_display
UNION ALL SELECT 'person_display', COUNT(*) FROM person_display
ORDER BY t;
```

Run in `imdb_star`:

```sql
SELECT 'fact_title'          AS t, COUNT(*) FROM fact_title
UNION ALL SELECT 'dim_person',          COUNT(*) FROM dim_person
UNION ALL SELECT 'dim_genre',           COUNT(*) FROM dim_genre
UNION ALL SELECT 'dim_time',            COUNT(*) FROM dim_time
UNION ALL SELECT 'dim_title_type',      COUNT(*) FROM dim_title_type
UNION ALL SELECT 'bridge_title_genre',  COUNT(*) FROM bridge_title_genre
UNION ALL SELECT 'bridge_title_person', COUNT(*) FROM bridge_title_person
ORDER BY t;
```

---

## Useful commands

```bash
# Stop everything (keeps data volumes)
docker compose down

# Stop and wipe all data (start fresh)
docker compose down -v

# Rebuild loader image only (after code changes)
docker compose build loader

# Re-run loader against existing database
docker compose run --rm loader

# Check loader exit status
docker compose ps loader
```

---

## Project structure

```
.
├── docker-compose.yml       # Postgres + loader services
├── Dockerfile               # Loader image (python:3.11)
├── .env.example             # Credentials template
├── imdb_db/                 # Put .tsv.gz files here (git-ignored)
├── init/
│   └── 01_create_databases.sql   # Creates imdb_raw, imdb_3nf, imdb_star
└── python_part/
    ├── main.py              # Orchestrator — runs all three loaders in sequence
    ├── load.py              # Raw import → imdb_raw (stg_* tables)
    ├── load_3nf.py          # 3NF transform → imdb_3nf
    ├── load_star.py         # Star schema transform → imdb_star
    └── requirements.txt
```
