import time
import gzip
import psycopg2
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

DB_CONFIG = {
    "host": "db",
    "dbname": os.getenv("POSTGRES_DB", "imdb_raw"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

FILES = {
    "stg_title_basics":     "/imdb_data/title.basics.tsv.gz",
    "stg_name_basics":      "/imdb_data/name.basics.tsv.gz",
    "stg_title_crew":       "/imdb_data/title.crew.tsv.gz",
    "stg_title_episode":    "/imdb_data/title.episode.tsv.gz",
    "stg_title_principals": "/imdb_data/title.principals.tsv.gz",
    "stg_title_ratings":    "/imdb_data/title.ratings.tsv.gz",
}


def wait_for_db():
    print("Waiting for database...")
    while True:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.close()
            print("Database is ready!")
            return
        except psycopg2.OperationalError:
            time.sleep(2)


def load_table(table_name, filepath):
    """
    Load one TSV.gz file into a staging table using PostgreSQL COPY.
    Each call opens its own connection so tables can be loaded in parallel.
    COPY streams data directly into Postgres — no Python row loop needed.
    """
    t0 = time.time()
    print(f"[RAW] Loading {table_name}...")
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with gzip.open(filepath, "rt", encoding="utf-8") as f:
            # Consume header to get column names; f is now positioned at row 1
            header  = f.readline().rstrip("\n")
            columns = [col.lower() for col in header.split("\t")]
            cols_ddl = ", ".join(f'"{c}" TEXT' for c in columns)

            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
                cur.execute(f"CREATE TABLE {table_name} ({cols_ddl})")
                # FORMAT TEXT = tab-separated; \N = NULL (IMDb default)
                cur.copy_expert(
                    f"COPY {table_name} FROM STDIN (FORMAT TEXT, NULL '\\N')",
                    f,
                )
            conn.commit()

        print(f"[RAW] Done: {table_name} in {time.time() - t0:.0f}s")
    except Exception as e:
        print(f"[RAW] Error loading {table_name}: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def main():
    wait_for_db()

    # All 6 tables are independent — load them in parallel
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(load_table, name, path): name
            for name, path in FILES.items()
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                future.result()
            except Exception as exc:
                print(f"[RAW] {name} failed: {exc}")

    print("[RAW] All tables loaded!")


if __name__ == "__main__":
    main()
