"""
Orchestrator: loads all three IMDB databases in sequence.
  1. imdb_raw  — flat TEXT tables, direct from TSV files
  2. imdb_3nf  — 3NF normalised schema (OLTP)
  3. imdb_star — star schema (OLAP)
"""

import time
import psycopg2
import os
import load
import load_3nf
import load_star


DB_CONFIG = {
    "host": "db",
    "dbname": os.getenv("POSTGRES_DB", "imdb_raw"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}


def wait_for_db():
    print("Waiting for Postgres…")
    while True:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.close()
            print("Postgres is ready.")
            return
        except psycopg2.OperationalError:
            time.sleep(2)


def timed(label, fn):
    t0 = time.time()
    fn()
    print(f"{label} completed in {time.time() - t0:.0f}s\n")


if __name__ == "__main__":
    wait_for_db()
    timed("=== Raw load",      load.main)
    timed("=== 3NF transform", load_3nf.main)
    timed("=== Star transform", load_star.main)
    print("All databases loaded successfully.")
