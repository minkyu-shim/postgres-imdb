"""
Transform raw IMDB flat tables → Star schema in imdb_star database.

Schema
------
Dimensions : dim_title_type, dim_genre, dim_time, dim_person
Bridge     : bridge_title_genre, bridge_title_person
Fact       : fact_title
"""

import psycopg2
import psycopg2.extras
import os

BATCH = 10_000
NULL = r"\N"


# ── connections ───────────────────────────────────────────────────────────────

def raw_conn():
    return psycopg2.connect(
        host="db",
        dbname=os.getenv("POSTGRES_DB", "imdb_raw"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def star_conn():
    return psycopg2.connect(
        host="db",
        dbname="imdb_star",
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


# ── value helpers ─────────────────────────────────────────────────────────────

def n(v):  return None if (v is None or v == NULL) else v
def ni(v): x = n(v); return int(x)   if x else None
def nf(v): x = n(v); return float(x) if x else None
def nb(v): x = n(v); return bool(int(x)) if x else None


# ── DDL ───────────────────────────────────────────────────────────────────────

SCHEMA = """
SET session_replication_role = 'replica';

DROP TABLE IF EXISTS bridge_title_person CASCADE;
DROP TABLE IF EXISTS bridge_title_genre  CASCADE;
DROP TABLE IF EXISTS fact_title          CASCADE;
DROP TABLE IF EXISTS dim_person          CASCADE;
DROP TABLE IF EXISTS dim_title_type      CASCADE;
DROP TABLE IF EXISTS dim_genre           CASCADE;
DROP TABLE IF EXISTS dim_time            CASCADE;

CREATE TABLE dim_time (
    time_key     SERIAL PRIMARY KEY,
    year         INTEGER,
    decade       INTEGER,       -- e.g. 1990, 2000
    century      INTEGER,       -- e.g. 19, 20, 21
    decade_label VARCHAR(10)    -- e.g. '1990s', '2000s'
);

CREATE TABLE dim_genre (
    genre_key  SERIAL PRIMARY KEY,
    genre_name VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE dim_title_type (
    title_type_key SERIAL PRIMARY KEY,
    title_type     VARCHAR(20) UNIQUE NOT NULL
);

CREATE TABLE dim_person (
    person_key         SERIAL PRIMARY KEY,
    nconst             VARCHAR(12) UNIQUE NOT NULL,
    primary_name       TEXT,
    birth_year         INTEGER,
    birth_decade       INTEGER,
    death_year         INTEGER,
    is_alive           BOOLEAN,
    primary_profession TEXT        -- kept as comma-separated string for filtering
);

CREATE TABLE fact_title (
    title_key       SERIAL PRIMARY KEY,
    tconst          VARCHAR(12) UNIQUE NOT NULL,
    title_type_key  INTEGER REFERENCES dim_title_type(title_type_key),
    time_key        INTEGER REFERENCES dim_time(time_key),
    -- Measures
    runtime_minutes INTEGER,
    average_rating  DECIMAL(3,1),
    num_votes       INTEGER,
    is_adult        BOOLEAN,
    -- Pre-aggregated
    cast_count      INTEGER,    -- number of cast members
    genre_count     INTEGER     -- number of genres
);

CREATE TABLE bridge_title_genre (
    title_key INTEGER REFERENCES fact_title(title_key),
    genre_key INTEGER REFERENCES dim_genre(genre_key),
    PRIMARY KEY (title_key, genre_key)
);

CREATE TABLE bridge_title_person (
    title_key     INTEGER     REFERENCES fact_title(title_key),
    person_key    INTEGER     REFERENCES dim_person(person_key),
    role_category VARCHAR(50),    -- 'actor', 'actress', 'director', 'writer', etc.
    ordering      INTEGER,
    PRIMARY KEY (title_key, person_key, role_category)
);
"""


# ── helpers ───────────────────────────────────────────────────────────────────

def build_lookup(cur, table, id_col, name_col, items):
    psycopg2.extras.execute_batch(
        cur,
        f"INSERT INTO {table}({name_col}) VALUES (%s) ON CONFLICT DO NOTHING",
        [(v,) for v in items],
        page_size=1000,
    )
    cur.execute(f"SELECT {name_col}, {id_col} FROM {table}")
    return dict(cur.fetchall())


def stream(conn, query, cursor_name):
    cur = conn.cursor(cursor_name)
    cur.execute(query)
    while True:
        rows = cur.fetchmany(BATCH)
        if not rows:
            break
        yield from rows
    cur.close()


def bulk_insert(dst, sql, rows, commit=True):
    inserted = 0
    with dst.cursor() as cur:
        batch = []
        for row in rows:
            batch.append(row)
            if len(batch) == BATCH:
                psycopg2.extras.execute_batch(cur, sql, batch, page_size=BATCH)
                inserted += len(batch)
                batch = []
        if batch:
            psycopg2.extras.execute_batch(cur, sql, batch, page_size=BATCH)
            inserted += len(batch)
    if commit:
        dst.commit()
    return inserted


def exec_sql(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    raw = raw_conn()
    dst = star_conn()

    print("[STAR] Creating schema…")
    with dst.cursor() as cur:
        cur.execute(SCHEMA)
    dst.commit()

    # ── dim_title_type ────────────────────────────────────────────────────────
    print("[STAR] dim_title_type…")
    with raw.cursor() as cur:
        cur.execute("SELECT DISTINCT titletype FROM stg_title_basics WHERE titletype IS NOT NULL")
        types = [r[0] for r in cur.fetchall()]
    with dst.cursor() as cur:
        type_map = build_lookup(cur, "dim_title_type", "title_type_key", "title_type", types)
    dst.commit()

    # ── dim_genre ─────────────────────────────────────────────────────────────
    print("[STAR] dim_genre…")
    with raw.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT unnest(string_to_array(genres, ',')) "
            "FROM stg_title_basics WHERE genres IS NOT NULL"
        )
        genres = [r[0] for r in cur.fetchall()]
    with dst.cursor() as cur:
        genre_map = build_lookup(cur, "dim_genre", "genre_key", "genre_name", genres)
    dst.commit()

    # ── dim_time ──────────────────────────────────────────────────────────────
    print("[STAR] dim_time…")
    with raw.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT startyear::int "
            "FROM stg_title_basics WHERE startyear IS NOT NULL ORDER BY 1"
        )
        years = [r[0] for r in cur.fetchall() if r[0]]

    def year_rows():
        for y in years:
            decade = (y // 10) * 10
            century = y // 100
            yield (y, decade, century, f"{decade}s")

    bulk_insert(
        dst,
        "INSERT INTO dim_time(year,decade,century,decade_label) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        year_rows(),
    )

    with dst.cursor() as cur:
        cur.execute("SELECT year, time_key FROM dim_time")
        year_map = dict(cur.fetchall())

    # ── dim_person ────────────────────────────────────────────────────────────
    print("[STAR] dim_person…")
    def person_rows():
        for nc, pn, by, dy, pp in stream(
            raw,
            "SELECT nconst,primaryname,birthyear,deathyear,primaryprofession FROM stg_name_basics",
            "cur_dp",
        ):
            by_int = ni(by)
            dy_int = ni(dy)
            birth_decade = ((by_int // 10) * 10) if by_int else None
            is_alive = dy_int is None
            yield (n(nc), n(pn), by_int, birth_decade, dy_int, is_alive, n(pp))

    c = bulk_insert(
        dst,
        "INSERT INTO dim_person(nconst,primary_name,birth_year,birth_decade,"
        "death_year,is_alive,primary_profession) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        person_rows(),
    )
    print(f"  → {c:,} rows")

    # ── fact_title ────────────────────────────────────────────────────────────
    print("[STAR] Loading ratings index…")
    ratings_map = {}
    with raw.cursor() as cur:
        cur.execute("SELECT tconst, averagerating, numvotes FROM stg_title_ratings")
        for tc, ar, nv in cur.fetchall():
            ratings_map[tc] = (nf(ar), ni(nv))

    print("[STAR] fact_title…")
    def fact_title_rows():
        for tc, tt, ia, sy, rm, gs in stream(
            raw,
            "SELECT tconst,titletype,isadult,startyear,runtimeminutes,genres "
            "FROM stg_title_basics",
            "cur_ft",
        ):
            tc = n(tc)
            if not tc:
                continue
            type_key  = type_map.get(n(tt))
            time_key  = year_map.get(ni(sy))
            ar, nv    = ratings_map.get(tc, (None, None))
            gc        = len(gs.split(',')) if gs else None
            yield (tc, type_key, time_key, ni(rm), ar, nv, nb(ia), gc)

    c = bulk_insert(
        dst,
        "INSERT INTO fact_title(tconst,title_type_key,time_key,runtime_minutes,"
        "average_rating,num_votes,is_adult,genre_count) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        fact_title_rows(),
    )
    print(f"  → {c:,} rows")

    # Build lookup maps needed for bridge tables
    print("[STAR] building tconst→title_key index…")
    tconst_map = {}
    with dst.cursor("cur_tmap") as cur:
        cur.execute("SELECT tconst, title_key FROM fact_title")
        while True:
            rows = cur.fetchmany(50_000)
            if not rows:
                break
            tconst_map.update(rows)

    print("[STAR] building nconst→person_key index…")
    nconst_map = {}
    with dst.cursor("cur_pmap") as cur:
        cur.execute("SELECT nconst, person_key FROM dim_person")
        while True:
            rows = cur.fetchmany(50_000)
            if not rows:
                break
            nconst_map.update(rows)

    # ── bridge_title_genre ────────────────────────────────────────────────────
    print("[STAR] bridge_title_genre…")
    def bridge_genre_rows():
        for tc, g in stream(
            raw,
            "SELECT tconst, unnest(string_to_array(genres, ',')) "
            "FROM stg_title_basics WHERE genres IS NOT NULL",
            "cur_btg",
        ):
            tsk = tconst_map.get(tc)
            gsk = genre_map.get(g)
            if tsk and gsk:
                yield (tsk, gsk)

    c = bulk_insert(
        dst,
        "INSERT INTO bridge_title_genre(title_key,genre_key) VALUES (%s,%s) ON CONFLICT DO NOTHING",
        bridge_genre_rows(),
    )
    print(f"  → {c:,} rows")

    # ── bridge_title_person ───────────────────────────────────────────────────
    # Principals (actors, actresses, self, host, etc.)
    print("[STAR] bridge_title_person (principals)…")
    def principal_rows():
        for tc, o, nc, cat in stream(
            raw,
            "SELECT tconst,ordering,nconst,category FROM stg_title_principals",
            "cur_pr",
        ):
            tsk  = tconst_map.get(n(tc))
            psk  = nconst_map.get(n(nc))
            role = n(cat)
            if tsk and psk and role:
                yield (tsk, psk, role, ni(o))

    c = bulk_insert(
        dst,
        "INSERT INTO bridge_title_person(title_key,person_key,role_category,ordering) "
        "VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        principal_rows(),
    )
    print(f"  → {c:,} rows")

    # Directors from stg_title_crew
    print("[STAR] bridge_title_person (directors)…")
    def director_rows():
        for tc, nc in stream(
            raw,
            "SELECT tconst, unnest(string_to_array(directors, ',')) "
            "FROM stg_title_crew WHERE directors IS NOT NULL",
            "cur_dir",
        ):
            tsk = tconst_map.get(n(tc))
            psk = nconst_map.get(n(nc))
            if tsk and psk:
                yield (tsk, psk, 'director', None)

    c = bulk_insert(
        dst,
        "INSERT INTO bridge_title_person(title_key,person_key,role_category,ordering) "
        "VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        director_rows(),
    )
    print(f"  → {c:,} rows")

    # Writers from stg_title_crew
    print("[STAR] bridge_title_person (writers)…")
    def writer_rows():
        for tc, nc in stream(
            raw,
            "SELECT tconst, unnest(string_to_array(writers, ',')) "
            "FROM stg_title_crew WHERE writers IS NOT NULL",
            "cur_wr",
        ):
            tsk = tconst_map.get(n(tc))
            psk = nconst_map.get(n(nc))
            if tsk and psk:
                yield (tsk, psk, 'writer', None)

    c = bulk_insert(
        dst,
        "INSERT INTO bridge_title_person(title_key,person_key,role_category,ordering) "
        "VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        writer_rows(),
    )
    print(f"  → {c:,} rows")

    # ── update pre-aggregated counts ──────────────────────────────────────────
    print("[STAR] Updating cast_count…")
    exec_sql(dst, """
        UPDATE fact_title ft
        SET cast_count = sub.cnt
        FROM (
            SELECT title_key, COUNT(*) AS cnt
            FROM bridge_title_person
            WHERE role_category IN ('actor', 'actress', 'self', 'host',
                                    'archive_footage', 'archive_sound')
            GROUP BY title_key
        ) sub
        WHERE sub.title_key = ft.title_key
    """)

    print("[STAR] Updating genre_count…")
    exec_sql(dst, """
        UPDATE fact_title ft
        SET genre_count = sub.cnt
        FROM (
            SELECT title_key, COUNT(*) AS cnt
            FROM bridge_title_genre
            GROUP BY title_key
        ) sub
        WHERE sub.title_key = ft.title_key
    """)

    with dst.cursor() as cur:
        cur.execute("SET session_replication_role = 'origin'")
    dst.commit()

    raw.close()
    dst.close()
    print("[STAR] Done!")


if __name__ == "__main__":
    main()
