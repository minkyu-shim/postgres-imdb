"""
Transform raw IMDB flat tables → 3NF schema in imdb_3nf database.

Tables
------
Lookup  : title_type, genres, profession, category
Core    : titles, people
Junction: title_genres, person_profession, title_crew
Dependent: ratings, episodes, title_cast
Display : title_display, person_display
"""

import psycopg2
import psycopg2.extras
import os

BATCH = 10_000
NULL = r"\N"


# ── connections ──────────────────────────────────────────────────────────────

def raw_conn():
    return psycopg2.connect(
        host="db",
        dbname=os.getenv("POSTGRES_DB", "imdb_raw"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def nf3_conn():
    return psycopg2.connect(
        host="db",
        dbname="imdb_3nf",
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


# ── value helpers ─────────────────────────────────────────────────────────────

def n(v):    return None if (v is None or v == NULL) else v
def ni(v):   x = n(v); return int(x)   if x else None
def nf(v):   x = n(v); return float(x) if x else None
def nb(v):   x = n(v); return bool(int(x)) if x else None


# ── DDL ───────────────────────────────────────────────────────────────────────

SCHEMA = """
SET session_replication_role = 'replica';

DROP TABLE IF EXISTS person_display    CASCADE;
DROP TABLE IF EXISTS title_display     CASCADE;
DROP TABLE IF EXISTS title_crew        CASCADE;
DROP TABLE IF EXISTS title_cast        CASCADE;
DROP TABLE IF EXISTS person_profession CASCADE;
DROP TABLE IF EXISTS title_genres      CASCADE;
DROP TABLE IF EXISTS ratings           CASCADE;
DROP TABLE IF EXISTS episodes          CASCADE;
DROP TABLE IF EXISTS titles            CASCADE;
DROP TABLE IF EXISTS people            CASCADE;
DROP TABLE IF EXISTS title_type        CASCADE;
DROP TABLE IF EXISTS genres            CASCADE;
DROP TABLE IF EXISTS profession        CASCADE;
DROP TABLE IF EXISTS category          CASCADE;

CREATE TABLE title_type (
    type_id   SERIAL PRIMARY KEY,
    type_name TEXT   UNIQUE NOT NULL
);

CREATE TABLE genres (
    genre_id   SERIAL PRIMARY KEY,
    genre_name TEXT   UNIQUE NOT NULL
);

CREATE TABLE profession (
    profession_id   SERIAL PRIMARY KEY,
    profession_name TEXT   UNIQUE NOT NULL
);

CREATE TABLE category (
    category_id   SERIAL PRIMARY KEY,
    category_name TEXT   UNIQUE NOT NULL
);

CREATE TABLE titles (
    tconst          TEXT    PRIMARY KEY,
    type_id         INT     REFERENCES title_type(type_id),
    primary_title   TEXT,
    original_title  TEXT,
    is_adult        BOOLEAN,
    start_year      SMALLINT,
    end_year        SMALLINT,
    runtime_minutes INT
);

CREATE TABLE title_genres (
    tconst   TEXT REFERENCES titles(tconst),
    genre_id INT  REFERENCES genres(genre_id),
    PRIMARY KEY (tconst, genre_id)
);

CREATE TABLE ratings (
    tconst         TEXT    PRIMARY KEY REFERENCES titles(tconst),
    average_rating NUMERIC(3,1),
    num_votes      INT
);

CREATE TABLE episodes (
    tconst         TEXT PRIMARY KEY REFERENCES titles(tconst),
    parent_tconst  TEXT REFERENCES titles(tconst),
    season_number  SMALLINT,
    episode_number INT
);

CREATE TABLE people (
    nconst       TEXT    PRIMARY KEY,
    primary_name TEXT,
    birth_year   SMALLINT,
    death_year   SMALLINT
);

CREATE TABLE person_profession (
    nconst        TEXT REFERENCES people(nconst),
    profession_id INT  REFERENCES profession(profession_id),
    PRIMARY KEY (nconst, profession_id)
);

CREATE TABLE title_cast (
    tconst      TEXT     REFERENCES titles(tconst),
    ordering    SMALLINT,
    nconst      TEXT     REFERENCES people(nconst),
    category_id INT      REFERENCES category(category_id),
    job         TEXT,
    characters  TEXT,
    PRIMARY KEY (tconst, ordering)
);

-- Directors and writers per title, combined with a role discriminator
CREATE TABLE title_crew (
    tconst TEXT        REFERENCES titles(tconst),
    nconst TEXT        REFERENCES people(nconst),
    role   VARCHAR(10) NOT NULL,   -- 'director' or 'writer'
    PRIMARY KEY (tconst, nconst, role)
);

-- Denormalized display table for fast title page queries (section 1.3)
CREATE TABLE title_display (
    tconst          VARCHAR(12) PRIMARY KEY,
    primary_title   TEXT        NOT NULL,
    original_title  TEXT,
    title_type      VARCHAR(20),
    start_year      INTEGER,
    end_year        INTEGER,
    runtime_minutes INTEGER,
    is_adult        BOOLEAN,
    genres          TEXT[],
    average_rating  DECIMAL(3,1),
    num_votes       INTEGER,
    top_cast        JSONB,
    directors       TEXT[],
    writers         TEXT[]
);

-- Denormalized display table for fast person page queries (section 1.3)
CREATE TABLE person_display (
    nconst              VARCHAR(12) PRIMARY KEY,
    primary_name        TEXT        NOT NULL,
    birth_year          INTEGER,
    death_year          INTEGER,
    primary_professions TEXT[],
    filmography         JSONB,
    known_for           JSONB
);
"""


# ── helpers ───────────────────────────────────────────────────────────────────

def build_lookup(cur, table, id_col, name_col, items):
    """Insert unique items into a lookup table; return {name: id} dict."""
    psycopg2.extras.execute_batch(
        cur,
        f"INSERT INTO {table}({name_col}) VALUES (%s) ON CONFLICT DO NOTHING",
        [(v,) for v in items],
        page_size=1000,
    )
    cur.execute(f"SELECT {name_col}, {id_col} FROM {table}")
    return dict(cur.fetchall())


def stream(conn, query, cursor_name):
    """Server-side cursor generator — yields rows without loading all into memory."""
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
    dst = nf3_conn()

    print("[3NF] Creating schema…")
    with dst.cursor() as cur:
        cur.execute(SCHEMA)
    dst.commit()

    # ── lookup tables ──────────────────────────────────────────────────────────

    print("[3NF] title_type…")
    with raw.cursor() as cur:
        cur.execute("SELECT DISTINCT titletype FROM stg_title_basics WHERE titletype IS NOT NULL")
        types = [r[0] for r in cur.fetchall()]
    with dst.cursor() as cur:
        type_map = build_lookup(cur, "title_type", "type_id", "type_name", types)
    dst.commit()

    print("[3NF] genres…")
    with raw.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT unnest(string_to_array(genres, ',')) "
            "FROM stg_title_basics WHERE genres IS NOT NULL"
        )
        genre_list = [r[0] for r in cur.fetchall()]
    with dst.cursor() as cur:
        genre_map = build_lookup(cur, "genres", "genre_id", "genre_name", genre_list)
    dst.commit()

    print("[3NF] profession…")
    with raw.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT unnest(string_to_array(primaryprofession, ',')) "
            "FROM stg_name_basics WHERE primaryprofession IS NOT NULL"
        )
        professions = [r[0] for r in cur.fetchall()]
    with dst.cursor() as cur:
        prof_map = build_lookup(cur, "profession", "profession_id", "profession_name", professions)
    dst.commit()

    print("[3NF] category…")
    with raw.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT category FROM stg_title_principals WHERE category IS NOT NULL"
        )
        cats = [r[0] for r in cur.fetchall()]
    with dst.cursor() as cur:
        cat_map = build_lookup(cur, "category", "category_id", "category_name", cats)
    dst.commit()

    # ── core tables ────────────────────────────────────────────────────────────

    print("[3NF] titles…")
    def title_rows():
        for tc, tt, pt, ot, ia, sy, ey, rm in stream(
            raw,
            "SELECT tconst,titletype,primarytitle,originaltitle,"
            "isadult,startyear,endyear,runtimeminutes FROM stg_title_basics",
            "cur_title",
        ):
            yield (n(tc), type_map.get(n(tt)), n(pt), n(ot), nb(ia), ni(sy), ni(ey), ni(rm))

    c = bulk_insert(
        dst,
        "INSERT INTO titles(tconst,type_id,primary_title,original_title,"
        "is_adult,start_year,end_year,runtime_minutes) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        title_rows(),
    )
    print(f"  → {c:,} rows")

    print("[3NF] people…")
    def person_rows():
        for nc, pn, by, dy in stream(
            raw,
            "SELECT nconst,primaryname,birthyear,deathyear FROM stg_name_basics",
            "cur_person",
        ):
            yield (n(nc), n(pn), ni(by), ni(dy))

    c = bulk_insert(
        dst,
        "INSERT INTO people(nconst,primary_name,birth_year,death_year) "
        "VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        person_rows(),
    )
    print(f"  → {c:,} rows")

    # ── junction / dependent tables ────────────────────────────────────────────

    print("[3NF] title_genres…")
    def tg_rows():
        for tc, g in stream(
            raw,
            "SELECT tconst, unnest(string_to_array(genres, ',')) "
            "FROM stg_title_basics WHERE genres IS NOT NULL",
            "cur_tg",
        ):
            gid = genre_map.get(g)
            if gid:
                yield (tc, gid)

    c = bulk_insert(
        dst,
        "INSERT INTO title_genres(tconst,genre_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
        tg_rows(),
    )
    print(f"  → {c:,} rows")

    print("[3NF] ratings…")
    def rating_rows():
        for tc, ar, nv in stream(
            raw,
            "SELECT tconst,averagerating,numvotes FROM stg_title_ratings",
            "cur_rat",
        ):
            yield (n(tc), nf(ar), ni(nv))

    c = bulk_insert(
        dst,
        "INSERT INTO ratings(tconst,average_rating,num_votes) "
        "VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
        rating_rows(),
    )
    print(f"  → {c:,} rows")

    print("[3NF] episodes…")
    def episode_rows():
        for tc, pt, sn, en in stream(
            raw,
            "SELECT tconst,parenttconst,seasonnumber,episodenumber FROM stg_title_episode",
            "cur_ep",
        ):
            yield (n(tc), n(pt), ni(sn), ni(en))

    c = bulk_insert(
        dst,
        "INSERT INTO episodes(tconst,parent_tconst,season_number,episode_number) "
        "VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        episode_rows(),
    )
    print(f"  → {c:,} rows")

    print("[3NF] person_profession…")
    def pp_rows():
        for nc, p in stream(
            raw,
            "SELECT nconst, unnest(string_to_array(primaryprofession, ',')) "
            "FROM stg_name_basics WHERE primaryprofession IS NOT NULL",
            "cur_pp",
        ):
            pid = prof_map.get(p)
            if pid:
                yield (nc, pid)

    c = bulk_insert(
        dst,
        "INSERT INTO person_profession(nconst,profession_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
        pp_rows(),
    )
    print(f"  → {c:,} rows")

    print("[3NF] title_cast…")
    def cast_rows():
        for tc, o, nc, cat, job, chars in stream(
            raw,
            "SELECT tconst,ordering,nconst,category,job,characters FROM stg_title_principals",
            "cur_cast",
        ):
            cid = cat_map.get(n(cat))
            yield (n(tc), ni(o), n(nc), cid, n(job), n(chars))

    c = bulk_insert(
        dst,
        "INSERT INTO title_cast(tconst,ordering,nconst,category_id,job,characters) "
        "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        cast_rows(),
    )
    print(f"  → {c:,} rows")

    print("[3NF] title_crew (directors)…")
    def director_rows():
        for tc, nc in stream(
            raw,
            "SELECT tconst, unnest(string_to_array(directors, ',')) "
            "FROM stg_title_crew WHERE directors IS NOT NULL",
            "cur_dir",
        ):
            if nc:
                yield (tc, nc, 'director')

    c = bulk_insert(
        dst,
        "INSERT INTO title_crew(tconst,nconst,role) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
        director_rows(),
    )
    print(f"  → {c:,} rows")

    print("[3NF] title_crew (writers)…")
    def writer_rows():
        for tc, nc in stream(
            raw,
            "SELECT tconst, unnest(string_to_array(writers, ',')) "
            "FROM stg_title_crew WHERE writers IS NOT NULL",
            "cur_wr",
        ):
            if nc:
                yield (tc, nc, 'writer')

    c = bulk_insert(
        dst,
        "INSERT INTO title_crew(tconst,nconst,role) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
        writer_rows(),
    )
    print(f"  → {c:,} rows")

    # ── display tables (section 1.3) ───────────────────────────────────────────

    print("[3NF] title_display…")
    exec_sql(dst, """
        INSERT INTO title_display (
            tconst, primary_title, original_title, title_type,
            start_year, end_year, runtime_minutes, is_adult,
            genres, average_rating, num_votes,
            top_cast, directors, writers
        )
        SELECT
            t.tconst,
            t.primary_title,
            t.original_title,
            tt.type_name,
            t.start_year,
            t.end_year,
            t.runtime_minutes,
            t.is_adult,
            COALESCE(g.genres, ARRAY[]::TEXT[]),
            r.average_rating,
            r.num_votes,
            tc.top_cast,
            COALESCE(cr_d.directors, ARRAY[]::TEXT[]),
            COALESCE(cr_w.writers, ARRAY[]::TEXT[])
        FROM titles t
        LEFT JOIN title_type tt ON tt.type_id = t.type_id
        LEFT JOIN ratings r ON r.tconst = t.tconst
        LEFT JOIN (
            SELECT tg.tconst,
                   array_agg(gn.genre_name ORDER BY gn.genre_name) AS genres
            FROM title_genres tg
            JOIN genres gn ON gn.genre_id = tg.genre_id
            GROUP BY tg.tconst
        ) g ON g.tconst = t.tconst
        LEFT JOIN (
            SELECT sub.tconst,
                   jsonb_agg(
                       jsonb_build_object(
                           'name', sub.primary_name,
                           'role', sub.characters,
                           'ordering', sub.ordering
                       ) ORDER BY sub.ordering
                   ) AS top_cast
            FROM (
                SELECT tc.tconst, tc.ordering, tc.characters,
                       p.primary_name,
                       ROW_NUMBER() OVER (PARTITION BY tc.tconst ORDER BY tc.ordering) AS rn
                FROM title_cast tc
                JOIN people p ON p.nconst = tc.nconst
            ) sub
            WHERE sub.rn <= 10
            GROUP BY sub.tconst
        ) tc ON tc.tconst = t.tconst
        LEFT JOIN (
            SELECT cr.tconst, array_agg(p.primary_name) AS directors
            FROM title_crew cr
            JOIN people p ON p.nconst = cr.nconst
            WHERE cr.role = 'director'
            GROUP BY cr.tconst
        ) cr_d ON cr_d.tconst = t.tconst
        LEFT JOIN (
            SELECT cr.tconst, array_agg(p.primary_name) AS writers
            FROM title_crew cr
            JOIN people p ON p.nconst = cr.nconst
            WHERE cr.role = 'writer'
            GROUP BY cr.tconst
        ) cr_w ON cr_w.tconst = t.tconst
        WHERE t.primary_title IS NOT NULL
        ON CONFLICT DO NOTHING
    """)
    print("  done")

    print("[3NF] person_display…")
    exec_sql(dst, """
        INSERT INTO person_display (
            nconst, primary_name, birth_year, death_year,
            primary_professions, filmography, known_for
        )
        SELECT
            p.nconst,
            p.primary_name,
            p.birth_year,
            p.death_year,
            COALESCE(pp.professions, ARRAY[]::TEXT[]),
            COALESCE(fm.filmography, '[]'::JSONB),
            COALESCE(kf.known_for, '[]'::JSONB)
        FROM people p
        LEFT JOIN (
            SELECT pp.nconst,
                   array_agg(pr.profession_name ORDER BY pr.profession_name) AS professions
            FROM person_profession pp
            JOIN profession pr ON pr.profession_id = pp.profession_id
            GROUP BY pp.nconst
        ) pp ON pp.nconst = p.nconst
        LEFT JOIN (
            SELECT tc.nconst,
                   jsonb_agg(
                       jsonb_build_object(
                           'tconst', t.tconst,
                           'title', t.primary_title,
                           'year', t.start_year,
                           'role', tc.characters,
                           'rating', r.average_rating
                       ) ORDER BY t.start_year DESC NULLS LAST
                   ) AS filmography
            FROM title_cast tc
            JOIN titles t ON t.tconst = tc.tconst
            LEFT JOIN ratings r ON r.tconst = t.tconst
            GROUP BY tc.nconst
        ) fm ON fm.nconst = p.nconst
        LEFT JOIN (
            SELECT sub.nconst,
                   jsonb_agg(
                       jsonb_build_object(
                           'tconst', sub.tconst,
                           'title', sub.primary_title,
                           'year', sub.start_year,
                           'rating', sub.average_rating
                       ) ORDER BY sub.num_votes DESC NULLS LAST
                   ) AS known_for
            FROM (
                SELECT tc.nconst, t.tconst, t.primary_title, t.start_year,
                       r.average_rating, r.num_votes,
                       ROW_NUMBER() OVER (
                           PARTITION BY tc.nconst
                           ORDER BY COALESCE(r.num_votes, 0) DESC
                       ) AS rn
                FROM title_cast tc
                JOIN titles t ON t.tconst = tc.tconst
                LEFT JOIN ratings r ON r.tconst = t.tconst
            ) sub
            WHERE sub.rn <= 4
            GROUP BY sub.nconst
        ) kf ON kf.nconst = p.nconst
        WHERE p.primary_name IS NOT NULL
        ON CONFLICT DO NOTHING
    """)
    print("  done")

    with dst.cursor() as cur:
        cur.execute("SET session_replication_role = 'origin'")
    dst.commit()

    raw.close()
    dst.close()
    print("[3NF] Done!")


if __name__ == "__main__":
    main()
