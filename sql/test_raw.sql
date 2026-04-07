-- =============================================================================
-- Test: imdb_raw import validation
-- Run against: imdb_raw database
-- Usage: psql -d imdb_raw -f sql/test_raw.sql
-- =============================================================================

\echo '=== RAW IMPORT TESTS ==='

-- 1. All 6 staging tables must exist
\echo ''
\echo '--- [1] Table existence ---'
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN (
      'stg_title_basics',
      'stg_name_basics',
      'stg_title_crew',
      'stg_title_episode',
      'stg_title_principals',
      'stg_title_ratings'
  )
ORDER BY table_name;
-- Expected: 6 rows

-- 2. Row counts — must all be > 0
\echo ''
\echo '--- [2] Row counts (must all be > 0) ---'
SELECT 'stg_title_basics'     AS tbl, COUNT(*) AS rows FROM stg_title_basics
UNION ALL
SELECT 'stg_name_basics',              COUNT(*) FROM stg_name_basics
UNION ALL
SELECT 'stg_title_crew',               COUNT(*) FROM stg_title_crew
UNION ALL
SELECT 'stg_title_episode',            COUNT(*) FROM stg_title_episode
UNION ALL
SELECT 'stg_title_principals',         COUNT(*) FROM stg_title_principals
UNION ALL
SELECT 'stg_title_ratings',            COUNT(*) FROM stg_title_ratings
ORDER BY tbl;

-- 3. Expected columns present in key tables
\echo ''
\echo '--- [3] Key columns in stg_title_basics ---'
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'stg_title_basics'
ORDER BY ordinal_position;
-- Expected: tconst, titletype, primarytitle, originaltitle, isadult,
--           startyear, endyear, runtimeminutes, genres

-- 4. No completely NULL tconst values (primary identifier must be present)
\echo ''
\echo '--- [4] NULL tconst check (must be 0) ---'
SELECT 'stg_title_basics'     AS tbl, COUNT(*) AS null_tconst FROM stg_title_basics     WHERE tconst IS NULL
UNION ALL
SELECT 'stg_title_ratings',           COUNT(*) FROM stg_title_ratings    WHERE tconst IS NULL
UNION ALL
SELECT 'stg_title_principals',        COUNT(*) FROM stg_title_principals WHERE tconst IS NULL;

-- 5. Sample rows
\echo ''
\echo '--- [5] Sample: stg_title_basics (5 rows) ---'
SELECT tconst, titletype, primarytitle, startyear, genres
FROM stg_title_basics
LIMIT 5;

\echo ''
\echo '--- [5] Sample: stg_title_ratings (5 rows) ---'
SELECT tconst, averagerating, numvotes
FROM stg_title_ratings
LIMIT 5;

-- 6. Distinct title types loaded (sanity check on genre/type variety)
\echo ''
\echo '--- [6] Distinct title types ---'
SELECT titletype, COUNT(*) AS cnt
FROM stg_title_basics
WHERE titletype IS NOT NULL
GROUP BY titletype
ORDER BY cnt DESC;

\echo ''
\echo '=== RAW TESTS DONE ==='
