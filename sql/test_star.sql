-- =============================================================================
-- Test: imdb_star import validation
-- Run against: imdb_star database
-- Usage: psql -d imdb_star -f sql/test_star.sql
-- =============================================================================

\echo '=== STAR SCHEMA IMPORT TESTS ==='

-- 1. All expected tables must exist
\echo ''
\echo '--- [1] Table existence ---'
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN (
      'dim_time', 'dim_genre', 'dim_title_type', 'dim_person',
      'fact_title',
      'bridge_title_genre', 'bridge_title_person'
  )
ORDER BY table_name;
-- Expected: 7 rows

-- 2. Row counts — all tables must be > 0
\echo ''
\echo '--- [2] Row counts (must all be > 0) ---'
SELECT 'dim_time'             AS tbl, COUNT(*) AS rows FROM dim_time
UNION ALL
SELECT 'dim_genre',                   COUNT(*) FROM dim_genre
UNION ALL
SELECT 'dim_title_type',              COUNT(*) FROM dim_title_type
UNION ALL
SELECT 'dim_person',                  COUNT(*) FROM dim_person
UNION ALL
SELECT 'fact_title',                  COUNT(*) FROM fact_title
UNION ALL
SELECT 'bridge_title_genre',          COUNT(*) FROM bridge_title_genre
UNION ALL
SELECT 'bridge_title_person',         COUNT(*) FROM bridge_title_person
ORDER BY tbl;

-- 3. Referential integrity spot-checks
\echo ''
\echo '--- [3] Orphan fact_title rows (no matching dim_title_type) ---'
SELECT COUNT(*) AS orphan_title_type
FROM fact_title ft
LEFT JOIN dim_title_type dtt ON dtt.title_type_key = ft.title_type_key
WHERE ft.title_type_key IS NOT NULL
  AND dtt.title_type_key IS NULL;

\echo ''
\echo '--- [3] Orphan fact_title rows (no matching dim_time) ---'
SELECT COUNT(*) AS orphan_time
FROM fact_title ft
LEFT JOIN dim_time dt ON dt.time_key = ft.time_key
WHERE ft.time_key IS NOT NULL
  AND dt.time_key IS NULL;

\echo ''
\echo '--- [3] Orphan bridge_title_genre rows (must be 0) ---'
SELECT COUNT(*) AS orphan_bridge_genre
FROM bridge_title_genre bg
LEFT JOIN fact_title ft ON ft.title_key = bg.title_key
WHERE ft.title_key IS NULL;

\echo ''
\echo '--- [3] Orphan bridge_title_person rows (must be 0) ---'
SELECT COUNT(*) AS orphan_bridge_person
FROM bridge_title_person bp
LEFT JOIN dim_person dp ON dp.person_key = bp.person_key
WHERE dp.person_key IS NULL;

-- 4. dim_time: decade/century derived values are consistent
\echo ''
\echo '--- [4] dim_time consistency (year must equal decade + offset, century = year/100) ---'
SELECT COUNT(*) AS inconsistent_decade
FROM dim_time
WHERE decade != (year / 10) * 10;

SELECT COUNT(*) AS inconsistent_century
FROM dim_time
WHERE century != year / 100;

-- 5. Pre-aggregated counts in fact_title are set (not all NULL)
\echo ''
\echo '--- [5] Pre-aggregated counts coverage ---'
SELECT
    COUNT(*)                             AS total_titles,
    COUNT(cast_count)                    AS with_cast_count,
    COUNT(genre_count)                   AS with_genre_count,
    ROUND(100.0 * COUNT(cast_count)  / NULLIF(COUNT(*), 0), 1) AS pct_cast,
    ROUND(100.0 * COUNT(genre_count) / NULLIF(COUNT(*), 0), 1) AS pct_genre
FROM fact_title;

-- 6. Rating sanity (same as raw; must be 1.0–10.0)
\echo ''
\echo '--- [6] Rating range in fact_title (1.0–10.0) ---'
SELECT MIN(average_rating) AS min_rating,
       MAX(average_rating) AS max_rating,
       COUNT(average_rating) AS total_rated
FROM fact_title;

-- 7. bridge_title_person role categories (no unexpected values)
\echo ''
\echo '--- [7] role_category distribution in bridge_title_person ---'
SELECT role_category, COUNT(*) AS cnt
FROM bridge_title_person
GROUP BY role_category
ORDER BY cnt DESC;

-- 8. dim_time decade labels look correct
\echo ''
\echo '--- [8] Sample dim_time rows ---'
SELECT time_key, year, decade, century, decade_label
FROM dim_time
ORDER BY year
LIMIT 10;

-- 9. dim_person is_alive flag: people with a death_year should not be alive
\echo ''
\echo '--- [9] Inconsistent is_alive flag (must be 0) ---'
SELECT COUNT(*) AS inconsistent_alive
FROM dim_person
WHERE death_year IS NOT NULL
  AND is_alive = TRUE;

-- 10. Sample fact join
\echo ''
\echo '--- [10] Sample: top-rated titles with type and decade ---'
SELECT ft.tconst,
       dtt.title_type,
       dt.year,
       dt.decade_label,
       ft.average_rating,
       ft.num_votes,
       ft.cast_count,
       ft.genre_count
FROM fact_title ft
JOIN dim_title_type dtt ON dtt.title_type_key = ft.title_type_key
JOIN dim_time       dt  ON dt.time_key         = ft.time_key
WHERE ft.average_rating IS NOT NULL
  AND ft.num_votes > 10000
ORDER BY ft.average_rating DESC, ft.num_votes DESC
LIMIT 10;

\echo ''
\echo '=== STAR TESTS DONE ==='
