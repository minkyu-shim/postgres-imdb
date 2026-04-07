-- =============================================================================
-- Test: imdb_3nf import validation
-- Run against: imdb_3nf database
-- Usage: psql -d imdb_3nf -f sql/test_3nf.sql
-- =============================================================================

\echo '=== 3NF IMPORT TESTS ==='

-- 1. All expected tables must exist
\echo ''
\echo '--- [1] Table existence ---'
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN (
      'title_type', 'genres', 'profession', 'category',
      'titles', 'people',
      'title_genres', 'person_profession',
      'ratings', 'episodes', 'title_cast', 'title_crew',
      'title_display', 'person_display'
  )
ORDER BY table_name;
-- Expected: 14 rows

-- 2. Row counts — all core tables must be > 0
\echo ''
\echo '--- [2] Row counts (must all be > 0) ---'
SELECT 'title_type'        AS tbl, COUNT(*) AS rows FROM title_type
UNION ALL
SELECT 'genres',                   COUNT(*) FROM genres
UNION ALL
SELECT 'profession',               COUNT(*) FROM profession
UNION ALL
SELECT 'category',                 COUNT(*) FROM category
UNION ALL
SELECT 'titles',                   COUNT(*) FROM titles
UNION ALL
SELECT 'people',                   COUNT(*) FROM people
UNION ALL
SELECT 'title_genres',             COUNT(*) FROM title_genres
UNION ALL
SELECT 'person_profession',        COUNT(*) FROM person_profession
UNION ALL
SELECT 'ratings',                  COUNT(*) FROM ratings
UNION ALL
SELECT 'episodes',                 COUNT(*) FROM episodes
UNION ALL
SELECT 'title_cast',               COUNT(*) FROM title_cast
UNION ALL
SELECT 'title_crew',               COUNT(*) FROM title_crew
UNION ALL
SELECT 'title_display',            COUNT(*) FROM title_display
UNION ALL
SELECT 'person_display',           COUNT(*) FROM person_display
ORDER BY tbl;

-- 3. Referential integrity spot-check
\echo ''
\echo '--- [3] Orphan titles in title_genres (must be 0) ---'
SELECT COUNT(*) AS orphan_title_genres
FROM title_genres tg
LEFT JOIN titles t ON t.tconst = tg.tconst
WHERE t.tconst IS NULL;

\echo ''
\echo '--- [3] Orphan people in person_profession (must be 0) ---'
SELECT COUNT(*) AS orphan_person_profession
FROM person_profession pp
LEFT JOIN people p ON p.nconst = pp.nconst
WHERE p.nconst IS NULL;

\echo ''
\echo '--- [3] Orphan title_crew entries without matching titles (must be 0) ---'
SELECT COUNT(*) AS orphan_title_crew
FROM title_crew tc
LEFT JOIN titles t ON t.tconst = tc.tconst
WHERE t.tconst IS NULL;

-- 4. title_crew roles must only be 'director' or 'writer'
\echo ''
\echo '--- [4] title_crew role values (must be only director/writer) ---'
SELECT role, COUNT(*) AS cnt
FROM title_crew
GROUP BY role
ORDER BY role;

-- 5. Lookup table sizes
\echo ''
\echo '--- [5] Lookup table sizes ---'
SELECT 'title_type' AS tbl, COUNT(*) AS distinct_values FROM title_type
UNION ALL
SELECT 'genres',             COUNT(*) FROM genres
UNION ALL
SELECT 'profession',         COUNT(*) FROM profession
UNION ALL
SELECT 'category',           COUNT(*) FROM category
ORDER BY tbl;

-- 6. ratings range sanity (1.0 – 10.0)
\echo ''
\echo '--- [6] Rating range (must be between 1.0 and 10.0) ---'
SELECT MIN(average_rating) AS min_rating,
       MAX(average_rating) AS max_rating,
       COUNT(*)            AS total_ratings
FROM ratings;

-- 7. Display tables populated (JSONB / array columns non-trivially filled)
\echo ''
\echo '--- [7] title_display: rows with genres array non-empty ---'
SELECT COUNT(*) AS with_genres
FROM title_display
WHERE array_length(genres, 1) > 0;

\echo ''
\echo '--- [7] person_display: rows with filmography entries ---'
SELECT COUNT(*) AS with_filmography
FROM person_display
WHERE jsonb_array_length(filmography) > 0;

-- 8. Sample data
\echo ''
\echo '--- [8] Sample: titles (5 rows) ---'
SELECT t.tconst, t.primary_title, tt.type_name, t.start_year
FROM titles t
JOIN title_type tt ON tt.type_id = t.type_id
LIMIT 5;

\echo ''
\echo '--- [8] Sample: title_display with cast (3 rows) ---'
SELECT tconst, primary_title, title_type, genres, average_rating, top_cast
FROM title_display
WHERE top_cast IS NOT NULL
LIMIT 3;

\echo ''
\echo '=== 3NF TESTS DONE ==='
