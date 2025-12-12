USE WAREHOUSE DE_WH;
USE DATABASE NETFLIX_DE;
USE SCHEMA ANALYTICS;

-- Clean “silver” table
CREATE OR REPLACE TABLE TITLES_CLEAN AS
SELECT
  TRIM(show_id) AS show_id,
  TRIM(type) AS type,
  TRIM(title) AS title,
  NULLIF(TRIM(director), '') AS director,
  NULLIF(TRIM("CAST"), '') AS cast,
  NULLIF(TRIM(country), '') AS country,
  TRY_TO_DATE(date_added, 'MMMM DD, YYYY') AS date_added,
  release_year,
  NULLIF(TRIM(rating), '') AS rating,
  NULLIF(TRIM(duration), '') AS duration,
  NULLIF(TRIM(listed_in), '') AS listed_in,
  NULLIF(TRIM(description), '') AS description
FROM NETFLIX_DE.RAW.NETFLIX_TITLES_RAW;

-- Bridge table: one row per (show_id, genre)
CREATE OR REPLACE TABLE TITLE_GENRES AS
SELECT
  show_id,
  TRIM(value::string) AS genre
FROM TITLES_CLEAN,
LATERAL SPLIT_TO_TABLE(listed_in, ',');

-- Bridge table: one row per (show_id, country) (filters blanks)
CREATE OR REPLACE TABLE TITLE_COUNTRIES AS
SELECT
  show_id,
  c AS country
FROM (
  SELECT
    show_id,
    NULLIF(TRIM(value::string), '') AS c
  FROM TITLES_CLEAN,
  LATERAL SPLIT_TO_TABLE(country, ',')
)
WHERE c IS NOT NULL;

-- KPI table: titles by year and type
CREATE OR REPLACE TABLE YEARLY_COUNTS AS
SELECT
  release_year,
  type,
  COUNT(*) AS titles
FROM TITLES_CLEAN
GROUP BY 1, 2;
