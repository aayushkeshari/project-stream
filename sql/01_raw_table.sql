USE WAREHOUSE DE_WH;
USE DATABASE NETFLIX_DE;
USE SCHEMA RAW;

CREATE OR REPLACE TABLE RAW.NETFLIX_TITLES_RAW (
  show_id STRING,
  type STRING,
  title STRING,
  director STRING,
  cast STRING,
  country STRING,
  date_added STRING,
  release_year INTEGER,
  rating STRING,
  duration STRING,
  listed_in STRING,
  description STRING
);
