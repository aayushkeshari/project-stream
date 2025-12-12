-- Top genres
SELECT genre, COUNT(*) AS titles
FROM NETFLIX_DE.ANALYTICS.TITLE_GENRES
GROUP BY 1
ORDER BY titles DESC
LIMIT 10;

-- Titles by year and type
SELECT release_year, type, titles
FROM NETFLIX_DE.ANALYTICS.YEARLY_COUNTS
ORDER BY release_year DESC, type;

-- Top countries (from clean bridge table)
SELECT country, COUNT(*) AS titles
FROM NETFLIX_DE.ANALYTICS.TITLE_COUNTRIES
GROUP BY 1
ORDER BY titles DESC
LIMIT 10;
