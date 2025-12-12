import os
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

def connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE"),
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema="ANALYTICS",
    )

def fetch_one(cs, sql: str):
    cs.execute(sql)
    return cs.fetchone()[0]

def check(name: str, ok: bool, details: str = ""):
    if ok:
        print(f"PASSED ✅ {name}")
    else:
        raise AssertionError(f"FAILED ❌ {name} {details}".strip())

def main():
    with connect() as cn:
        cs = cn.cursor()
        try:
            raw = fetch_one(cs, "SELECT COUNT(*) FROM NETFLIX_DE.RAW.NETFLIX_TITLES_RAW")
            clean = fetch_one(cs, "SELECT COUNT(*) FROM NETFLIX_DE.ANALYTICS.TITLES_CLEAN")
            genres = fetch_one(cs, "SELECT COUNT(*) FROM NETFLIX_DE.ANALYTICS.TITLE_GENRES")
            countries = fetch_one(cs, "SELECT COUNT(*) FROM NETFLIX_DE.ANALYTICS.TITLE_COUNTRIES")
            yearly = fetch_one(cs, "SELECT COUNT(*) FROM NETFLIX_DE.ANALYTICS.YEARLY_COUNTS")

            # 1) Rowcount sanity
            check("RAW rowcount > 0", raw > 0, f"(raw={raw})")
            check("TITLES_CLEAN rowcount equals RAW rowcount", clean == raw, f"(raw={raw}, clean={clean})")

            # 2) Primary key-ish checks
            null_show_id = fetch_one(cs, "SELECT COUNT(*) FROM NETFLIX_DE.ANALYTICS.TITLES_CLEAN WHERE show_id IS NULL")
            check("No NULL show_id in TITLES_CLEAN", null_show_id == 0, f"(null_show_id={null_show_id})")

            dup_show_id = fetch_one(cs, """
                SELECT COUNT(*)
                FROM (
                  SELECT show_id
                  FROM NETFLIX_DE.ANALYTICS.TITLES_CLEAN
                  GROUP BY show_id
                  HAVING COUNT(*) > 1
                )
            """)
            check("No duplicate show_id in TITLES_CLEAN", dup_show_id == 0, f"(dup_show_id_groups={dup_show_id})")

            # 3) Domain/value checks
            invalid_type = fetch_one(cs, """
                SELECT COUNT(*)
                FROM NETFLIX_DE.ANALYTICS.TITLES_CLEAN
                WHERE type NOT IN ('Movie', 'TV Show') OR type IS NULL
            """)
            check("type is only Movie/TV Show (and not NULL)", invalid_type == 0, f"(invalid_type={invalid_type})")

            bad_years = fetch_one(cs, """
                SELECT COUNT(*)
                FROM NETFLIX_DE.ANALYTICS.TITLES_CLEAN
                WHERE release_year IS NULL OR release_year < 1900 OR release_year > YEAR(CURRENT_DATE()) + 1
            """)
            check("release_year is within a reasonable range", bad_years == 0, f"(bad_years={bad_years})")

            # 4) Bridge tables should be populated (not tiny)
            check("TITLE_GENRES populated", genres > clean, f"(genres={genres}, clean={clean})")
            check("TITLE_COUNTRIES populated (can be smaller than clean, but should be > 0)", countries > 0, f"(countries={countries})")
            check("YEARLY_COUNTS populated", yearly > 0, f"(yearly={yearly})")

            # 5) No blank strings after splitting
            blank_genres = fetch_one(cs, "SELECT COUNT(*) FROM NETFLIX_DE.ANALYTICS.TITLE_GENRES WHERE genre IS NULL OR genre = ''")
            check("No blank genres after split", blank_genres == 0, f"(blank_genres={blank_genres})")

            blank_countries = fetch_one(cs, "SELECT COUNT(*) FROM NETFLIX_DE.ANALYTICS.TITLE_COUNTRIES WHERE country IS NULL OR country = ''")
            check("No blank countries after split", blank_countries == 0, f"(blank_countries={blank_countries})")

            print("\n🎉 All data quality checks passed.")
        finally:
            cs.close()

if __name__ == "__main__":
    main()
