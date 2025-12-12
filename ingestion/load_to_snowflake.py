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
        schema="RAW",
    )

def main():
    local_path = os.path.abspath("data/raw/netflix_titles.csv")

    with connect() as cn:
        cs = cn.cursor()
        try:
            def run_sql_file(cs, path: str):
                sql_text = open(path, "r").read()
                statements = [s.strip() for s in sql_text.split(";") if s.strip()]
                for stmt in statements:
                    cs.execute(stmt)

            run_sql_file(cs, "sql/01_raw_table.sql")


            cs.execute(
                f"PUT file://{local_path} @NETFLIX_DE.RAW.NETFLIX_STAGE "
                "AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
            )

            run_sql_file(cs, "sql/02_load_raw.sql")

            cs.execute("SELECT COUNT(*) FROM RAW.NETFLIX_TITLES_RAW")
            print("RAW rowcount:", cs.fetchone()[0])
        finally:
            cs.close()

if __name__ == "__main__":
    main()
