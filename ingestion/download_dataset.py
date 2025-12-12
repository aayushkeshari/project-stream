import os
import requests

# Real dataset file (netflix_titles.csv)
ZENODO_FILE_URL = "https://zenodo.org/records/13925131/files/netflix_titles.csv?download=1"

def main():
    os.makedirs("data/raw", exist_ok=True)
    out_path = "data/raw/netflix_titles.csv"

    r = requests.get(ZENODO_FILE_URL, timeout=60)
    r.raise_for_status()

    with open(out_path, "wb") as f:
        f.write(r.content)

    print(f"Saved: {out_path} ({len(r.content):,} bytes)")

if __name__ == "__main__":
    main()
