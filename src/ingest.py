from __future__ import annotations
import os
import pandas as pd

BRONZE_DIR = "bronze"
RAW_DIR = "dados/raw"

os.makedirs(BRONZE_DIR, exist_ok=True)

def ingest_movies():
    df = pd.read_csv(f"{RAW_DIR}/movies.csv", low_memory=False)
    df.to_parquet(f"{BRONZE_DIR}/movies_raw.parquet", index=False)
    print(f"[bronze] movies_raw.parquet: {len(df):,} linhas")

if __name__ == "__main__":
    ingest_movies()