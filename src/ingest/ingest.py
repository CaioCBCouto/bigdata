from __future__ import annotations
import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BRONZE_DIR = os.path.join(BASE_DIR, "dados", "bronze")
RAW_DIR = os.path.join(BASE_DIR, "dados", "raw")

os.makedirs(BRONZE_DIR, exist_ok=True)

def ingest_movies():
    df = pd.read_csv(f"{RAW_DIR}/movies.csv", low_memory=False)
    df.to_parquet(f"{BRONZE_DIR}/movies_raw.parquet", index=False)
    print(f"[bronze] movies_raw.parquet: {len(df):,} linhas")

def ingest_ratings(batch_size: int = 0):
    src = f"{RAW_DIR}/ratings.csv"
    if batch_size and batch_size > 0:
        iter_csv = pd.read_csv(src, chunksize=batch_size)
        out = []
        for i, chunk in enumerate(iter_csv, start=1):
            out.append(chunk)
            print(f"[bronze] micro-lote {i}: {len(chunk):,} linhas")
        df = pd.concat(out, ignore_index=True)
    else:
        df = pd.read_csv(src, low_memory=False)
    df.to_parquet(f"{BRONZE_DIR}/ratings_raw.parquet", index=False)
    print(f"[bronze] ratings_raw.parquet: {len(df):,} linhas")

if __name__ == "__main__":
    ingest_movies()
    ingest_ratings(batch_size=100000)
