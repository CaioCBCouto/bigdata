from __future__ import annotations
import os
import sys
from typing import Any, Dict, List
import pandas as pd
import numpy as np

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, BASE_DIR)

from src.utils.utils import parse_list, parse_dict, to_bool

BRONZE_DIR = os.path.join(BASE_DIR, "dados", "bronze")
SILVER_DIR = os.path.join(BASE_DIR, "dados", "silver")

os.makedirs(SILVER_DIR, exist_ok=True)

def clean_movies() -> None:
    df = pd.read_parquet(f"{BRONZE_DIR}/movies_raw.parquet")

    if "adult" in df.columns:
        df["adult"] = df["adult"].apply(to_bool)
    if "video" in df.columns:
        df["video"] = df["video"].apply(to_bool)

    if "release_date" in df.columns:
        df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")

    numeric_cols = [
        "budget", "revenue", "runtime", "popularity",
        "vote_average", "vote_count"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "original_language" in df.columns:
        df["original_language"] = (
            df["original_language"].astype("string").str.strip().str.lower()
        )

    if "genres" in df.columns:
        df["genres_list"] = df["genres"].apply(parse_list)
    else:
        df["genres_list"] = [[] for _ in range(len(df))]

    if "belongs_to_collection" in df.columns:
        df["collection_obj"] = df["belongs_to_collection"].apply(parse_dict)
        df["collection_id"] = df["collection_obj"].apply(
            lambda d: d.get("id") if isinstance(d, dict) else None
        )
        df["collection_name"] = df["collection_obj"].apply(
            lambda d: d.get("name") if isinstance(d, dict) else None
        )
        df["collection_poster_path"] = df["collection_obj"].apply(
            lambda d: d.get("poster_path") if isinstance(d, dict) else None
        )
        df["collection_backdrop_path"] = df["collection_obj"].apply(
            lambda d: d.get("backdrop_path") if isinstance(d, dict) else None
        )

    for col in ["production_companies", "production_countries", "spoken_languages"]:
        if col in df.columns:
            df[col] = df[col].apply(parse_list)

    keep = [
        "id", "imdb_id",
        "title", "original_title", "original_language",
        "overview", "tagline", "status", "homepage",
        "release_date", "runtime",
        "budget", "revenue", "popularity", "vote_average", "vote_count",
        "adult", "video",
        "genres_list",
        "production_companies", "production_countries", "spoken_languages",
        "collection_id", "collection_name",
    ]
    present = [c for c in keep if c in df.columns]
    df = df[present].drop_duplicates(subset=["id"]).reset_index(drop=True)

    df.to_parquet(f"{SILVER_DIR}/movies_clean.parquet", index=False)
    print(f"[silver] movies_clean.parquet: {len(df):,} linhas")

def clean_ratings() -> None:
    df = pd.read_parquet(f"{BRONZE_DIR}/ratings_raw.parquet")

    for c in ["userId", "movieId"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    if "rating" in df.columns:
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")

    if "timestamp" in df.columns:
        df["ts"] = pd.to_datetime(df["timestamp"], unit="s", errors="coerce")
    elif "ts" not in df.columns:
        df["ts"] = pd.NaT

    need_cols = {"userId", "movieId", "rating"}
    if need_cols.issubset(df.columns):
        df = df.dropna(subset=["userId", "movieId", "rating"])
        df = df.query("rating >= 0 and rating <= 5")

    df.to_parquet(f"{SILVER_DIR}/ratings_clean.parquet", index=False)
    print(f"[silver] ratings_clean.parquet: {len(df):,} linhas")

if __name__ == "__main__":
    clean_movies()
    clean_ratings()