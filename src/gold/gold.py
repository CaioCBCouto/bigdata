from __future__ import annotations
import os
import sys
import pandas as pd
import numpy as np

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, BASE_DIR)

SILVER_DIR = os.path.join(BASE_DIR, "dados", "silver")
GOLD_DIR = os.path.join(BASE_DIR, "dados", "gold")

os.makedirs(GOLD_DIR, exist_ok=True)


def compute_weighted_rating() -> pd.DataFrame:
    """
    Calcula o Weighted Rating para cada filme (IMDB-style), usando a
    fórmula:
         WR = (v/(v+m)) * R + (m/(v+m)) * C
    onde:
         R = média do filme
         v = número de votos do filme
         m = voto mínimo (percentil 80)
         C = média global de todos os filmes
    """
    print("[gold] Lendo movies_clean...")
    df = pd.read_parquet(f"{SILVER_DIR}/movies_clean.parquet")

    if "vote_average" not in df.columns or "vote_count" not in df.columns:
        raise ValueError("Colunas vote_average e vote_count não encontradas em movies_clean.")

    df["vote_average"] = pd.to_numeric(df["vote_average"], errors="coerce")
    df["vote_count"] = pd.to_numeric(df["vote_count"], errors="coerce")

    C = df["vote_average"].mean()

    m = df["vote_count"].quantile(0.80)

    print(f"[gold] Média global (C): {C:.3f}")
    print(f"[gold] Mínimo de votos (m): {m:.1f}")

    qualified = df[df["vote_count"] >= m].copy()

    qualified["weighted_rating"] = (
        (qualified["vote_count"] / (qualified["vote_count"] + m)) * qualified["vote_average"]
        + (m / (qualified["vote_count"] + m)) * C
    )

    print(f"[gold] Filmes qualificados: {len(qualified):,}")

    qualified.to_parquet(f"{GOLD_DIR}/movies_scores.parquet", index=False)
    print(f"[gold] movies_scores.parquet salvo ({len(qualified):,} linhas)")

    return qualified


def compute_user_genre_profiles() -> pd.DataFrame:
    """
    userId | genre | rating_mean | n_ratings
    """
    print("[gold] Lendo ratings_clean e movies_clean...")
    ratings = pd.read_parquet(f"{SILVER_DIR}/ratings_clean.parquet")
    movies = pd.read_parquet(f"{SILVER_DIR}/movies_clean.parquet")

    if "genres_list" not in movies.columns:
        raise ValueError("Coluna genres_list não encontrada em movies_clean.")

    movies_exploded = movies.explode("genres_list", ignore_index=True)
    movies_exploded["genre"] = movies_exploded["genres_list"].apply(
        lambda d: d.get("name") if isinstance(d, dict) else None
    )

    ratings["movieId"] = pd.to_numeric(ratings["movieId"], errors="coerce").astype("Int64")
    movies_exploded["id"] = pd.to_numeric(movies_exploded["id"], errors="coerce").astype("Int64")

    merged = ratings.merge(
        movies_exploded[["id", "genre"]],
        left_on="movieId",
        right_on="id",
        how="left"
    )

    merged = merged.dropna(subset=["genre"])

    profile = (
        merged.groupby(["userId", "genre"])
        .agg(
            rating_mean=("rating", "mean"),
            n_ratings=("rating", "count")
        )
        .reset_index()
    )

    profile.to_parquet(f"{GOLD_DIR}/user_genre_profiles.parquet", index=False)
    print(f"[gold] user_genre_profiles.parquet salvo ({len(profile):,} linhas)")

    return profile

def recommend(user_id: int, n: int = 10) -> pd.DataFrame:

    print(f"[gold] Recomendando para user_id={user_id}...")

    scores = pd.read_parquet(f"{GOLD_DIR}/movies_scores.parquet")
    profiles = pd.read_parquet(f"{GOLD_DIR}/user_genre_profiles.parquet")
    ratings = pd.read_parquet(f"{SILVER_DIR}/ratings_clean.parquet")

    pref = profiles[profiles["userId"] == user_id]
    if pref.empty:
        print("[gold] Usuário não possui histórico suficiente.")
        return pd.DataFrame()

    fav_genre = pref.sort_values("rating_mean", ascending=False).iloc[0]["genre"]
    print(f"[gold] Gênero favorito: {fav_genre}")

    rated_ids = set(ratings[ratings["userId"] == user_id]["movieId"].unique())

    df = pd.read_parquet(f"{SILVER_DIR}/movies_clean.parquet")
    df = df.explode("genres_list", ignore_index=True)
    df["genre"] = df["genres_list"].apply(lambda d: d.get("name") if isinstance(d, dict) else None)

    df = df[df["genre"] == fav_genre]

    df = df.merge(
        scores[["id", "title", "weighted_rating"]],
        left_on="id",
        right_on="id",
        how="inner"
    )

    df = df[~df["id"].isin(rated_ids)]

    df = df.sort_values("weighted_rating", ascending=False)

    return df.head(n)


if __name__ == "__main__":
    print("===== GOLD PIPELINE =====")

    print("\n1) Calculando Weighted Rating...")
    compute_weighted_rating()

    print("\n2) Construindo perfis de usuários por gênero...")
    compute_user_genre_profiles()

    print("\n[gold] Pipeline Gold finalizado com sucesso!")
