# Recomendador de Filmes — Pipeline (AV1)

## Descrição
Pipeline de dados para preparar `movies.csv` e `ratings.csv` em camadas Bronze/Silver/Gold e gerar um baseline de recomendação.

## Fontes de dados
- `movies.csv`: adult, belongs_to_collection, budget, genres, ... release_date, revenue, runtime, ...
- `ratings.csv`: userId, movieId, rating, timestamp

## Ferramentas
Python (pandas, numpy, pyarrow), Jupyter/Colab, matplotlib/plotly, Apache Parquet.

## Como rodar
1. Crie venv e instale dependências: `pip install -r requirements.txt`
2. Coloque os CSVs (disponíveis em [The Movies Dataset](https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset?resource=download&select=movies_metadata.csv), utilizamos o movies_metadata.csv e o ratings.csv) em `dados/raw/`.
3. Rode o pipeline:
   - `python src/ingest.py`

## Estrutura
- `/src`: scripts ETL e baseline.
- `/docs`: arquitetura + checklist.

## Equipe
- Dados/ETL: Caio Couto, Lucas Rocha e Rafael Falk
- Modelagem: Caio Couto, Lucas Rocha e Rafael Falk
- Documentação/Repo: Caio Couto, Lucas Rocha e Rafael Falk
