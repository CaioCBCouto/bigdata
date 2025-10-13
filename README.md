# Recomendador de Filmes — Pipeline (AV1)

## Descrição
Pipeline de dados para preparar `movies.csv` e `ratings.csv` em camadas Bronze/Silver/Gold e gerar um baseline de recomendação.

## Fontes de dados
Disponivel no seguinte link -> [The Movies Dataset](https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset?resource=download&select=movies_metadata.csv)

Colunas encontradas nos 2 datasets:
- `movies.csv`: adult, belongs_to_collection, budget, genres, ... release_date, revenue, runtime, ...
- `ratings.csv`: userId, movieId, rating, timestamp

## Ferramentas
Python (pandas, numpy, pyarrow), Jupyter/Colab, matplotlib/plotly, Apache Parquet.

## Como rodar
1. Crie venv com `python -m venv .venv`
2. Acesse a venv `source .venv/bin/activate`
3. instale dependências: `pip install -r requirements.txt`
4. Acesse o link do Kaggle para baixar os CSV's e adicione-os em `dados/raw/` (voce deve criar as 2 pastas).
5. Rode o pipeline:
   - `python src/run_all.py`

## Estrutura
- `/src`: scripts ETL e baseline.
- `/documentacao`: arquitetura + checklist.
- `/notebooks`: notebooks para visualizações mais didáticas

## Equipe
- `Dados/ETL`: Caio Couto, Lucas Rocha e Rafael Falk
- `Modelagem`: Caio Couto, Lucas Rocha e Rafael Falk
- `Documentação/Repo`: Caio Couto, Lucas Rocha e Rafael Falk
