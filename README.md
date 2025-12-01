# Recomendador de Filmes — Pipeline de Dados Big Data (AV1)

---

## 1. Introdução

O crescimento das plataformas de streaming e da indústria do entretenimento digital intensificou o uso de **grandes volumes de dados (Big Data)** para apoiar decisões de negócio e personalizar a experiência dos usuários. Milhões de registros de filmes, avaliações, gêneros, receitas e orçamentos são gerados continuamente, criando um cenário em que decisões baseadas em dados tornam-se fundamentais para destacar conteúdos relevantes em catálogos cada vez mais extensos.

Nesse contexto, este projeto utiliza o conjunto de dados público **“The Movies Dataset”** (Kaggle), contendo informações de filmes (*movies.csv*) e avaliações de usuários (*ratings.csv*), como base para construção de um **pipeline de dados completo** e de um **sistema de recomendação básico**. 

O problema central abordado é a **dificuldade do usuário em escolher um filme** dentro de um catálogo massivo, ao mesmo tempo em que os dados brutos disponibilizados em formato CSV são **heterogêneos, desestruturados e com ruídos** (tipos inconsistentes, colunas em formato JSON como *string*, valores ausentes, entre outros). Assim, é necessário aplicar técnicas de Engenharia de Dados para **transformar dados brutos em informação utilizável**, culminando em métricas de qualidade e recomendações personalizadas.

---

## 2. Motivação

A motivação deste projeto está diretamente ligada à **aplicação prática de conceitos de Engenharia de Dados**, com ênfase em:

* Construção de **pipelines ETL/ELT** (extração, transformação e carregamento);
* Organização de dados em camadas lógicas (**Bronze, Silver, Gold**);
* Uso de formatos otimizados (como **Parquet**) para leitura analítica em larga escala. 

Além do aspecto técnico, há uma motivação de negócio clara: **sistemas de recomendação** são hoje um dos principais diferenciais competitivos em plataformas de streaming, pois permitem:

* Personalizar a interface de cada usuário;
* Aumentar engajamento e tempo de visualização;
* Destacar conteúdos com alta probabilidade de interesse;
* Apoiar estratégias de curadoria e marketing.

Assim, o projeto integra **boas práticas de Engenharia de Dados** com **métodos básicos de recomendação**, resultando em uma solução didática, porém alinhada a arquiteturas adotadas em ambientes de produção.

---

## 3. Objetivo do Projeto

### 3.1 Objetivo Geral

Desenvolver um **pipeline de dados automatizado**, estruturado segundo a **Arquitetura Medalhão (Bronze, Silver, Gold)**, capaz de ingerir, limpar, transformar e modelar dados de filmes e avaliações, produzindo artefatos analíticos para suporte a sistemas de recomendação.

### 3.2 Objetivos Específicos

* **Camada Bronze**:

  * Ingerir os arquivos *movies.csv* e *ratings.csv* a partir de `dados/raw/` e convertê-los para o formato **Parquet**, preservando a estrutura original (dados “raw”). 

* **Camada Silver**:

  * Limpar e padronizar tipos de dados (datas, numéricos, booleanos);
  * Realizar *parsing* de colunas em formato string-JSON (como *genres* e *belongs_to_collection*);
  * Remover duplicidades e registros inválidos ou inconsistentes nas avaliações. 

* **Camada Gold**:

  * Construir a tabela **`movies_scores`** com o cálculo do **Weighted Rating** (fórmula inspirada no IMDB), a partir das colunas `vote_average` e `vote_count`; 
  * Construir a tabela **`user_genre_profiles`**, estimando o perfil de preferência por gênero de cada usuário com base em sua média de notas e número de avaliações; 
  * Implementar a função **`recommend()`**, que gera recomendações personalizadas combinando o gênero favorito do usuário e a nota ponderada dos filmes. 

* **Orquestração**:

  * Automatizar a execução sequencial do pipeline por meio do script **`run_all.py`**. 

---

## 4. Metodologia (Pipeline de Dados)

### 4.1 Tecnologias Utilizadas

O projeto é desenvolvido em **Python**, com ênfase em bibliotecas para manipulação e análise de dados:

* **Linguagem**: Python 3.x
* **Bibliotecas principais**:

  * `pandas`, `numpy` (manipulação e análise de dados);
  * `pyarrow` / Apache Parquet (armazenamento colunar e eficiente);
  * `matplotlib`, `plotly` (visualizações em notebooks);
* **Ambiente**: Jupyter/Colab para passos exploratórios. 

A arquitetura do código está organizada em:

* `src/ingest/ingest.py` – ingestão (Bronze); 
* `src/transform/transform.py` – limpeza e padronização (Silver); 
* `src/gold/gold.py` – agregação, modelagem e recomendação (Gold); 
* `src/utils/utils.py` – funções auxiliares robustas de parsing. 

Além dos scripts, há notebooks (`01_ingest_validacao.ipynb`, `02_transform_exploracao.ipynb`, `03_gold_analise.ipynb`) que documentam e validam visualmente cada etapa do pipeline.

---

### 4.2 Arquitetura Medalhão

A solução segue uma **Arquitetura Medalhão** clássica, com três camadas de dados:

| Camada | Diretório       | Responsabilidade Principal                                        | Script          |
| ------ | --------------- | ----------------------------------------------------------------- | --------------- |
| Bronze | `dados/bronze/` | Ingestão dos dados brutos em Parquet, com mínima intervenção      | `ingest.py`     |
| Silver | `dados/silver/` | Limpeza, padronização de tipos e enriquecimento estrutural        | `transform.py`  |
| Gold   | `dados/gold/`   | Agregação, cálculo de métricas e construção de tabelas analíticas | `gold.py`       |

A orquestração do fluxo é feita pelo script `src/run_all.py`, responsável por disparar as três etapas de forma sequencial. 

---

### 4.3 Camada Bronze — Ingestão (`ingest.py`)

A camada **Bronze** é responsável por:

* Ler os arquivos CSV brutos em `dados/raw/`:

  * `movies.csv`
  * `ratings.csv` 
* Persistir esses dados em formato **Parquet** no diretório `dados/bronze/`, criando os artefatos:

  * `movies_raw.parquet`
  * `ratings_raw.parquet` 

O arquivo `src/ingest/ingest.py` contém duas funções principais:

```python
def ingest_movies():
    df = pd.read_csv(f"{RAW_DIR}/movies.csv", low_memory=False)
    df.to_parquet(f"{BRONZE_DIR}/movies_raw.parquet", index=False)

def ingest_ratings(batch_size: int = 0):
    # leitura direta ou em micro-lotes (chunks)
```



Destaques metodológicos:

* **Processamento em micro-lotes (chunks)** para o dataset de ratings:
  `ingest_ratings(batch_size=100000)` permite ler o arquivo em blocos de 100.000 linhas, mitigando problemas de memória para arquivos muito volumosos. 
* Preservação da estrutura original dos dados (“raw”), sem remoção de colunas ou filtragens, garantindo reprodutibilidade e rastreabilidade.

---

### 4.4 Camada Silver — Transformação (`transform.py`)

A camada **Silver** é responsável por **limpar e padronizar** os dados, preparando-os para análises e modelagem.

#### 4.4.1 Limpeza de Filmes (`clean_movies`)

A função `clean_movies()` lê `movies_raw.parquet` da camada Bronze e realiza as seguintes etapas: 

* **Conversão de tipos booleanos**

  * Colunas como `adult` e `video` são convertidas para booleanos com a função auxiliar `to_bool`, que trata valores textuais como `"true"`, `"false"`, `"1"`, `"0"`, `"yes"`, `"no"` e variantes. 

* **Parsing de datas**

  * `release_date` é convertida para o tipo `datetime`, com tratamento robusto de erros (`errors="coerce"`). 

* **Padronização de tipos numéricos**

  * Colunas como `budget`, `revenue`, `runtime`, `popularity`, `vote_average` e `vote_count` são convertidas para numéricos, com conversão de valores inválidos para `NaN`. 

* **Normalização de textos**

  * `original_language` é padronizada para minúsculas, removendo espaços excedentes. 

* **Parsing de colunas JSON em string**

  * A coluna `genres` é convertida para uma lista de objetos (dicionários) na nova coluna `genres_list`, usando `parse_list()`, que por sua vez utiliza `parse_json_flexible()` (tratando JSON e literais Python).  
  * A coluna `belongs_to_collection` é interpretada como dicionário e decomposta em campos derivados (`collection_id`, `collection_name`, `collection_poster_path`, `collection_backdrop_path`). 
  * Colunas como `production_companies`, `production_countries` e `spoken_languages` também são transformadas em listas estruturadas. 

* **Seleção e deduplicação**

  * Apenas colunas de interesse são mantidas (identificadores, títulos, metadados relevantes, métricas numéricas, listas de gêneros e informações de coleção);
  * Remoção de duplicados com base em `id`, garantindo uma linha por filme. 

O resultado é salvo como `dados/silver/movies_clean.parquet`. 

#### 4.4.2 Limpeza de Ratings (`clean_ratings`)

A função `clean_ratings()` trata os dados de avaliações (`ratings_raw.parquet`) com as seguintes etapas: 

* Conversão de `userId` e `movieId` para inteiros (tipo `Int64` tolerante a valores nulos);
* Conversão de `rating` para numérico;
* Criação da coluna temporal `ts` a partir de `timestamp` (segundos desde a época Unix), com conversão para `datetime`;
* Remoção de registros com `userId`, `movieId` ou `rating` nulos;
* Filtragem de notas fora do intervalo esperado (`0 ≤ rating ≤ 5`). 

O resultado é salvo como `dados/silver/ratings_clean.parquet`. 

---

### 4.5 Camada Gold — Modelagem e Métricas (`gold.py`)

A camada **Gold** é responsável por consolidar métricas e estruturas analíticas diretamente utilizáveis para recomendação.

#### 4.5.1 Cálculo do Weighted Rating (`compute_weighted_rating`)

O método `compute_weighted_rating()` lê `movies_clean.parquet` e calcula a nota ponderada de cada filme com base na fórmula clássica inspirada no IMDB: 

[
WR = \left(\frac{v}{v + m}\right) \cdot R + \left(\frac{m}{v + m}\right) \cdot C
]

Onde:

* ( R ) = média de avaliação do filme (`vote_average`);
* ( v ) = número de votos do filme (`vote_count`);
* ( C ) = média global das notas de todos os filmes;
* ( m ) = valor mínimo de votos para entrar no ranking (definido como o percentil **80** de `vote_count`). 

Passos principais:

1. Conversão de `vote_average` e `vote_count` para numérico;
2. Cálculo de ( C ) (média global) e ( m ) (quantile 0.80); 
3. Filtragem de filmes com `vote_count >= m` (filmes “qualificados”);
4. Cálculo de `weighted_rating` para cada filme qualificado;
5. Gravação da tabela resultante `movies_scores.parquet` em `dados/gold/`. 

Essa tabela representa a **base de ranking global de filmes**, independente de usuário.

#### 4.5.2 Perfil de Gênero por Usuário (`compute_user_genre_profiles`)

O método `compute_user_genre_profiles()` constrói uma visão **userId × gênero**, com a média de notas e o número de avaliações por combinação: 

1. Leitura de `ratings_clean.parquet` e `movies_clean.parquet`;
2. Explosão da lista `genres_list` para obter uma linha por filme-gênero;
3. Criação da coluna `genre` (nome do gênero, extraído de cada dicionário de `genres_list`);
4. Junção de ratings com filmes (via `movieId` ↔ `id`), associando cada avaliação aos gêneros correspondentes;
5. Agrupamento por `userId` e `genre`, calculando:

   * `rating_mean` – média de notas do usuário naquele gênero;
   * `n_ratings` – contagem de avaliações naquele gênero. 

O resultado é salvo como `user_genre_profiles.parquet` em `dados/gold/`. 

#### 4.5.3 Função de Recomendação (`recommend`)

A função `recommend(user_id: int, n: int = 10)` implementa o **baseline de recomendação personalizada**:

1. Carrega:

   * `movies_scores.parquet` (notas ponderadas);
   * `user_genre_profiles.parquet` (perfil de gêneros);
   * `ratings_clean.parquet` (histórico do usuário). 

2. Identifica o **gênero favorito** do usuário com base em `rating_mean` (gênero em que o usuário, em média, atribui notas mais altas). 

3. Monta a lista de filmes:

   * Explode novamente `genres_list` de `movies_clean`;
   * Filtra apenas filmes do gênero favorito;
   * Junta com `movies_scores` para recuperar `weighted_rating`;
   * Exclui filmes já avaliados pelo usuário (para evitar recomendações redundantes). 

4. Ordena os candidatos por `weighted_rating` em ordem decrescente e retorna os **Top N filmes recomendados**.

Este método combina:

* **Preferência individual** (perfil de gênero);
* **Qualidade global do filme** (nota ponderada pelo volume de votos);
* **Histórico do usuário** (exclusão de filmes já avaliados).

---

### 4.6 Orquestração do Pipeline (`run_all.py`)

O script `src/run_all.py` automatiza a execução das três camadas principais do pipeline: 

```python
steps = [
    ["python", "src/ingest/ingest.py"],
    ["python", "src/transform/transform.py"],
    ["python", "src/gold/gold.py"],
]
for s in steps:
    print("\n=== Executando:", " ".join(s))
    subprocess.run(s, check=True)
print("\n=== Concluído!")
```

Ele executa, na ordem correta:

1. **Bronze** – ingestão dos CSVs; 
2. **Silver** – limpeza e padronização; 
3. **Gold** – cálculo de métricas e geração de recomendações. 

Essa abordagem garante reprodutibilidade, permitindo que todo o pipeline seja disparado por um único comando (`python src/run_all.py`).

---

## 5. Resultados e Visualizações

Os resultados analíticos são explorados principalmente no notebook **`03_gold_analise.ipynb`**, que utiliza os artefatos produzidos pela camada Gold (`movies_scores.parquet` e `user_genre_profiles.parquet`). Nessa etapa, são produzidas visualizações e análises descritivas que auxiliam na interpretação do modelo de recomendação.

### 5.1 Distribuição das Notas Ponderadas (Weighted Rating)

Um histograma ou gráfico de densidade das colunas `weighted_rating` evidencia:

* A **distribuição geral da qualidade dos filmes** segundo a métrica ponderada;
* A existência de uma **cauda superior** representando filmes com destaque expressivo em popularidade e avaliação;
* A diferença entre filmes com alta média porém poucos votos e filmes com votação massiva e notas consistentes.

Essa visão ajuda a validar se o critério de qualificação por `vote_count` e o percentil escolhido para ( m ) estão produzindo um ranking coerente com as expectativas de negócio. 

### 5.2 Top 20 Filmes Melhor Avaliados

O notebook apresenta uma tabela ou gráfico contendo os **20 filmes com maior `weighted_rating`**, evidenciando:

* Títulos consolidados em termos de popularidade e avaliação;
* Possíveis franquias ou séries recorrentes;
* A aderência dos resultados ao senso comum sobre “clássicos” ou sucessos de crítica.

Essa lista corresponde ao **ranking global de recomendação**, caso nenhum perfil individual de usuário seja considerado.

### 5.3 Correlação entre Orçamento e Receita

A partir das colunas `budget` e `revenue` da tabela `movies_clean`, é possível gerar: 

* Gráficos de dispersão para analisar a relação entre **investimento** e **retorno financeiro**;
* Identificação de filmes com alto orçamento e baixa receita (riscos de negócio) e filmes de baixo orçamento com alta receita (alto ROI).

Essa análise fornece insumos para discutir estratégias de produção e distribuição à luz dos dados.

### 5.4 Distribuição dos Gêneros Mais Populares

Utilizando as estruturas de `genres_list` e, eventualmente, a contagem de ratings por gênero, o notebook ilustra:

* Os **gêneros mais frequentes** no catálogo;
* Os **gêneros mais avaliados** pelos usuários;
* Possíveis desalinhamentos entre o que é mais produzido e o que é mais consumido/avaliado.

Essa visão é fundamental para interpretar o impacto do perfil de gênero do usuário na recomendação.

### 5.5 Comparativo entre Nota do Usuário e Média do Público

Com base na junção entre `ratings_clean`, `movies_clean` e `movies_scores`, é possível comparar:

* As notas individuais de um usuário específico;
* A média global das notas (`vote_average`) ou mesmo o `weighted_rating`.  

Essa comparação permite identificar:

* Usuários mais “exigentes” (tendem a dar notas abaixo da média);
* Usuários mais “generosos” (tendem a dar notas acima da média);
* Desvios relevantes em relação ao consenso geral do público.

### 5.6 Funcionamento da Função `recommend()`

A função `recommend()` é utilizada no notebook para demonstrar o fluxo completo de recomendação:

1. Seleciona-se um `user_id` com histórico suficiente em `ratings_clean`; 
2. O perfil de gênero do usuário é obtido a partir de `user_genre_profiles`, identificando o gênero favorito; 
3. São filtrados filmes desse gênero em `movies_clean` e cruzados com `movies_scores`; 
4. Filmes já avaliados pelo usuário são excluídos, evitando recomendações redundantes; 
5. O resultado é ordenado por `weighted_rating` e os Top N filmes são retornados.

O notebook exibe esse resultado em forma de tabela, permitindo avaliar se as recomendações fazem sentido intuitivo em relação ao histórico de preferências do usuário.

---

## 6. Conclusões

A implementação do **Recomendador de Filmes — Pipeline de Dados Big Data (AV1)** demonstra a viabilidade e a importância de uma **Arquitetura Medalhão** aplicada a um caso real de dados de entretenimento:

* A **camada Bronze** garantiu ingestão robusta de arquivos CSV volumosos, utilizando Parquet como formato intermediário e suporte a micro-lotes para o arquivo de ratings. 
* A **camada Silver** foi responsável por resolver desafios significativos, como:

  * Tratamento de colunas JSON serializadas como texto (`genres`, `belongs_to_collection` etc.);
  * Padronização de tipos e filtragem de registros inválidos;
  * Deduplicação e estruturação de dados para facilitar análises posteriores.  
* A **camada Gold** consolidou uma base analítica, com:

  * Cálculo de notas ponderadas (`weighted_rating`) baseado em voto mínimo por percentil;
  * Perfis de gênero por usuário (`user_genre_profiles`);
  * Um baseline de recomendação que combina preferência individual e qualidade global do filme. 

### 6.1 Dificuldades Superadas

Durante o desenvolvimento, destacam-se:

* **Manipulação de colunas em formato string-JSON**: exigiu a criação de funções auxiliares (`parse_json_flexible`, `parse_list`, `parse_dict`) para lidar com diferentes formatações, valores ausentes e inconsistências. 
* **Escalabilidade em memória**: o arquivo de ratings, com grande número de linhas, demandou leitura em chunks (`chunksize`) na ingestão, evitando estouro de memória e permitindo processamento incremental. 

### 6.2 Trabalhos Futuros

Como caminhos naturais de evolução do projeto, sugerem-se:

* **Filtragem colaborativa e modelos de Machine Learning**

  * Implementar algoritmos de recomendação baseados em fatoração de matrizes, vizinhos mais próximos ou redes neurais, para ir além do perfil de gênero e da nota ponderada.

* **Migração para motores distribuídos**

  * Portar o pipeline para tecnologias como **Apache Spark** ou **Spark Structured Streaming**, utilizando formatos Parquet já produzidos, a fim de:

    * Aumentar a escalabilidade para volumes de dados ainda maiores;
    * Integrar o pipeline com ambientes de produção em nuvem (Data Lakes / Lakehouses).

* **Serviços de recomendação em tempo real**

  * Expor a função `recommend()` via API (por exemplo, Flask/FastAPI), integrando-a a aplicações web ou interfaces de usuário.

---

## 7. Como Executar (Anexo Técnico)

A seguir estão as instruções práticas para execução do pipeline, adaptadas e detalhadas a partir do README original. 

### 7.1 Pré-requisitos

* **Python 3.x** instalado;
* Acesso à internet para baixar o dataset do Kaggle;
* Sistema operacional compatível (Linux, macOS ou Windows).

### 7.2 Preparação do Ambiente Virtual

No diretório raiz do projeto:

```bash
# 1. Criar ambiente virtual
python -m venv .venv

# 2. Ativar o ambiente virtual
# Linux/macOS:
source .venv/bin/activate

# Windows (PowerShell):
# .\.venv\Scripts\Activate.ps1
```

Instalar as dependências:

```bash
pip install -r requirements.txt
```



### 7.3 Download e Organização dos Dados

1. Acessar o dataset **“The Movies Dataset”** no Kaggle (autor: *rounakbanik*). 
2. Baixar os arquivos necessários (ou gerar `movies.csv` e `ratings.csv` a partir do conjunto disponibilizado).
3. Criar a estrutura de diretórios:

```bash
mkdir -p dados/raw
mkdir -p dados/bronze
mkdir -p dados/silver
mkdir -p dados/gold
```

4. Copiar os arquivos CSV para `dados/raw/`:

```text
dados/raw/movies.csv
dados/raw/ratings.csv
```



### 7.4 Execução do Pipeline Completo

Com o ambiente virtual ativo e os CSVs posicionados, executar:

```bash
python src/run_all.py
```



Esse comando irá:

1. Ingerir os CSVs para Parquet (`dados/bronze/`); 
2. Limpar e padronizar os dados (`dados/silver/`); 
3. Gerar as tabelas analíticas e perfis de usuário (`dados/gold/`). 

Ao final, o terminal exibirá as mensagens de progresso e a confirmação `=== Concluído!`. 

### 7.5 Uso da Função de Recomendação em Código

Após a execução do pipeline, é possível utilizar a função `recommend()` diretamente em um shell Python ou notebook:

```python
from src.gold.gold import recommend

# Exemplo: recomendar 10 filmes para o usuário 1
df_rec = recommend(user_id=1, n=10)
print(df_rec[["title", "genre", "weighted_rating"]])
```



### 7.6 Exploração via Notebooks

Os notebooks no diretório `notebooks/` podem ser usados para:

* Validar a ingestão (`01_ingest_validacao.ipynb`);
* Explorar transformações e distribuições intermediárias (`02_transform_exploracao.ipynb`);
* Analisar métricas finais e recomendações (`03_gold_analise.ipynb`).

Recomenda-se executá-los **após** a conclusão do pipeline, de forma a garantir que os arquivos Parquet das camadas Bronze, Silver e Gold já estejam disponíveis.

---

## 8. Equipe

A responsabilidade pelo desenvolvimento deste projeto está distribuída da seguinte forma: 

* **Dados/ETL**:

  * Caio Couto
  * Lucas Rocha
  * Rafael Falk

* **Modelagem**:

  * Caio Couto
  * Lucas Rocha
  * Rafael Falk

* **Documentação/Repo**:

  * Caio Couto
  * Lucas Rocha
  * Rafael Falk

---
