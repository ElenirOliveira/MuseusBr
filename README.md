
# Documentação Arquitetura Medallion — MuseusBR


## 1. Visão Geral da Arquitetura

- **Bronze**: Dados brutos da API.
- **Silver**: Dados limpos e estruturados.
- **Gold**: Dados analíticos prontos para consumo.

**Tecnologias**: Spark, PySpark, Parquet, JDBC, Azure Databricks, Key Vault, Azure SQL.

---

## 2. Configuração do Ambiente

### Montagem do Data Lake (Azure Blob Storage)

- Configuração de credenciais via Spark.
- Montagem das pastas:


---

## 3. Camada Bronze: Ingestão de Dados Brutos

- Coleta paginada via REST API dos Museus do Brasil.
- Transformação em RDD → DataFrame Spark.
- Persistência em Parquet na camada Bronze.

---

## 4. Camada Silver: Transformação e Limpeza

- Seleção de colunas principais.
- Limpeza e padronização de dados.
- Enriquecimento:
  - Latitude e Longitude.
  - Região geográfica via UDF.
- Persistência na camada Silver.

---

## 5. Publicação: Azure SQL Database

- Autenticação segura via Key Vault.
- Gravação com JDBC na tabela: `Elenir_Oliveira.silver_limpa`.

---

## 6. Camada Gold

- Aplicar regras de negócio.
- Criar datasets agregados.
- Disponibilizar para dashboards.

---

## 7.  Camada Gold — Regras de Negócio e Visualização

### ✔️ Objetivo:

Consolidar os dados das instituições culturais do Brasil, organizando-os por **Região** e **UF**, para permitir **consultas analíticas**, **visualização em dashboards** e **filtros interativos**.

### ✔️ Fluxo de Processamento:

1. **Leitura da Camada Silver**:

```python
df_silver = spark.read.parquet(silver_path)
```

2. **Aplicação de Regras de Negócio**:

- Consolidação do total de instituições por **Região** e **UF**.
- Agregações para análise em dashboards.

3. **Persistência da Gold**:

```python
df_gold.write.mode('overwrite').parquet(gold_path)
```

### ✔️ Consultas e Visualização Analítica:

#### ✅ Total de Instituições por Região e UF:

```sql
SELECT 
    Regiao, UF, TotalInstituicoes
FROM gold_view
ORDER BY Regiao, UF
```

- Representado em **gráfico de barras**.

#### ✅ Total de Instituições por Região:

```sql
SELECT 
    Regiao,
    SUM(TotalInstituicoes) AS TotalPorRegiao
FROM gold_view
GROUP BY Regiao
ORDER BY Regiao
```

- Exibição agregada por **Região** em **gráfico de barras**.

### ✔️ Visualização e Dashboard:

- **Mapa interativo** das instituições culturais.
- Possibilidade de **filtrar** e **visualizar**:
  - **Endereço**.
  - **Descrição**.
  - **Tipo da instituição**.
  - **Região** (Norte, Nordeste, Centro-Oeste, Sul, Sudeste).

➡️ Isso permite aos usuários explorar facilmente a **distribuição cultural** no Brasil.
----

## 8. Boas Práticas

- Segurança com Key Vault.
- Modularidade entre camadas.
- Formatos eficientes: Parquet.
- Integração com Azure.


