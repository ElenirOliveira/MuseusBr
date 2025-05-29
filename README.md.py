# Databricks notebook source
# MAGIC %md
# MAGIC # Documentação Arquitetura Medallion — MuseusBR
# MAGIC
# MAGIC
# MAGIC ## 1. Visão Geral da Arquitetura
# MAGIC
# MAGIC - **Bronze**: Dados brutos da API.
# MAGIC - **Silver**: Dados limpos e estruturados.
# MAGIC - **Gold**: Dados analíticos prontos para consumo.
# MAGIC
# MAGIC **Tecnologias**: Spark, PySpark, Parquet, JDBC, Azure Databricks, Key Vault, Azure SQL.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. Configuração do Ambiente
# MAGIC
# MAGIC ### Montagem do Data Lake (Azure Blob Storage)
# MAGIC
# MAGIC - Configuração de credenciais via Spark.
# MAGIC - Montagem das pastas:
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3. Camada Bronze: Ingestão de Dados Brutos
# MAGIC
# MAGIC - Coleta paginada via REST API dos Museus do Brasil.
# MAGIC - Transformação em RDD → DataFrame Spark.
# MAGIC - Persistência em Parquet na camada Bronze.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4. Camada Silver: Transformação e Limpeza
# MAGIC
# MAGIC - Seleção de colunas principais.
# MAGIC - Limpeza e padronização de dados.
# MAGIC - Enriquecimento:
# MAGIC   - Latitude e Longitude.
# MAGIC   - Região geográfica via UDF.
# MAGIC - Persistência na camada Silver.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5. Publicação: Azure SQL Database
# MAGIC
# MAGIC - Autenticação segura via Key Vault.
# MAGIC - Gravação com JDBC na tabela: `Elenir_Oliveira.silver_limpa`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 6. Camada Gold
# MAGIC
# MAGIC - Aplicar regras de negócio.
# MAGIC - Criar datasets agregados.
# MAGIC - Disponibilizar para dashboards.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 7.  Camada Gold — Regras de Negócio e Visualização
# MAGIC
# MAGIC ### ✔️ Objetivo:
# MAGIC
# MAGIC Consolidar os dados das instituições culturais do Brasil, organizando-os por **Região** e **UF**, para permitir **consultas analíticas**, **visualização em dashboards** e **filtros interativos**.
# MAGIC
# MAGIC ### ✔️ Fluxo de Processamento:
# MAGIC
# MAGIC 1. **Leitura da Camada Silver**:
# MAGIC
# MAGIC ```python
# MAGIC df_silver = spark.read.parquet(silver_path)
# MAGIC ```
# MAGIC
# MAGIC 2. **Aplicação de Regras de Negócio**:
# MAGIC
# MAGIC - Consolidação do total de instituições por **Região** e **UF**.
# MAGIC - Agregações para análise em dashboards.
# MAGIC
# MAGIC 3. **Persistência da Gold**:
# MAGIC
# MAGIC ```python
# MAGIC df_gold.write.mode('overwrite').parquet(gold_path)
# MAGIC ```
# MAGIC
# MAGIC ### ✔️ Consultas e Visualização Analítica:
# MAGIC
# MAGIC #### ✅ Total de Instituições por Região e UF:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     Regiao, UF, TotalInstituicoes
# MAGIC FROM gold_view
# MAGIC ORDER BY Regiao, UF
# MAGIC ```
# MAGIC
# MAGIC - Representado em **gráfico de barras**.
# MAGIC
# MAGIC #### ✅ Total de Instituições por Região:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     Regiao,
# MAGIC     SUM(TotalInstituicoes) AS TotalPorRegiao
# MAGIC FROM gold_view
# MAGIC GROUP BY Regiao
# MAGIC ORDER BY Regiao
# MAGIC ```
# MAGIC
# MAGIC - Exibição agregada por **Região** em **gráfico de barras**.
# MAGIC
# MAGIC ### ✔️ Visualização e Dashboard:
# MAGIC
# MAGIC - **Mapa interativo** das instituições culturais.
# MAGIC - Possibilidade de **filtrar** e **visualizar**:
# MAGIC   - **Endereço**.
# MAGIC   - **Descrição**.
# MAGIC   - **Tipo da instituição**.
# MAGIC   - **Região** (Norte, Nordeste, Centro-Oeste, Sul, Sudeste).
# MAGIC
# MAGIC ➡️ Isso permite aos usuários explorar facilmente a **distribuição cultural** no Brasil.
# MAGIC ----
# MAGIC
# MAGIC ## 8. Boas Práticas
# MAGIC
# MAGIC - Segurança com Key Vault.
# MAGIC - Modularidade entre camadas.
# MAGIC - Formatos eficientes: Parquet.
# MAGIC - Integração com Azure.
# MAGIC
# MAGIC