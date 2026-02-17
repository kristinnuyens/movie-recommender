# Databricks notebook source
# MAGIC %md
# MAGIC I downloaded free iMDB files from https://datasets.imdbws.com/
# MAGIC
# MAGIC I uploaded those raw data files, but not before splitting the title_basics_tsv into chunks to be able to work with them; For possible future automation, this splitting will need to be looked into when ingesting newer data...
# MAGIC
# MAGIC First code block is to show all files loaded

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/movie-recommender-data-raw/")


# COMMAND ----------

# MAGIC %md
# MAGIC We set the path where these files are

# COMMAND ----------

# Base path to your uploaded raw files in DBFS
data_path = "/FileStore/tables/movie-recommender-data-raw/"


# COMMAND ----------

# MAGIC %md
# MAGIC For each of the files, we import, perform some minimal cleaning (\N, header duplicates, drop PK duplicates) and write the data frame to Delta (DFs are not "stored" and with the serverless option refreshing we need to re-run earlier cells regularly)

# COMMAND ----------

# DBTITLE 1,Cell 6
from pyspark.sql.functions import col, when

# --------- LOAD (ALL STRING) ----------
title_basics_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .option("inferSchema", False) \
    .csv(f"{data_path}title_basics_part_*.tsv")

# --------- REMOVE HEADER DUPLICATES ----------
title_basics_df = title_basics_df.filter(col("tconst") != "tconst")

# --------- REPLACE IMDb NULL PLACEHOLDER ----------
for c in title_basics_df.columns:
    title_basics_df = title_basics_df.withColumn(
        c,
        when(col(c) == "\\N", None).otherwise(col(c))
    )

# --------- DROP DUPLICATES ----------
title_basics_df = title_basics_df.dropDuplicates(["tconst"])

# --------- RESET TABLE (VERY IMPORTANT) ----------
spark.sql("DROP TABLE IF EXISTS bronze_title_basics")

# --------- WRITE BRONZE ----------
title_basics_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_title_basics")


# COMMAND ----------

spark.sql("DESCRIBE bronze_title_basics").show(truncate=False)


# COMMAND ----------

from pyspark.sql.functions import col, when

# LOAD (compressed gzip, all string)
name_basics_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .option("inferSchema", False) \
    .csv("dbfs:/FileStore/tables/movie-recommender-data-raw/name_basics_tsv.gz")

# REMOVE DUPLICATED HEADER ROWS
name_basics_df = name_basics_df.filter(col("nconst") != "nconst")

# REPLACE IMDb NULL PLACEHOLDER
for c in name_basics_df.columns:
    name_basics_df = name_basics_df.withColumn(
        c,
        when(col(c) == "\\N", None).otherwise(col(c))
    )

# DROP DUPLICATES
name_basics_df = name_basics_df.dropDuplicates(["nconst"])

# DROP EXISTING TABLE
spark.sql("DROP TABLE IF EXISTS bronze_name_basics")

# WRITE DELTA TABLE
name_basics_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_name_basics")


# COMMAND ----------

# LOAD
title_ratings_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .option("inferSchema", False) \
    .csv("dbfs:/FileStore/tables/movie-recommender-data-raw/title_ratings_tsv.gz")

# REMOVE HEADER ROWS
title_ratings_df = title_ratings_df.filter(col("tconst") != "tconst")

# REPLACE NULLS
for c in title_ratings_df.columns:
    title_ratings_df = title_ratings_df.withColumn(
        c,
        when(col(c) == "\\N", None).otherwise(col(c))
    )

# DROP DUPLICATES
title_ratings_df = title_ratings_df.dropDuplicates(["tconst"])

# DROP TABLE IF EXISTS
spark.sql("DROP TABLE IF EXISTS bronze_title_ratings")

# WRITE
title_ratings_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_title_ratings")


# COMMAND ----------

# LOAD
title_principals_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .option("inferSchema", False) \
    .csv("dbfs:/FileStore/tables/movie-recommender-data-raw/title_principals_tsv.gz")

# REMOVE HEADER ROWS
title_principals_df = title_principals_df.filter(col("tconst") != "tconst")

# REPLACE NULLS
for c in title_principals_df.columns:
    title_principals_df = title_principals_df.withColumn(
        c,
        when(col(c) == "\\N", None).otherwise(col(c))
    )

# DROP DUPLICATES
title_principals_df = title_principals_df.dropDuplicates(["tconst","nconst","ordering"])

# DROP TABLE IF EXISTS
spark.sql("DROP TABLE IF EXISTS bronze_title_principals")

# WRITE
title_principals_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_title_principals")


# COMMAND ----------

# LOAD
title_crew_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .option("inferSchema", False) \
    .csv("dbfs:/FileStore/tables/movie-recommender-data-raw/title_crew_tsv.gz")

# REMOVE HEADER ROWS
title_crew_df = title_crew_df.filter(col("tconst") != "tconst")

# REPLACE NULLS
for c in title_crew_df.columns:
    title_crew_df = title_crew_df.withColumn(
        c,
        when(col(c) == "\\N", None).otherwise(col(c))
    )

# DROP DUPLICATES
title_crew_df = title_crew_df.dropDuplicates(["tconst"])

# DROP TABLE IF EXISTS
spark.sql("DROP TABLE IF EXISTS bronze_title_crew")

# WRITE
title_crew_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_title_crew")


# COMMAND ----------

from pyspark.sql.functions import col, when

# --------- LOAD (ALL STRING) ----------
title_akas_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .option("inferSchema", False) \
    .csv("dbfs:/FileStore/tables/movie-recommender-data-raw/title_akas_tsv.gz")

# --------- REMOVE DUPLICATED HEADER ROWS ----------
title_akas_df = title_akas_df.filter(col("titleId") != "titleId")

# --------- REPLACE IMDb NULL PLACEHOLDER ----------
for c in title_akas_df.columns:
    title_akas_df = title_akas_df.withColumn(
        c,
        when(col(c) == "\\N", None).otherwise(col(c))
    )

# --------- DROP DUPLICATES ----------
title_akas_df = title_akas_df.dropDuplicates(["titleId","ordering"])

# --------- DROP TABLE IF EXISTS ----------
spark.sql("DROP TABLE IF EXISTS bronze_title_akas")

# --------- WRITE DELTA TABLE ----------
title_akas_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_title_akas")


# COMMAND ----------

# MAGIC %md
# MAGIC We list all the tables created

# COMMAND ----------

spark.sql("SHOW TABLES").filter("tableName LIKE 'bronze_%'").show(truncate=False)


# COMMAND ----------

tables = spark.sql("SHOW TABLES").filter("tableName LIKE 'bronze_%'").collect()

for t in tables:
    table_name = t.tableName
    path = spark.sql(f"DESCRIBE DETAIL {table_name}").select("location").collect()[0][0]
    print(f"{table_name} -> {path}")


# COMMAND ----------

# MAGIC %md
# MAGIC We show the number of rows per file

# COMMAND ----------

# Get all Bronze tables
tables = spark.sql("SHOW TABLES").filter("tableName LIKE 'bronze_%'").collect()

for t in tables:
    table_name = t.tableName
    row_count = spark.table(table_name).count()
    print(f"{table_name}: {row_count} rows")


# COMMAND ----------

# MAGIC %md
# MAGIC We show the column headers

# COMMAND ----------

from pyspark.sql import Row

summary_list = []

for t in tables:
    table_name = t.tableName
    df = spark.table(table_name)
    row_count = df.count()
    cols = df.columns
    summary_list.append(Row(table=table_name, rows=row_count, columns=cols))

summary_df = spark.createDataFrame(summary_list)
summary_df.show(truncate=False)
