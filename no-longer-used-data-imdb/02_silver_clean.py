# Databricks notebook source
# MAGIC %md
# MAGIC Imports

# COMMAND ----------

from pyspark.sql.functions import col, expr
from datetime import datetime


# COMMAND ----------

# MAGIC %md
# MAGIC Load bronze tables needed for cleaning/filtering

# COMMAND ----------

title_basics = spark.table("bronze_title_basics")
title_ratings = spark.table("bronze_title_ratings")
title_akas = spark.table("bronze_title_akas")

# Inspect schemas
title_basics.printSchema()
title_ratings.printSchema()
title_akas.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC Safe numeric casting

# COMMAND ----------

title_basics = title_basics \
    .withColumn("startYear", expr("try_cast(startYear as int)")) \
    .withColumn("runtimeMinutes", expr("try_cast(runtimeMinutes as int)"))

title_ratings = title_ratings \
    .withColumn("averageRating", expr("try_cast(averageRating as float)")) \
    .withColumn("numVotes", expr("try_cast(numVotes as int)"))


# COMMAND ----------

# MAGIC %md
# MAGIC Filter movies only and after 2000

# COMMAND ----------

title_basics = title_basics.filter(
    (col("titleType") == "movie") &
    (col("startYear") >= 2000)
)


# COMMAND ----------

# MAGIC %md
# MAGIC Filter languages: only English and Dutch (nl)

# COMMAND ----------

langs = ["en", "nl"]
title_akas_filtered = title_akas.filter(
    (col("language").isin(langs)) &
    (col("language").isNotNull())
)


# COMMAND ----------

# MAGIC %md
# MAGIC Join title_basics with title_akas

# COMMAND ----------

silver_movies = title_basics.join(
    title_akas_filtered,
    title_basics.tconst == title_akas_filtered.titleId,
    "inner"
).select(
    title_basics["*"],
    title_akas_filtered["language"],
    title_akas_filtered["region"]
)


# COMMAND ----------

# MAGIC %md
# MAGIC Join with ratings

# COMMAND ----------

silver_movies = silver_movies.join(
    title_ratings,
    silver_movies.tconst == title_ratings.tconst,
    "inner"
).select(
    silver_movies["*"],
    title_ratings["averageRating"],
    title_ratings["numVotes"]
)


# COMMAND ----------

# MAGIC %md
# MAGIC Apply vote & upcoming filters

# COMMAND ----------

min_votes = 5
current_year = datetime.now().year

silver_movies_filtered = silver_movies.filter(
    (col("numVotes") >= min_votes) &
    (col("startYear") <= current_year)  
)


# COMMAND ----------

# MAGIC %md
# MAGIC Filter only English/Dutch speaking country/region

# COMMAND ----------

allowed_regions = ["US", "GB", "NL", "BE", "AU"] 

title_akas_filtered = title_akas_filtered.filter(
    (col("region").isin(allowed_regions)) &
    (col("language").isin(["en", "nl"])) &
    (col("language").isNotNull())
)


# COMMAND ----------

# MAGIC %md
# MAGIC Join with title_basics + ratings

# COMMAND ----------

silver_movies = title_basics.join(
    title_akas_filtered,
    title_basics.tconst == title_akas_filtered.titleId,
    "inner"
).select(
    title_basics["*"],
    title_akas_filtered["language"],
    title_akas_filtered["region"]
)

silver_movies = silver_movies.join(
    title_ratings,
    silver_movies.tconst == title_ratings.tconst,
    "inner"
).select(
    silver_movies["*"],
    title_ratings["averageRating"],
    title_ratings["numVotes"]
)


# COMMAND ----------

# MAGIC %md
# MAGIC Deduplicate and pick top 1000

# COMMAND ----------

silver_movies_top = silver_movies.orderBy(
    col("averageRating").desc(),
    col("numVotes").desc()
).dropDuplicates(["tconst"]).limit(1000)


# COMMAND ----------

# MAGIC %md
# MAGIC Write data frame to table

# COMMAND ----------

silver_movies_top.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("silver_movies_top")


# COMMAND ----------

# MAGIC %md
# MAGIC List the created table(s)

# COMMAND ----------

spark.sql("SHOW TABLES").filter("tableName LIKE 'silver_%'").show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC Show number of rows in the created table(s)

# COMMAND ----------

# Get all Silver tables
tables = spark.sql("SHOW TABLES").filter("tableName LIKE 'silver_%'").collect()

for t in tables:
    table_name = t.tableName
    row_count = spark.table(table_name).count()
    print(f"{table_name}: {row_count} rows")
