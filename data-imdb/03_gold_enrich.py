# Databricks notebook source
# MAGIC %md
# MAGIC Imports

# COMMAND ----------

from pyspark.sql.functions import col, split, explode, collect_list, desc, expr


# COMMAND ----------

# MAGIC %md
# MAGIC Load silver table(s)

# COMMAND ----------

silver_movies = spark.table("silver_movies_top")

# Quick check
silver_movies.show(5, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC Fact ratings

# COMMAND ----------

# Drop existing table if it exists
spark.sql("DROP TABLE IF EXISTS gold_fact_ratings")

# Cast numeric columns to proper types
fact_ratings = silver_movies.select(
    "tconst",
    expr("try_cast(averageRating as double)").alias("averageRating"),
    expr("try_cast(numVotes as int)").alias("numVotes")
)

# Persist Gold fact table
fact_ratings.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("gold_fact_ratings")

# Verify top ratings
fact_ratings.orderBy(desc("averageRating")).show(10, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC Enrich with relevant data

# COMMAND ----------

from pyspark.sql.functions import col

# Load Bronze tables
title_crew = spark.table("bronze_title_crew")
name_basics = spark.table("bronze_name_basics")

# Load top movies from Gold dim table
dim_movies_enriched_full = spark.table("gold_dim_movies_enriched")

# Select only the top 1000 (or desired number) movies by rating
top_movies = dim_movies_enriched_full.orderBy(col("averageRating").desc()) \
    .limit(1000) \
    .select("tconst")


# COMMAND ----------

# MAGIC %md
# MAGIC Further enrich with directors

# COMMAND ----------

from pyspark.sql.functions import split, explode

# Keep only top movies
title_crew_top = title_crew.join(top_movies, on="tconst", how="inner")

# Split multiple directors and explode
directors_exp = title_crew_top.select(
    "tconst",
    explode(split(col("directors"), ",")).alias("directorId")
)


# COMMAND ----------

# Join director IDs to get director names
directors_full = directors_exp.join(
    name_basics.select("nconst", "primaryName"),
    directors_exp.directorId == name_basics.nconst,
    "left"
).select(
    "tconst",
    col("primaryName").alias("directorName")
)


# COMMAND ----------

from pyspark.sql.functions import collect_list

# Aggregate all director names per movie
directors_agg = directors_full.groupBy("tconst") \
    .agg(collect_list("directorName").alias("directorNames"))


# COMMAND ----------

# Avoid column name conflicts by dropping existing directorNames if present
if "directorNames" in dim_movies_enriched_full.columns:
    dim_movies_enriched_full = dim_movies_enriched_full.drop("directorNames")

# Join aggregated directors
dim_movies_enriched_full_top = dim_movies_enriched_full.join(
    directors_agg,
    on="tconst",
    how="left"
)


# COMMAND ----------

# MAGIC %md
# MAGIC Write to tables

# COMMAND ----------

# Drop existing table if it exists
spark.sql("DROP TABLE IF EXISTS gold_dim_movies_enriched_full_top1000")

# Save as Delta Gold table
dim_movies_enriched_full_top.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("gold_dim_movies_enriched_full_top1000")

# Quick check: top 10 by rating
dim_movies_enriched_full_top.select("primaryTitle", "directorNames", "averageRating") \
    .orderBy(col("averageRating").desc()) \
    .show(10, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC List all created tables

# COMMAND ----------

spark.sql("SHOW TABLES").filter("tableName LIKE 'gold_%'").show(truncate=False)


# COMMAND ----------

# Get all Gold tables
tables = spark.sql("SHOW TABLES").filter("tableName LIKE 'gold_%'").collect()

for t in tables:
    table_name = t.tableName
    row_count = spark.table(table_name).count()
    print(f"{table_name}: {row_count} rows")


# COMMAND ----------

from pyspark.sql.functions import col

# Load Silver table (already filtered top movies)
silver_movies = spark.table("silver_movies_top")

# Select only required columns and cast types
fact_ratings = silver_movies.select(
    col("tconst"),
    col("averageRating").cast("double"),
    col("numVotes").cast("int")
)

# Drop existing Gold table if exists
spark.sql("DROP TABLE IF EXISTS gold_fact_ratings")

# Persist as Delta table
fact_ratings.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("gold_fact_ratings")

# Quick check: top 10 by averageRating
fact_ratings.orderBy(col("averageRating").desc()).show(10, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC Preparing weighted rating dataset (as ALS not possible with my IMDB source data)

# COMMAND ----------

from pyspark.sql.functions import col, avg

# Load fact table
fact_ratings = spark.table("gold_fact_ratings")

# Compute global mean rating (C)
C = fact_ratings.select(avg("averageRating")).collect()[0][0]

# Set minimum votes threshold
m = 50

# Compute weighted rating
fact_weighted = fact_ratings.withColumn(
    "weightedRating",
    ((col("numVotes") / (col("numVotes") + m)) * col("averageRating")) +
    ((m / (col("numVotes") + m)) * C)
)

# Top 20 recommendations
top_recommendations = fact_weighted.orderBy(col("weightedRating").desc()).limit(20)

top_recommendations.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC Save as gold tables

# COMMAND ----------

# Drop if exists (prevents schema conflicts)
spark.sql("DROP TABLE IF EXISTS gold_fact_weighted")

# Write weighted dataset
fact_weighted.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("gold_fact_weighted")


# COMMAND ----------

from pyspark.sql.functions import col

top_recommendations = fact_weighted \
    .orderBy(col("weightedRating").desc()) \
    .limit(20)

spark.sql("DROP TABLE IF EXISTS gold_top_recommendations")

top_recommendations.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_top_recommendations")


# COMMAND ----------

# MAGIC %md
# MAGIC List created gold tables

# COMMAND ----------

spark.sql("SHOW TABLES").filter("tableName LIKE 'gold_%'").show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC We will now look into what we can get out of the data, first top 20 movies per genre 

# COMMAND ----------

from pyspark.sql.functions import split, explode, col
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

movies = spark.table("gold_dim_movies_enriched_full_top1000")

# Explode genres
movies_genres = movies.withColumn(
    "genre",
    explode(split(col("genres"), ","))
)

# Ranking window
window_spec = Window.partitionBy("genre") \
    .orderBy(col("weightedRating").desc())

movies_ranked = movies_genres.withColumn(
    "rank_in_genre",
    row_number().over(window_spec)
)

top20_per_genre = movies_ranked.filter(col("rank_in_genre") <= 20)


# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS gold_top20_per_genre")

top20_per_genre.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_top20_per_genre")


# COMMAND ----------

spark.sql("SHOW TABLES").filter("tableName LIKE 'gold_%'").show(truncate=False)


# COMMAND ----------

spark.sql("""
SELECT DISTINCT genre
FROM gold_top20_per_genre
ORDER BY genre
""").show(30,truncate=False)


# COMMAND ----------

from pyspark.sql.functions import col, split, array_contains
from functools import reduce

# Load the Gold top1000 enriched movies
dim_movies = spark.table("gold_dim_movies_enriched_full_top1000")

# Optional: split genres temporarily for the check
genres_array = split(col("genres"), ",")

# Female-friendly genres
female_friendly_genres = ["Romance", "Comedy", "Family", "Musical", "Drama", "Biography", "History"]

# Create flag without changing the original genres column
female_cond = reduce(lambda a, b: a | b,
                     [array_contains(genres_array, g) for g in female_friendly_genres])

dim_movies = dim_movies.withColumn("female_friendly", female_cond)

# Quick check: original genres are intact
dim_movies.select("primaryTitle", "genres", "female_friendly").show(20, truncate=False)

# Persist updated table
dim_movies.write.format("delta").mode("overwrite") \
    .option("mergeSchema", "true").saveAsTable("gold_dim_movies_enriched_full_top1000")


# COMMAND ----------

spark.sql("""
SELECT primaryTitle, weightedRating, averageRating, numVotes
FROM gold_top20_per_genre
WHERE genre = 'Romance'
ORDER BY rank_in_genre
""").show()


# COMMAND ----------

from pyspark.sql.functions import col, split, expr

# Load Gold top1000 movies
dim_movies = spark.table("gold_dim_movies_enriched_full_top1000")

female_friendly_genres = ["Romance", "Comedy", "Family", "Musical", "Drama", "Biography", "History"]

# Split genres string into array temporarily
dim_movies = dim_movies.withColumn("genres_array", split(col("genres"), ","))

# Count number of female-friendly genres in each movie
dim_movies = dim_movies.withColumn(
    "female_genre_count",
    expr(f"aggregate(genres_array, 0, (acc, x) -> acc + IF(x IN ({', '.join([f'\"{g}\"' for g in female_friendly_genres])}), 1, 0))")
)

# Optional: sort by weighted count + rating
dim_movies.orderBy(col("female_genre_count").desc(), col("weightedRating").desc()).show(20, truncate=False)


# COMMAND ----------

from pyspark.sql.functions import col, split, expr

# Load Gold top1000 movies
dim_movies = spark.table("gold_dim_movies_enriched_full_top1000")

# Female-friendly genres
female_friendly_genres = ["Romance", "Comedy", "Family", "Musical", "Drama", "Biography", "History"]

# Split genres string into array temporarily
dim_movies = dim_movies.withColumn("genres_array", split(col("genres"), ","))

# Count number of female-friendly genres in each movie
dim_movies = dim_movies.withColumn(
    "female_genre_count",
    expr(f"aggregate(genres_array, 0, (acc, x) -> acc + IF(x IN ({', '.join([f'\"{g}\"' for g in female_friendly_genres])}), 1, 0))")
)

# Persist updated Gold table
dim_movies.write.format("delta").mode("overwrite") \
    .option("mergeSchema", "true").saveAsTable("gold_dim_movies_enriched_full_top1000")

# Quick check
dim_movies.select("primaryTitle", "genres", "female_friendly", "female_genre_count") \
    .orderBy(col("female_genre_count").desc(), col("averageRating").desc()) \
    .show(20, truncate=False)


# COMMAND ----------

# List tables in default database
spark.sql("SHOW TABLES IN default").show(truncate=False)

# Check columns in your Gold table
spark.table("gold_dim_movies_enriched_full_top1000").printSchema()

# Optional: peek at data
spark.table("gold_dim_movies_enriched_full_top1000") \
    .select("primaryTitle", "genres", "female_genre_count", "weightedRating") \
    .orderBy(col("female_genre_count").desc(), col("weightedRating").desc()) \
    .show(20, truncate=False)


# COMMAND ----------

from pyspark.sql.functions import col, split, explode, row_number
from pyspark.sql.window import Window

# Load Gold top1000 movies with female_genre_count
dim_movies = spark.table("gold_dim_movies_enriched_full_top1000")

# Split genres for per-genre processing
dim_movies_genre_exp = dim_movies.withColumn("genre", explode(split(col("genres"), ",")))

# Define window to rank movies within each genre
window_spec = Window.partitionBy("genre").orderBy(col("female_genre_count").desc(), col("weightedRating").desc())

# Add rank per genre
dim_movies_ranked = dim_movies_genre_exp.withColumn("rank", row_number().over(window_spec))

# Keep only top 20 per genre
top20_per_genre = dim_movies_ranked.filter(col("rank") <= 20).drop("rank")

# Persist as Gold table
spark.sql("DROP TABLE IF EXISTS gold_top20_per_genre")
top20_per_genre.write.format("delta").mode("overwrite").saveAsTable("gold_top20_per_genre")

# Quick check: top 5 movies per genre
top20_per_genre.orderBy("genre", col("female_genre_count").desc(), col("weightedRating").desc()).show(50, truncate=False)


# COMMAND ----------

from pyspark.sql.functions import col, split, explode, row_number
from pyspark.sql.window import Window

# Load Gold top1000 movies with female_genre_count
dim_movies = spark.table("gold_dim_movies_enriched_full_top1000")

# Split genres for per-genre processing
dim_movies_genre_exp = dim_movies.withColumn("genre", explode(split(col("genres"), ",")))

# Define window to rank movies within each genre
window_spec = Window.partitionBy("genre").orderBy(col("female_genre_count").desc(), col("weightedRating").desc())

# Add rank per genre
dim_movies_ranked = dim_movies_genre_exp.withColumn("rank", row_number().over(window_spec))

# Keep only top 20 per genre
top20_per_genre = dim_movies_ranked.filter(col("rank") <= 20).drop("rank")

# Remove duplicates (keep one row per movie, highest female_genre_count / weightedRating)
window_spec_dedup = Window.partitionBy("tconst").orderBy(col("female_genre_count").desc(), col("weightedRating").desc())
top20_per_genre_unique = top20_per_genre.withColumn("rn", row_number().over(window_spec_dedup)) \
                                       .filter(col("rn") == 1) \
                                       .drop("rn")

# Persist as Gold table
spark.sql("DROP TABLE IF EXISTS gold_top20_per_genre")
top20_per_genre_unique.write.format("delta").mode("overwrite").saveAsTable("gold_top20_per_genre")

# Quick check: top 20 movies per genre
top20_per_genre_unique.orderBy("genre", col("female_genre_count").desc(), col("weightedRating").desc()).show(50, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC