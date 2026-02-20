# 🎬 MovieLens Lakehouse Recommender
PySpark | Delta Lake | Azure Databricks (Community Edition)

## Overview
This project simulates a real-world data engineering scenario at a streaming platform.
The objective was to build a production-style Lakehouse architecture using the Medallion design pattern and train a recommendation model using Spark MLlib.
Data originates from multiple fragmented sources (mainly CSV) and is progressively refined across Bronze, Silver, and Gold layers before training an ALS recommendation engine.

## Architecture
Raw Data → Bronze → Silver → Gold → ALS Model → Recommendations

## Data Sources
- MovieLens Ratings (CSV)
- MovieLens Movies (CSV)
- MovieLens Links (CSV)
- External metadata (CSV)

# 🥉 Bronze
Raw data is ingested and stored as Delta tables without transformation.

Key:
Bronze stores immutable raw data in Delta format to ensure reliability, reproducibility, and traceability.

Tables:
- bronze_ratings_csv
- bronze_movies_csv
- bronze_links_csv

# 🥈 Silver
Data quality enforcement and relational modeling.

Actions:
- Explicit schema casting
- Timestamp normalization
- Deduplication
- Genre parsing into ArrayType
- Left joins for referential integrity
- Creation of fact and dimension tables

Key:
Silver cleans, validates, and structurally models the data into reliable fact and dimension tables.

Tables:
- silver_fact_ratings
- silver_dim_movies_enriched
- silver_dim_users

# 🥇 Gold
Business-ready, optimized datasets for analytics and ML.

Actions:
- User-level aggregations (e.g. total_ratings, avg_rating)
- Final fact table for model training
- Clean dimensional enrichment
- Null validation
- Structured ML training dataset

Key:
Gold aggregates and optimizes trusted data into business-ready datasets for analytics and machine learning.

Tables:
- gold_fact_ratings
- gold_dim_users
- gold_dim_movies_enriched

## Machine Learning
ALS collaborative filtering model:
- Cold start handling
- Top 10 recommendations per user
- Results stored in Delta

## How to Run
1. Execute 01_bronze notebook
2. Execute 02_silver notebook
3. Execute 03_gold notebook
4. Execute 04_train notebook
