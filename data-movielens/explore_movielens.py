import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# -----------------------------
# Load datasets
# -----------------------------

ratings = pd.read_csv("data/ratings.csv")
movies = pd.read_csv("data/movies.csv")
links = pd.read_csv("data/links.csv")
tags = pd.read_csv("data/tags.csv")

# -----------------------------
# Basic Info
# -----------------------------

def basic_info(df, name):
    print(f"\n===== {name} =====")
    print("Shape:", df.shape)
    print("\nColumns:")
    print(df.columns)
    print("\nMissing values:")
    print(df.isnull().sum())
    print("\nHead:")
    print(df.head())

basic_info(ratings, "RATINGS")
basic_info(movies, "MOVIES")
basic_info(links, "LINKS")
basic_info(tags, "TAGS")

# -----------------------------
# Cardinality Checks
# -----------------------------

print("\n===== CARDINALITY =====")
print("Unique users:", ratings["userId"].nunique())
print("Unique movies in ratings:", ratings["movieId"].nunique())
print("Unique movies in movies table:", movies["movieId"].nunique())

# -----------------------------
# Orphan Checks
# -----------------------------

ratings_movie_ids = set(ratings["movieId"])
movies_movie_ids = set(movies["movieId"])

print("\nMovies with ratings but not in movies table:",
      len(ratings_movie_ids - movies_movie_ids))

print("Movies in movies table but no ratings:",
      len(movies_movie_ids - ratings_movie_ids))

# -----------------------------
# Sparsity Analysis
# -----------------------------

ratings_per_user = ratings.groupby("userId")["rating"].count()
ratings_per_movie = ratings.groupby("movieId")["rating"].count()

print("\n===== SPARSITY =====")
print("Average ratings per user:", ratings_per_user.mean())
print("Average ratings per movie:", ratings_per_movie.mean())

# -----------------------------
# Rating Distribution
# -----------------------------

plt.figure()
ratings["rating"].hist(bins=10)
plt.title("Rating Distribution")
plt.xlabel("Rating")
plt.ylabel("Frequency")
plt.show()

# -----------------------------
# Top 10 Most Rated Movies
# -----------------------------

top_movies = (
    ratings.groupby("movieId")
    .size()
    .reset_index(name="rating_count")
    .sort_values("rating_count", ascending=False)
    .merge(movies, on="movieId")
)

print("\n===== TOP 10 MOST RATED MOVIES =====")
print(top_movies[["title", "rating_count"]].head(10))

# -----------------------------
# Genre Exploration
# -----------------------------

movies["genres_list"] = movies["genres"].str.split("|")

all_genres = movies.explode("genres_list")

genre_counts = all_genres["genres_list"].value_counts()

print("\n===== GENRE COUNTS =====")
print(genre_counts)

plt.figure()
genre_counts.head(10).plot(kind="bar")
plt.title("Top Genres")
plt.show()
