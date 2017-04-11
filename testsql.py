from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from schema import mainSchema, movieSchema, ratingsSchema


# filtering movie_metadata to bring out all coloured movies after 1997
def schema_test(spark):
    main.createOrReplaceTempView("colour")
    coloured = spark.sql("SELECT gross, directorName, budget FROM colour WHERE color == color AND titleYear >= 1997") \
        .show()

def director_movies_count(spark):
    main.createOrReplaceTempView("director_movies")
    total_director_movies = spark.sql("SELECT directorName FROM director_movies") \
        .groupBy('directorName') \
        .count() \
        .orderBy("count", ascending=False) \
        .show()

# joining 2 CSV files into one with linking movieId fild to bring all movies and the coresponding rating
def schema_rating(spark):
    movie_ratings = movie.join(ratings, movie.movieId == ratings.movieId)
    movie_ratings.createOrReplaceTempView('joined')
    movies = spark.sql("SELECT movieTitle2, rating FROM joined WHERE rating > 4 ") \
        .show()


# function that will run first at the start of the program every time
if __name__ == "__main__":

# loading Spark content and session to initialise DF
    spark = SparkSession \
        .builder \
        .appName("Pyspark sql test program") \
        .getOrCreate()

    sc = spark.sparkContext


    """"
    Loading multiple CSV files separately as loading them all at once into a single DF
    dose not work properly and prints out all fields as collums
    """
    main = spark.read.csv("/Users/CaptainIvanov/Documents/303spark/datasets/movie_metadata1.csv",
        header=True,
        schema = mainSchema)
    ratings = spark.read.csv("/Users/CaptainIvanov/Documents/303spark/datasets/movie_ratings.csv",
        header=True,
        schema = ratingsSchema)
    movie = spark.read.csv("/Users/CaptainIvanov/Documents/303spark/datasets/movie_movies.csv",
        header=True,
        schema = ratingsSchema)


    # function calls
    # schema_test(spark)
    # schema_rating(spark)
    director_movies_count(spark)

    spark.stop()
