from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from schema import mainSchema

# joining 2 CSV files into one with linking movieId fild to bring all movies and the coresponding rating
def schema_rating(spark):
    # movie_ratings = movie.join(ratings, movie.movieId == ratings.movieId, 'inner').drop('movieId')
    # movie_ratings.select('movie_title2', 'rating').show()

    # joined2 = main.join(movies, main.movieTitle == movies.movieTitle2) \
    #     .drop('facenumberInPosters') \
    #     .drop('aspectRatio') \
    #     .drop('contentRating') \
    #     .drop('duration') \
    #     .show(10, False)

    # joined2.createOrReplaceTempView("grouped")

    """
    Filtering information and calculating the highest gross and rating in a movie, printing them in an descending orderBy, This would also show the top directors with highest ratings
    """
    main.createOrReplaceTempView("grouped")
    coloured = spark.sql("SELECT * FROM grouped WHERE color == color AND titleYear >= 1997")
    coloured.createOrReplaceTempView('colour')
    # ratings_movies = spark.sql("SELECT directorName, imdbScore, gross, titleYear, movieTitle FROM colour") \
    #     .orderBy('imdbScore', ascending = False) \
    #     .orderBy('gross', ascending = False) \
    #     .show(10, False)

    """
    Calculate directors average gross and movie ratings and sorting them in an descending order
    """
    avg_director_rating = spark.sql("SELECT directorName, imdbScore FROM colour") \
        .groupBy('directorName').agg({'imdbScore': 'avg'})
    avg_director_gross = spark.sql("SELECT directorName, gross FROM colour WHERE gross > 1") \
        .groupBy('directorName').agg({'gross': 'avg'})
    avg_director_main = avg_director_rating.join(avg_director_gross, avg_director_rating.directorName == avg_director_gross.directorName, 'inner') \
        .drop(avg_director_gross.directorName)
    avg_director_main.orderBy('avg(gross)', ascending=False).show(20, False)

    """
    Calculating actors highest ratings and their development throughout the years
    """
    avg_actor_rating = spark.sql("SELECT actor1Name, actor2Name, actor3Name, imdbScore, gross, movieTitle, titleYear FROM colour") \
        .orderBy("gross", ascending=False)
    reduced_actor_list = avg_actor_rating.limit(200)
    reduced_actor_list.createOrReplaceTempView("reducedActorList")

    top_actor1_gross = spark.sql("SELECT actor1Name , gross FROM reducedActorList") \
        .groupBy("actor1Name").agg({'gross': 'avg'}) \
        .orderBy("avg(gross)", ascending=False)
        # .show()
    top_actor1_sum = spark.sql("SELECT actor1Name FROM reducedActorList") \
        .groupBy("actor1Name") \
        .count()
        main.create
    top_actor1_list = top_actor1_gross.join(top_actor1_sum, top_actor1_gross.actor1Name == top_actor1_sum.actor1Name, 'inner') \
        .drop(top_actor1_sum.actor1Name) \
        .orderBy("avg(gross)", ascending=False) \
        .orderBy("count", ascending=False) \
        .show()

    top_actor2_list = spark.sql("SELECT actor2Name , gross FROM reducedActorList") \
        .groupBy("actor2Name").agg({'gross': 'avg'}) \
        .orderBy("avg(gross)", ascending=False)
        # .show()
    top_actor2_list = top_actor2_gross.join(top_actor2_sum, top_actor2_gross.actor1Name == top_actor2_sum.actor1Name, 'inner') \
        .drop(top_actor2_sum.actor2Name) \
        .orderBy("avg(gross)", ascending=False) \
        .orderBy("count", ascending=False) \
        .show()

    top_actor3_list = spark.sql("SELECT actor3Name , gross FROM reducedActorList") \
        .groupBy("actor3Name").agg({'gross': 'avg'}) \
        .orderBy("avg(gross)", ascending=False)
        # .show()
    top_actor3_list = top_actor3_gross.join(top_actor3_sum, top_actor3_gross.actor1Name == top_actor3_sum.actor1Name, 'inner') \
        .drop(top_actor3_sum.actor1Name) \
        .orderBy("avg(gross)", ascending=False) \
        .orderBy("count", ascending=False) \
        .show()



# filtering movie_metadata to bring out all coloured movies after 1997
# def schema_test(spark):
#     main.createOrReplaceTempView("colour")
#     coloured = spark.sql("SELECT gross, directorName, budget FROM colour WHERE color == color AND titleYear >= 1997") \
#         .show()
#
# def director_movies_count(spark):
#     main.createOrReplaceTempView("director_movies")
#     total_director_movies = spark.sql("SELECT directorName FROM director_movies") \
#         .groupBy('directorName') \
#         .count() \
#         .orderBy("count", ascending=False) \
#         .show()


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
    dose not work properly and prints out all cells seperatly
    """
    main = spark.read.csv("/Users/CaptainIvanov/Documents/303spark/datasets/movie_metadata1.csv",
        header=True,
        schema = mainSchema)
    ratings = spark.read.csv("/Users/CaptainIvanov/Documents/303spark/datasets/movie_ratings.csv",
        header=True)
    movie = spark.read.csv("/Users/CaptainIvanov/Documents/303spark/datasets/movie_movies.csv",
        header=True)


    # function calls
    # schema_test(spark)
    schema_rating(spark)
    # director_movies_count(spark)

    spark.stop()
