from pyspark.sql.types import *

mainSchema = StructType([StructField('color', StringType(), True),
                    StructField('directorName', StringType(), True),
                    StructField('numCriticForReviews', IntegerType(), True),
                    StructField('duration', IntegerType(), True),
                    StructField('directorFacebookLikes', IntegerType(), True),
                    StructField('actor3FacebookLikes', IntegerType(), True),
                    StructField('actor2Name', StringType(), True),
                    StructField('actor1FacebookLikes', IntegerType(), True),
                    StructField('gross', IntegerType(), True),
                    StructField('genres', StringType(), True),
                    StructField('actor1Name', StringType(), True),
                    StructField('movieTitle', StringType(), True),
                    StructField('numVotedUsers', IntegerType(), True),
                    StructField('castTotalFacebookLikes', IntegerType(), True),
                    StructField('actor3Name', StringType(), True),
                    StructField('facenumberInPosters', IntegerType(), True),
                    StructField('plotKeywords', StringType(), True),
                    StructField('movieImdbLink', StringType(), True),
                    StructField('numUsersForReviews', IntegerType(), True),
                    StructField('language', StringType(), True),
                    StructField('country', StringType(), True),
                    StructField('contentRating', StringType(), True),
                    StructField('budget', IntegerType(), True),
                    StructField('titleYear', IntegerType(), True),
                    StructField('actor2FacebookLikes', IntegerType(), True),
                    StructField('imdbScore', FloatType(), True),
                    StructField('aspectRatio', FloatType(), True),
                    StructField('movieFacebookLikes', IntegerType(), True)
                ])

ratingsSchema = StructType([StructField('userId', IntegerType(), True),
                    StructField('movieId', IntegerType(), True),
                    StructField('rating', FloatType(), True),
                    StructField('timestamp', IntegerType(), True)
                ])

movieSchema = StructType([StructField('movieId', IntegerType(), True),
                    StructField('movieTitle2', StringType(), True),
                    StructField('genres', StringType(), True)
                ])
