spark

######################
ACCESS_KEY = "AKIAJ55R5FQYKFUXKUIA"
SECRET_KEY = "ULzZyIKjBmXvtuIzXCYUZ/oZIhFQ9V/nYs+UPNhW".replace("/", "%2F")
AWS_BUCKET_NAME = "moviedataset303com2"
MOUNT_NAME = "moviedataset"

dbutils.fs.mount("s3a://%s:%s@%s" % (ACCESS_KEY, SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)
display(dbutils.fs.ls("/mnt/%s" % MOUNT_NAME))

#####################
from pyspark.sql.types import *
from pyspark.sql.functions import *

#####################
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
                    StructField('titleYear', LongType(), True),
                    StructField('actor2FacebookLikes', IntegerType(), True),
                    StructField('imdbScore', FloatType(), True),
                    StructField('aspectRatio', FloatType(), True),
                    StructField('movieFacebookLikes', IntegerType(), True)
                ])


#####################
main_movie_table = spark.read.csv('/mnt/moviedataset/movie_metadata1.csv', header=True, schema=mainSchema)
main_movie_table.createOrReplaceTempView("grouped")
coloured = spark.sql("SELECT * FROM grouped WHERE color == color AND titleYear >= 1997")
coloured.createOrReplaceTempView('colour')

#####################
display(coloured)


#####################
avg_director_rating = spark.sql("SELECT directorName, imdbScore FROM colour").groupBy('directorName').agg({'imdbScore': 'avg'})
avg_director_gross = spark.sql("SELECT directorName, gross FROM colour WHERE gross > 1").groupBy('directorName').agg({'gross': 'avg'})
avg_director_main = avg_director_rating.join(avg_director_gross, avg_director_rating.directorName == avg_director_gross.directorName, 'inner').drop(avg_director_gross.directorName)
# avg_director_main.orderBy('avg(gross)', ascending=False).show(20, False)
reduced_director_main = avg_director_main.limit(100)
display(reduced_director_main)


#####################
avg_actor_rating = spark.sql("SELECT actor1Name, actor2Name, actor3Name, imdbScore, gross, movieTitle, titleYear FROM colour").orderBy("gross", ascending=False)
reduced_actor_list = avg_actor_rating.limit(200)
reduced_actor_list.createOrReplaceTempView("reducedActorList")

top_actor1_gross = spark.sql("SELECT actor1Name , gross FROM reducedActorList").groupBy("actor1Name").agg({'gross': 'avg'}).orderBy("avg(gross)", ascending=False)
top_actor1_sum = spark.sql("SELECT actor1Name FROM reducedActorList").groupBy("actor1Name").count()
top_actor1_list = top_actor1_gross.join(top_actor1_sum, top_actor1_gross.actor1Name == top_actor1_sum.actor1Name, 'inner') \
  .drop(top_actor1_sum.actor1Name) \
  .orderBy("avg(gross)", ascending=False) \
  .orderBy("count", ascending=False)
display(top_actor1_list)

top_actor2_list = spark.sql("SELECT actor2Name , gross FROM reducedActorList") \
  .groupBy("actor2Name").agg({'gross': 'avg'}) \
  .orderBy("avg(gross)", ascending=False)
top_actor2_list = top_actor2_gross.join(top_actor2_sum, top_actor2_gross.actor1Name == top_actor2_sum.actor1Name, 'inner') \
  .drop(top_actor2_sum.actor2Name) \
  .orderBy("avg(gross)", ascending=False) \
  .orderBy("count", ascending=False)
display(top_actor2_list)

top_actor3_list = spark.sql("SELECT actor3Name , gross FROM reducedActorList") \
  .groupBy("actor3Name").agg({'gross': 'avg'}) \
  .orderBy("avg(gross)", ascending=False)
top_actor3_list = top_actor3_gross.join(top_actor3_sum, top_actor3_gross.actor1Name == top_actor3_sum.actor1Name, 'inner') \
  .drop(top_actor3_sum.actor1Name) \
  .orderBy("avg(gross)", ascending=False) \
  .orderBy("count", ascending=False)
display(top_actor3_list)


#####################
spark.table("colour").count()
newColour = spark.table("colour")
newColour.rdd.getNumPartitions()


#####################
newColour.repartition(24).createOrReplaceTempView("mainTable4");


#####################
spark.catalog.cacheTable("mainTable4")


#####################
spark.table("mainTable4").count()
#error occurs after this line before partitioning and caching is complete
