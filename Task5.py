from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder\
    .master('local[*]')\
    .appName('TopActors')\
    .getOrCreate()

title_ratings_schema = StructType([StructField('tconst', StringType(), True),
                                   StructField('averageRating', StringType(), True),
                                   StructField('numVotes', StringType(), True)])
title_basics_schema = StructType([StructField('tconst', StringType(), True),
                                   StructField('titleType', StringType(), True),
                                   StructField('primaryTitle', StringType(), True),
                                   StructField('originalTitle', StringType(), True),
                                   StructField('isAdult', StringType(), True),
                                   StructField('startYear', StringType(), True),
                                   StructField('endYear', StringType(), True),
                                   StructField('runtimeMinutes:', StringType(), True),
                                   StructField('genres', StringType(), True)
                                  ])

name_basics_schema = StructType([StructField('nconst', StringType(), True),
                                   StructField('primaryName', StringType(), True),
                                   StructField('birthYear', StringType(), True),
                                   StructField('deathYear', StringType(), True),
                                   StructField('primaryProfession', StringType(), True),
                                   StructField('knownForTitles', StringType(), True)])

title_principals_schema = StructType([StructField('tconst', StringType(), True),
                                   StructField('ordering', StringType(), True),
                                   StructField('nconst', StringType(), True),
                                   StructField('category', StringType(), True),
                                   StructField('job', StringType(), True),
                                   StructField('characters', StringType(), True)])

title_ratings = spark.read.option('header','true')\
                    .option('sep', '\t')\
                    .option('multiLine', 'true')\
                    .option('quote','\"')\
                    .option('escape','\"')\
                    .schema(title_ratings_schema)\
                    .option('ignoreTrailingWhiteSpace', 'true')\
                    .csv(r"C:\Users\Home\PycharmProjects\PySpark_Tsk\IMDBdataset\imdb_title.ratings.tsv")
title_basics = spark.read.option('header','true')\
                    .option('sep', '\t')\
                    .option('multiLine', 'true')\
                    .option('quote','\"')\
                    .schema(title_basics_schema)\
                    .option('escape','\"')\
                    .option('ignoreTrailingWhiteSpace', 'true')\
                    .csv(r"C:\Users\Home\PycharmProjects\PySpark_Tsk\IMDBdataset\imdb_title.basics.tsv")

name_basics = spark.read.option('header','true')\
                    .option('sep', '\t')\
                    .option('multiLine', 'true')\
                    .option('quote','\"')\
                    .schema(name_basics_schema)\
                    .option('escape','\"')\
                    .option('ignoreTrailingWhiteSpace', 'true')\
                    .csv(r"C:\Users\Home\PycharmProjects\PySpark_Tsk\IMDBdataset\imdb_name.basics.tsv")

title_principals = spark.read.option('header','true')\
                    .option('sep', '\t')\
                    .option('multiLine', 'true')\
                    .option('quote','\"')\
                    .schema(title_principals_schema)\
                    .option('escape','\"')\
                    .option('ignoreTrailingWhiteSpace', 'true')\
                    .csv(r"C:\Users\Home\PycharmProjects\PySpark_Tsk\IMDBdataset\imdb_title.principals.tsv")


title_basics = title_basics.filter(title_basics.titleType == 'movie')\
    .join(f.broadcast(title_ratings), title_basics.tconst == title_ratings.tconst)\
    .drop(title_basics.tconst)\
    .select('tconst','primaryTitle','startYear','averageRating','numVotes')


title_principals = title_principals.filter((title_principals.category == 'director')
                                           & (title_principals.category != "\\N")) \
    .join(f.broadcast(name_basics), title_principals.nconst == name_basics.nconst)\
    .drop(name_basics.nconst)\
    .select('tconst','primaryName')

res_tb = title_basics.join(f.broadcast(title_principals),title_basics.tconst == title_principals.tconst)\
    .drop(title_principals.tconst)\
    .drop(title_basics.tconst)\
    .orderBy(f.col('averageRating').desc())

window = Window.partitionBy(res_tb.primaryName).orderBy(res_tb.averageRating.desc())

res_tb = res_tb.select('*', f.rank().over(window).alias('rank'))\
    .filter(f.col('rank') <= 5)\
    .drop(f.col('rank'))


