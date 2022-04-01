from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *

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

res_tb = title_ratings.filter(title_ratings.numVotes > 100000)\
    .join(title_basics, title_basics.tconst == title_ratings.tconst)\
    .drop(title_basics.tconst)\
    .filter(f.col('titleType') == 'movie')\
    .limit(100)
res_tb1 = title_principals.filter((title_principals.category == 'actor')&(title_principals.category != "\\N"))\
    .join(name_basics, title_principals.nconst == name_basics.nconst)\
    .drop(name_basics.nconst)

res_tb2 = res_tb.join(res_tb1, res_tb.tconst == res_tb1.tconst)\
    .drop(res_tb1.tconst)\
    .orderBy(f.col('averageRating').desc())\
    .groupBy('primaryName').count()

res_tb2 = res_tb2.select(res_tb2["primaryName"])\
    .filter(res_tb2["count"]>=2)\
    .orderBy(res_tb2["count"].desc())

res_tb2.coalesce(1).write\
    .option('header','true')\
    .option("inferSchema","true")\
    .csv(r'/SparkTasks/Actors')




