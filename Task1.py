from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
spark =(SparkSession.builder
   .master('local')\
   .appName('Top Movies IMDb dataset')\
   .getOrCreate())


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

temp_tb = title_basics.filter(title_basics.titleType == 'movie')\
    .join(title_ratings, title_basics.tconst == title_ratings.tconst)\
    .filter(f.col('numVotes') > 100000)\
    .orderBy(f.col('averageRating').desc())\
    .drop(title_ratings.tconst)

#Top 100 Movies
top100 = temp_tb.select(temp_tb.tconst, temp_tb.primaryTitle,
                         temp_tb.averageRating, temp_tb.numVotes, temp_tb.startYear)\
    .limit(100)

#Top 100 for last 10 years
TopLast10Y = temp_tb.select(temp_tb.tconst, temp_tb.primaryTitle,
                         temp_tb.averageRating, temp_tb.numVotes, temp_tb.startYear)\
     .filter(temp_tb.startYear >= 2012)\
    .limit(100)
#Top for 60s
TopFor60 = temp_tb.select(temp_tb.tconst, temp_tb.primaryTitle,
                         temp_tb.averageRating, temp_tb.numVotes, temp_tb.startYear)\
     .filter((temp_tb.startYear > 1959) & (temp_tb.startYear < 1970))\
    .limit(100)

