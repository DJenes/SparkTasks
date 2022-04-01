from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.window import Window

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
    .filter(title_basics.genres != "\\N")\
    .orderBy(title_basics.genres)\
    .join(f.broadcast(title_ratings), title_basics.tconst == title_ratings.tconst)\
    .drop(title_ratings.tconst)\
    .filter(title_ratings.numVotes > 100000)\
    .orderBy(f.col('averageRating').desc())\
    .select('tconst', 'primaryTitle','startYear','genres','averageRating','numVotes')\
    .withColumn('yearRange',(f.col('startYear') - f.col('startYear') % 10))\
    .orderBy('yearRange')

window1 = Window.partitionBy(temp_tb.genres,temp_tb.yearRange).orderBy(temp_tb.averageRating.desc())

temp_tb = temp_tb.select('*', f.rank().over(window1).alias('rank'))\
    .filter(f.col('rank') <= 10)\
    .drop(f.col('rank'))

temp_tb.coalesce(1).write\
    .option('header','true')\
    .option("inferSchema","true")\
    .csv(r'/SparkTasks/50sMv')