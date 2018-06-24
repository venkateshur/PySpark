import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: wordcount <file>")
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()
    lines = spark.sparkContext.textFile(sys.argv[1])
    counts = lines.flatMap(lambda line: line.split(' ')) \
        .map(lambda word: (word, 1)) \
        .toDF(["WORD", "VALUE"]).groupBy("WORD").agg({"VALUE": "count"})\
        .withColumnRenamed('Count(value)', 'TOTAL_COUNT')
    counts.coalesce(1).write.mode("overwrite").format("CSV").option("header", "true").save(sys.argv[2])
    spark.stop()