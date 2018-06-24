import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: wordcount <file>")
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()
    lines = spark.sparkContext.textFile(sys.argv[1])
    counts = lines.flatMap(lambda line: line.split(' ')) \
        .map(lambda word: (word, 1)) \
        .toDF(["word", "value"]).groupBy("word").agg({"value": "count"})
    counts.show(n=100, truncate=False)
    spark.stop()