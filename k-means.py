import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, collect_set, mean, udf, approxCountDistinct, countDistinct
from pyspark.sql.types import BooleanType
from pyspark.ml.feature import VectorAssembler
from functools import reduce
from time import sleep

spark = pyspark.sql.SparkSession.builder \
    .master("local") \
    .appName("extract-metrics") \
    .getOrCreate()

# загружаем метрки из csv



df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("output.csv/")

one_range_in_column = df.select([column for column in df.columns if df.agg(approxCountDistinct(column)).first()[0] == 1])
print(one_range_in_column.columns)
df = df.drop(*one_range_in_column.columns)
# df.show()

df = df.replace(float('nan'), None)
# df.show()

df = df.filter(~reduce(lambda x, y: x & y, [df[c].isNull() for c in df.columns]))
df.show()
sleep(20)

df = df.drop('time')


# fill_values = {column: df.agg({column:"mean"}).rdd.flatMap(list).collect()[0] for column in df.columns}
# print(fill_values)
# df = df.na.fill(fill_values)
#
df.show()


