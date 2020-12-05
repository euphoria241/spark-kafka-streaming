import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, collect_set, mean, udf, approxCountDistinct, countDistinct
from pyspark.sql.types import BooleanType
from pyspark.ml.feature import VectorAssembler
from functools import reduce
from time import sleep
from pyspark.ml.stat import Correlation

spark = pyspark.sql.SparkSession.builder \
    .master("local") \
    .appName("extract-metrics") \
    .getOrCreate()

# загружаем метрки из csv
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("output.csv/")
print("Исходный датафрейм")
df.show()

# drop столбцов в которых 1 уникальное значение
print("drop столбцов в которых 1 уникальное значение")
one_range_in_column = df.select([column for column in df.columns if df.agg(approxCountDistinct(column)).first()[0] == 1])
df = df.drop(*one_range_in_column.columns)
print("Удаляем вот эти столбцы", one_range_in_column)
print("Вот что получилось после удаления столцов с 1 уникальным значением")
df.show()

# replace Nan на Null
print("Меняем NaN на Null")
df = df.replace(float('nan'), None)
print("Вот что получилось после Меняем NaN на Null")
df.show()

# drop столбца time
print("Убрали столбец time")
df = df.drop('time')
print("Вот что получилось после того как убрали столбец time")
df.show()

# На вссякий случай строки со всеми Null тоже убираем
print("На всякий случайный удаляем строки со всеми Null(такого не должно быть, но я перестраховался)")
df = df.filter(~reduce(lambda x, y: x & y, [df[c].isNull() for c in df.columns]))
print("Вот что получилось после уборки строк Null")
df.show()

# drop столбцов которые кореллируются (corr>0.9) оставляем только один из них
print("Пришла пора убрать похожие столбцы (corr> 0.9)")
k = set()
for column in df.columns:
    for column2 in df.columns:
        if df.stat.corr(column, column2) > 0.9 and column != column2 and not (column in k):
            k.add(column2)
df = df.drop(*k)
print("Вот так уже лучше стало смотри")
df.show()

# меняем значение Null на стреднее по столбцу
print("Последние штрихи, меняем Null на Mean по столбцу")
fill_values = {column: df.agg({column:"mean"}).rdd.flatMap(list).collect()[0] for column in df.columns}
df = df.na.fill(fill_values)
print("Все прям космос, глянь какой классный dataframe")
df.show()

# Пора бы все сохранить в csv
df.write.csv('clearDataframeToK-Means.csv')
# df = spark.write.format("csv").option("header", "true").option("inferSchema", "true")

