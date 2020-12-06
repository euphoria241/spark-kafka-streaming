import pyspark
import pandas
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, collect_set, mean, udf, approxCountDistinct, countDistinct
from pyspark.sql.types import BooleanType
from pyspark.ml.feature import VectorAssembler
from functools import reduce
from time import sleep
from pyspark.ml.stat import Correlation
from pyspark.ml.clustering import BisectingKMeans
import matplotlib.pyplot as plt
import numpy as np

spark = pyspark.sql.SparkSession.builder \
    .master("local") \
    .appName("extract-metrics") \
    .getOrCreate()

# загружаем метрки из csv
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("cleanDataFrame.csv")
print("Исходный датафрейм")

FEATURES_COL = df.columns
FEATURES_COL.pop(0)

for col in FEATURES_COL:
    df = df.withColumn(col, df[col].cast('float'))
df.show()
vecAssembler = VectorAssembler(inputCols=df.columns, outputCol="features")
df_kmeans = vecAssembler.transform(df).select('_c0', 'features')
df_kmeans.show()

# n - numbers of clusters
def search_opt_k(df_kmeans):
    # Trains a k-means model.
    df_kmeans.show()
    # найдем оптимальное k методом локтя
    cost = np.zeros(20)
    for k in range(2, 20):
        kmeans = BisectingKMeans().setK(k).setSeed(1).setFeaturesCol("features")
        print('kmeans ', kmeans)
        model = kmeans.fit(df_kmeans.sample(False, 0.1, seed=20))
        cost[k] = model.computeCost(df_kmeans)  # requires Spark 2.0 or later
    print(cost)
    # визуализируем локоть
    fig, ax = plt.subplots(1, 1, figsize=(8, 6))
    ax.plot(range(2, 20), cost[2:20])
    ax.set_xlabel('k')
    ax.set_ylabel('cost')
    plt.show()
search_opt_k(df_kmeans)

def clustering(df_kmeans, n):
    kmeans = BisectingKMeans().setK(n).setSeed(1).setFeaturesCol("features")
    print('kmeans ', kmeans)
    model = kmeans.fit(df_kmeans)

    centers = model.clusterCenters()

    print("Cluster Centers: ")
    for center in centers:
        print(center)
clustering(df_kmeans, 2)

# # drop столбцов в которых 1 уникальное значение
# print("drop столбцов в которых 1 уникальное значение")
# one_range_in_column = df.select([column for column in df.columns if df.agg(approxCountDistinct(column)).first()[0] == 1])
# df = df.drop(*one_range_in_column.columns)
# print("Удаляем вот эти столбцы", one_range_in_column)
# print("Вот что получилось после удаления столцов с 1 уникальным значением")
# df.show()
#
# # replace Nan на Null
# print("Меняем NaN на Null")
# df = df.replace(float('nan'), None)
# print("Вот что получилось после Меняем NaN на Null")
# df.show()
#
# # drop столбца time
# print("Убрали столбец time")
# df = df.drop('time')
# print("Вот что получилось после того как убрали столбец time")
# df.show()
#
# # На вссякий случай строки со всеми Null тоже убираем
# print("На всякий случайный удаляем строки со всеми Null(такого не должно быть, но я перестраховался)")
# df = df.filter(~reduce(lambda x, y: x & y, [df[c].isNull() for c in df.columns]))
# print("Вот что получилось после уборки строк Null")
# df.show()
#
# # drop столбцов которые кореллируются (corr>0.9) оставляем только один из них
# print("Пришла пора убрать похожие столбцы (corr> 0.9)")
# k = set()
# for column in df.columns:
#     for column2 in df.columns:
#         if df.stat.corr(column, column2) > 0.9 and column != column2 and not (column in k):
#             k.add(column2)
# df = df.drop(*k)
# print("Вот так уже лучше стало смотри")
# df.show()
#
# # меняем значение Null на стреднее по столбцу
# print("Последние штрихи, меняем Null на Mean по столбцу")
# fill_values = {column: df.agg({column:"mean"}).rdd.flatMap(list).collect()[0] for column in df.columns}
# df = df.na.fill(fill_values)
# print("Все прям космос, глянь какой классный dataframe")
# df.show()
#
# # Пора бы все сохранить в csv
# # df.write.csv('clearDataframeToK-Means.csv')
# df.toPandas().to_csv('cleanDataFrame.csv')