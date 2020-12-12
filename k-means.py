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
        model = kmeans.fit(df_kmeans.sample(False, 0.1, seed=100))
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
clustering(df_kmeans, 5)