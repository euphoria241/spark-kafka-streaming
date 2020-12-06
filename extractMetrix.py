from pyspark.sql import SparkSession
import prometheus_api_client as pac
import pyspark
import pandas
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


def dataframe_creation():
    """ Method to create spark dataframe from historical data in prometheus
    and save it to 'metrics' folder in root directory of this file

    Returns:
        [SparkDataFrame]
    """

    # connect to local prometheus server
    prometheus_connection = pac.PrometheusConnect(url="http://localhost:9090/", disable_ssl=True)

    # collect all metric names from prometheus
    metric_names = prometheus_connection.all_metrics()

    # set time range to collect metrics
    _from = pac.utils.parse_datetime("1d")
    _to = pac.utils.parse_datetime("now")

    # initialize empty dataframe
    dataframe = None

    # loop through all metric names
    for metric_name in metric_names:
        # request metric values
        metric_data = prometheus_connection.get_metric_range_data(
            metric_name,
            start_time=_from,
            end_time=_to
        )

        # check requested metric job
        if metric_data and metric_data[0]['metric']['job'] == 'alekseyarkhipov':

            # collect matric values
            values = metric_data[0]['values']

            # create new dataframe or if exists merge new columns to it
            if dataframe is None:
                dataframe = spark.createDataFrame(values, ['time', metric_name])
            else:
                df = spark.createDataFrame(values, ['time', metric_name])
                dataframe = dataframe.join(df, 'time', 'right')
    
    # save dataframe to 'metrics' folder in root directory
    dataframe.write.csv('metrics')

    return dataframe


def clean_dataframe(df):
    # drop столбцов в которых 1 уникальное значение
    print("drop столбцов в которых 1 уникальное значение")
    one_range_in_column = df.select(
        [column for column in df.columns if df.agg(approxCountDistinct(column)).first()[0] == 1])
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

    # Преобразуем все данные в float
    for col in df.columns:
        df = df.withColumn(col, df[col].cast('float'))

    print("Вот так уже лучше стало смотри")
    df.show()
    return df

# Создает Dataframe из Prometheus
df = dataframe_creation()

# Очищает DataFrame для K-means
df = clean_dataframe(df)



