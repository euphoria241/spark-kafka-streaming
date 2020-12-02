from pyspark.sql import SparkSession
import prometheus_api_client as pac
import pyspark

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