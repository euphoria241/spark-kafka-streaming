from pyspark.sql.session import SparkSession
from pyspark.sql.functions import explode, split, from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.ml.feature import VectorAssembler



spark = SparkSession \
        .builder \
        .appName("spark-kafka-app").getOrCreate()
        
# set log level to remove WARNS from cmd
spark.sparkContext.setLogLevel("ERROR")


df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "spark-topic") \
    .load()\
    .selectExpr("CAST(value AS STRING)")

#разобраться с метриками
# TODO: get schema of data from historical data!
schema = StructType([
    StructField("java_lang_ClassLoading_LoadedClassCount", StringType()),
    StructField("java_lang_Compilation_TotalCompilationTime", StringType()),
    StructField("java_lang_Memory_HeapMemoryUsage_used", StringType()),
    StructField("java_lang_OperatingSystem_FreePhysicalMemorySize", StringType()),
    StructField("java_lang_OperatingSystem_FreeSwapSpaceSize", StringType()),
    StructField("java_lang_OperatingSystem_ProcessCpuLoad", StringType()),
    StructField("java_lang_OperatingSystem_SystemCpuLoad", StringType()),
    StructField("java_lang_Runtime_Uptime", StringType()),
    StructField("java_lang_Threading_CurrentThreadUserTime", StringType()),
    StructField("java_lang_Threading_TotalStartedThreadCount", StringType()),
    StructField("jvm_memory_bytes_used", StringType()),
    StructField("scrape_duration_seconds", StringType()),
])

centers = [[2.01750000e+03,5.62325000e+03,2.05673008e+07, 5.65267456e+08,
 2.04043919e+09, 6.37960093e-04, 6.41255268e-01, 2.16990225e+06,
 4.36718752e+08, 1.88500000e+01, 2.08694620e+07, 6.87320352e-02],
[1.97633333e+03, 4.93009091e+03, 2.21628485e+07, 8.89927866e+08,
 1.70491581e+09, 5.64451044e-04, 5.09718462e-01, 1.47776488e+06,
 3.74526516e+08, 1.75454545e+01, 2.05545535e+07, 5.76015185e-02],
[1.93941071e+03, 3.65105357e+03, 2.20412631e+07,1.19414652e+09,
 2.05431771e+09, 1.09885792e-03, 3.70528207e-01, 6.78368214e+05,
 1.49553571e+08, 1.68928571e+01, 2.17075150e+07, 6.47477107e-02],
[1.98952941e+03, 4.79100000e+03, 2.22639318e+07, 1.11018601e+09,
 2.18873525e+09, 7.27119009e-04, 4.27478746e-01, 1.54096138e+06,
 4.23253673e+08, 1.79705882e+01, 2.05801294e+07, 4.56160535e-01],
[2.05712500e+03, 5.55806250e+03, 2.07176045e+07, 1.03655270e+09,
 2.55286285e+09, 6.42902910e-04, 4.02331106e-01, 2.19999566e+06,
 4.62402338e+08, 1.93437500e+01, 2.07930172e+07, 5.81395843e-02],
[2.21100000e+03, 6.41293333e+03, 2.16811835e+07, 1.04955822e+09,
 2.97510762e+09, 4.86057383e-04, 3.66010160e-01, 3.19590173e+06,
 6.12499999e+08, 2.41666667e+01, 2.16086595e+07, 6.01570634e-02],
[1.94888571e+03, 5.33860000e+03, 2.00092105e+07, 1.87186135e+09,
 2.50836606e+09, 3.94808290e-03, 3.32638546e-01, 2.09469240e+06,
 4.41964284e+08, 1.68857143e+01, 2.08556846e+07, 8.86640222e-02]]
print(centers)

json_df = df.withColumn("jsonData", from_json(col("value"), schema)).select("jsondata.*")
for col in json_df.columns:
    json_df = json_df.withColumn(col, json_df[col].cast('float'))

def process_row(row):
    row = [float(i) for i in list(row)]
    print(row)

    import math 
    min = math.inf
    index = math.inf
    for j in range(len(centers)):
        sum = 0
        for i in range(len(centers[j])):
            sum += (row[i]-centers[j][i])**2
        if sum < min:
            min = sum
            index = j
    print("Distance:", min, "\nCluster:", index)


json_df.writeStream.foreach(process_row).start().awaitTermination()


