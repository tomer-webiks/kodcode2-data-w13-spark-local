from pyspark.sql import SparkSession

def get_spark_session():
    return SparkSession.builder \
        .appName("Debugging Worker Errors") \
        .master("local[*]") \
        .getOrCreate()

spark_session = get_spark_session()