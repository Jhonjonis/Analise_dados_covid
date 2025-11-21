from pyspark.sql import SparkSession

def create_spark(app_name="BigDataApp"):
    return (
        SparkSession.builder
        .appName(app_name)
        .master("spark://spark-master:7077")
        .config("spark.executor.memory", "512m")
        .config("spark.executor.cores", "1")
        .getOrCreate()
    )
