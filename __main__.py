# %%
from pyspark.sql import SparkSession
from delta import *
from format_file import FormatInDataFrame



if __name__ == '__main__':

    builder = SparkSession.builder.appName("Transform") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Start Spark Session
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Read and transform dataframe
    path = "files"
    obj = FormatInDataFrame(path, spark)
    df = obj.format()

    # Show dataframe
    df.show(truncate=False)

    # Show dataframe schema
    df.printSchema()

