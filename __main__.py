# %%
from pyspark.sql import SparkSession
from delta import *
from format_file import FormatInDataFrame
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *



if __name__ == '__main__':

    builder = SparkSession.builder.appName("Transform") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Start Spark Session
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Read and transform dataframe
    path = "files"
    obj = FormatInDataFrame(path, spark)
    lines = obj.format()
    # %%
    df = obj.create_dataframe_with_column(lines)

    df_1 = obj.create_df_employee_times()

    df_1.createOrReplaceTempView("hor")
    df.createOrReplaceTempView("data")

    # %%

    df_2 = spark.sql("select * from data d \
              left join hor h on trim(h.name) = trim(d.name) \
              and trim(h.week_day) = trim(d.week_day)")

    #df_with_timestamp_in_seconds = df_2.withColumn("timestamp_in_seconds", expr("lunch_time * 86400"))
    #df_with_timestamp = df_2.withColumn("timestamp", df_2["lunch_time"].cast(TimestampType()))
    # Show dataframe
    df.show(30,truncate=False)

    # Show dataframe schema
    df.printSchema()

