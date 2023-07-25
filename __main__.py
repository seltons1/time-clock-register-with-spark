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
    
    df = obj.create_dataframe_with_column(lines)

    data = [("alessandro","Monday","08:00:00","01:00:00"),
            ("alessandro","Tuesday","08:00:00","01:00:00")]
    
    schema = StructType([ \
        StructField("name",StringType(),True), \
        StructField("week_day",StringType(),True), \
        StructField("total_work",StringType(),True), \
        StructField("lunch_time", StringType(), True)
    ])

    df_1 = spark.createDataFrame(data=data, schema=schema)
    df_1.show()

    df_1.createOrReplaceTempView("hor")
    df.createOrReplaceTempView("data")

    spark.sql("select * from data d left join hor h on trim(h.name) = trim(d.name) and trim(h.week_day) = trim(d.week_day)").show(truncate=False)

    # Show dataframe
    #df.show(30,truncate=False)

    # Show dataframe schema
    #df.printSchema()

