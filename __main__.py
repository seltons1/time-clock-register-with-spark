# %%
from pyspark.sql import SparkSession
from delta import *
from format_file import FormatInDataFrame
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
path = os.path.dirname(os.path.abspath(__file__))
print(path)


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

    df_1 = obj.create_df_employee_times()

    df_1.createOrReplaceTempView("hor")
    df.createOrReplaceTempView("data")

    # %%

    df_2 = spark.sql("select * from data d \
              left join hor h on trim(h.name) = trim(d.name) \
              and trim(h.week_day) = trim(d.week_day)")

    df_morning_work = obj.add_column(df, "unix_timestamp(leave_morning) - unix_timestamp(arrived_morning)","morning_work")
    df_afternoon_work = obj.add_column(df_morning_work, "unix_timestamp(leave_afternoon) - unix_timestamp(arrived_afternoon)","afternoon_work")
    df_lunch_time = obj.add_column(df_afternoon_work, "unix_timestamp(arrived_afternoon) - unix_timestamp(leave_morning)","lunch_time")
    df_total_work = obj.add_column(df_lunch_time, "(unix_timestamp(leave_morning) - unix_timestamp(arrived_morning))+(unix_timestamp(leave_afternoon) - unix_timestamp(arrived_afternoon))","total_work")

    # Show dataframe
    df_total_work.show(30,truncate=False)

    # Show dataframe schema
    df_total_work.printSchema()

    
'''
    output_path_reg = f'{path}\delta-table'
    output_path_hor = f'{path}\delta-table-hor'
    data_single_file_reg = df_with_hh_mm_ss.repartition(1)
    data_single_file_hor = df_1.repartition(1)
    #df.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(output_path)
    data_single_file_reg.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(output_path_reg)
    data_single_file_hor.write.format("delta").mode("overwrite").save(output_path_hor)
'''