# %%
from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import split, regexp_replace


if __name__ == '__main__':

    builder = SparkSession.builder.appName("Transform") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Start Spark Session
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Read and transform dataframe
    data = spark.read.text("files/", lineSep="\n")
    df_colunas = data.select(split(data['value'], '\t').alias('colunas'))
    df_final = df_colunas
    
    # Explode one column in various columns
    for i in range(7):
        df_final = df_final.withColumn(f"coluna{i+1}", regexp_replace(df_colunas['colunas'][i],'\r',''))
    
    # Remove first row, that conteins old columns names
    df_final = df_final.filter(df_final["coluna1"] != "No")
    
    # Drop old column
    df_final = df_final.drop(df_final["colunas"])
    
    # Drop another unused columns
    for i in [1,2,3,5,6]:
        df_final = df_final.drop(df_final[f"coluna{i}"])

    # Show dataframe
    df_final.show(truncate=False)

    # Show dataframe schema
    df_final.printSchema()

