
from pyspark.sql.functions import *
from pyspark.sql.types import *
class FormatInDataFrame():

    def __init__(self, path, spark):
        self.path = path
        self.spark = spark

    def format(self):

        data = self.spark.read.text(self.path, lineSep="\n")
        df_colunas = data.select(split(data['value'], '\t').alias('col'))
        df_final = df_colunas
        
        # Explode one column in various columns
        for i in range(7):
            df_final = df_final.withColumn(f"col{i+1}", regexp_replace(df_colunas['col'][i],'\r',''))
        
        # Remove first row, that conteins old columns names
        df_final = df_final.filter(df_final["col1"] != "No")
        
        # Drop old column
        df_final = df_final.drop(df_final["col"])
        
        # Drop another unused columns
        for i in [1,2,3,5,6]:
            df_final = df_final.drop(df_final[f"col{i}"])

        # Change column names to new names
        df_final = df_final.withColumnRenamed("col4","name")

        df_final = df_final.withColumn("time", split(df_final["col7"]," ").getItem(2))
        df_final = df_final.withColumn("day", split(df_final["col7"]," ").getItem(0))

        df_final = df_final.drop(df_final["col7"])

        # Pivotando as linhas em colunas por data
        df_pivot = df_final.groupBy("name").pivot("day").agg(collect_list("time"))
        df_pivot = df_pivot.drop('null')
    

        return df_pivot

    def create_dataframe_with_column(self, linhas):

        # Crie um DataFrame inicial
        schema = StructType([StructField("name", StringType(), nullable=False),
                            StructField("data", DateType(), nullable=False),
                            StructField("entrada_manha", StringType()),
                            StructField("saida_manha", StringType()),
                            StructField("entrada_tarde", StringType()),
                            StructField("saida_tarde", StringType())])
        
        df_final = self.spark.createDataFrame(linhas, schema)
        df_final = df_final.withColumn("arrived_morning", to_timestamp(df_final.entrada_manha))
        df_final = df_final.withColumn("leave_morning", to_timestamp(df_final.saida_manha))
        df_final = df_final.withColumn("arrived_afternoon", to_timestamp(df_final.entrada_tarde))
        df_final = df_final.withColumn("leave_afternoon", to_timestamp(df_final.saida_tarde))
        df_final = df_final.withColumn("week_day", date_format(df_final.data, "EEEE"))

        df_final.createOrReplaceTempView("REG")
        df = self.spark.sql("select name, data, arrived_morning, leave_morning, arrived_afternoon, leave_afternoon, \
                leave_morning-arrived_morning as morning_work, leave_afternoon-arrived_afternoon as afternoon_work, \
                morning_work+afternoon_work as total_work, arrived_afternoon-leave_morning as lunch_time, week_day from REG")

        return df