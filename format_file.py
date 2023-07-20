
from pyspark.sql.functions import split, regexp_replace

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
        df_final = df_final.withColumnRenamed("col7","datetime")

        return df_final

