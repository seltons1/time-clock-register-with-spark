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
    df = obj.format()

    colunas = df.columns
    df_final = None
    linhas_list = []
    for coluna in colunas:
        if coluna != "name":

            # Explode a lista em elementos individuais
            df_exploded = df.select("name", coluna).withColumn("elemento", explode(coluna))
            df1_exploded = df.select("name", coluna)

            col_name_elem = df_exploded.select("elemento", "name").collect()
        
            linhas = df.select("name", coluna).collect()

            for linha in linhas:
                #print(linha.__getitem__("name"))
                lista_horarios = linha.__getitem__(coluna)
                #print(lista_horarios)
                index = 0
                entrada_manha = datetime.strptime('00:00:00', "%H:%M:%S").time()
                saida_manha = datetime.strptime('00:00:00', "%H:%M:%S").time()
                entrada_tarde = datetime.strptime('00:00:00', "%H:%M:%S").time()
                saida_tarde = datetime.strptime('00:00:00', "%H:%M:%S").time()
            
                for horario in lista_horarios:
                    
                    horario_time = datetime.strptime(horario, "%H:%M:%S").time()
                    # Entrada manhã
                    if index == 0:
                        entrada_manha = horario
                    # Saída manhã
                    if index == 1:
                        saida_manha = horario
                    # Entrada tarde
                    if index == 2:
                        entrada_tarde = horario
                    # Saída manhã
                    if index == 3:
                        saida_tarde = horario
                    
                    index += 1

                data = datetime.strptime(coluna, "%Y/%m/%d").date()
                linhas_list.append(Row(name=str(linha.__getitem__("name")), data=data, entrada_manha=entrada_manha, saida_manha=saida_manha, entrada_tarde=entrada_tarde, saida_tarde=saida_tarde ))
    
    df = obj.create_dataframe_with_column(linhas_list)

    # Show dataframe
    df.show(30,truncate=False)

    # Show dataframe schema
    df.printSchema()

