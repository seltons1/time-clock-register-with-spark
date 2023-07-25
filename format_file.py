
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta

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

        lines = self.explode_columns(df_pivot)
        return lines

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
    
    def explode_columns(self, df):

        colunas = df.columns
        linhas_list = []
        for coluna in colunas:

            if coluna != "name":

                # Explode columns
                df_exploded = df.select("name", coluna).withColumn("elemento", explode(coluna))
                df1_exploded = df.select("name", coluna)

                col_name_elem = df_exploded.select("elemento", "name").collect()
            
                linhas = df.select("name", coluna).collect()


                for linha in linhas:
                    lista_horarios = linha.__getitem__(coluna)
                    index = 0
                    entrada_manha = datetime.strptime('00:00:00', "%H:%M:%S").time()
                    saida_manha = datetime.strptime('00:00:00', "%H:%M:%S").time()
                    entrada_tarde = datetime.strptime('00:00:00', "%H:%M:%S").time()
                    saida_tarde = datetime.strptime('00:00:00', "%H:%M:%S").time()
                
                    for horario in lista_horarios:
                        
                        horario_time = datetime.strptime(horario, "%H:%M:%S").time()
                        # Morning in
                        if index == 0:
                            entrada_manha = horario
                        # Morning leave
                        if index == 1:
                            saida_manha = horario
                        # Afternoon in
                        if index == 2:
                            entrada_tarde = horario
                        # Afternoon leave
                        if index == 3:
                            saida_tarde = horario
                        
                        index += 1

                    data = datetime.strptime(coluna, "%Y/%m/%d").date()
                    linhas_list.append(Row(name=str(linha.__getitem__("name")), data=data, entrada_manha=entrada_manha, saida_manha=saida_manha, entrada_tarde=entrada_tarde, saida_tarde=saida_tarde ))
            
        return linhas_list
        

    def create_df_employee_times(self):

        data = [("alessandro","Monday","08:00:00","01:00:00"),
                ("alessandro","Tuesday","08:00:00","01:00:00"),
                ("alessandro","Wednesday","08:00:00","01:00:00"),
                ("alessandro","Thursday","08:00:00","01:00:00"),
                ("alessandro","Friday","08:00:00","01:00:00"),
                ("alessandro","Saturday","08:00:00","01:00:00"),

                ("antonio","Monday","08:00:00","01:00:00"),
                ("antonio","Tuesday","08:00:00","01:00:00"),
                ("antonio","Wednesday","08:00:00","01:00:00"),
                ("antonio","Thursday","08:00:00","01:00:00"),
                ("antonio","Friday","08:00:00","01:00:00"),
                ("antonio","Saturday","08:00:00","01:00:00"),

                ("heloisa","Monday","08:00:00","01:00:00"),
                ("heloisa","Tuesday","08:00:00","01:00:00"),
                ("heloisa","Wednesday","08:00:00","01:00:00"),
                ("heloisa","Thursday","08:00:00","01:00:00"),
                ("heloisa","Friday","08:00:00","01:00:00"),
                ("heloisa","Saturday","08:00:00","01:00:00"),

                ("fernando","Monday","08:00:00","01:00:00"),
                ("fernando","Tuesday","08:00:00","01:00:00"),
                ("fernando","Wednesday","08:00:00","01:00:00"),
                ("fernando","Thursday","08:00:00","01:00:00"),
                ("fernando","Friday","08:00:00","01:00:00"),
                ("fernando","Saturday","08:00:00","01:00:00"),

                ("maxwell","Monday","08:00:00","01:00:00"),
                ("maxwell","Tuesday","08:00:00","01:00:00"),
                ("maxwell","Wednesday","08:00:00","01:00:00"),
                ("maxwell","Thursday","08:00:00","01:00:00"),
                ("maxwell","Friday","08:00:00","01:00:00"),
                ("maxwell","Saturday","08:00:00","01:00:00"),

                ("ricardo","Monday","08:00:00","01:00:00"),
                ("ricardo","Tuesday","08:00:00","01:00:00"),
                ("ricardo","Wednesday","08:00:00","01:00:00"),
                ("ricardo","Thursday","08:00:00","01:00:00"),
                ("ricardo","Friday","08:00:00","01:00:00"),
                ("ricardo","Saturday","08:00:00","01:00:00")]
    
        schema = StructType([ \
            StructField("name",StringType(),True), \
            StructField("week_day",StringType(),True), \
            StructField("total_work",StringType(),True), \
            StructField("lunch_time", StringType(), True)
        ])

        df_1 = self.spark.createDataFrame(data=data, schema=schema)
        return df_1