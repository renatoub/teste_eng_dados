from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


class entregavel3:
    def __init__(self, json) -> None:  # Passa os dados no construtor
        # Configura o Python para o Spark usar
        import os
        os.environ['PYSPARK_PYTHON'] = 'C:/Program Files/Python310/python.exe'

        # Configuração da sessão Spark
        self.spark = SparkSession.builder \
            .appName("Processando data.json") \
            .getOrCreate()

        # Armazena os dados de transações
        self.json = json

    def main(self):  # Remove o argumento transacoes
        try:
            # Criação do DataFrame          
            full_invoice = self.spark.read.option("multiline", "true").json(self.json) 

            # Expansão do ItemList e visualização da expansão em um mesmo DataFrame
            full_invoice = full_invoice.select('*', posexplode("ItemList").alias("ProductID", "Products_Items")).drop("ItemList")
            full_invoice.show()

            # Criação de um Dataframe somente de Notas Fiscais
            invoice = full_invoice.select("NFeID", "NFeNumber", "CreateDate", "EmissionDate")\
                                .withColumnRenamed("NFeId", "NFe_Id")\
                                .withColumnRenamed("NFeNumber", "NFe_Number")\
                                .withColumnRenamed("CreateDate", "NFe_Create_Date")\
                                .withColumnRenamed("EmissionDate", "NFe_Emission_Date").distinct().sort("NFe_Id")
            invoice.show()

            # Criação de um Dataframe somente com os produtos e seu relacionamento com notas fiscais.
            detail_invoice = full_invoice.withColumn("ProductName", col("Products_Items.ProductName"))\
                                        .withColumn("Value", col("Products_Items.Value"))\
                                        .withColumn("Quantity", col("Products_Items.Quantity"))\
                                        .withColumn("Final_Value", round(col("Value") * (1 - col("Discount")), 2))\
                                        .withColumnRenamed("NFeId", "NFe_Id")\
                                        .select("NFe_Id", "ProductID", "ProductName", "Quantity", "Value", "Discount", "Final_Value")\
                                        .sort("NFe_Id", asc("ProductID"))
            detail_invoice.show()

        except FileExistsError as e:
            print(f"Erro ao conectar com o arquivo JSON: {str(e)}")

        except Exception as e:
            print(f'Erro ao normalizar: {str(e)}')

    def __del__(self):
        # Parar a SparkSession
        self.spark.stop()

if __name__ == '__main__':
    # Cria um objeto da classe e chama o método main
    entregavel = entregavel3("./data.json")
    entregavel.main() 