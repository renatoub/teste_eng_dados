3) O terceiro entregável consiste na transformação de dados disponíveis em <a href="hsrc/ttps://drive.google.com/file/d/1IDCjpDZh5St97jw4K_bAewJ8hf-rax9C/view?usp=sharing">arquivo Json</a> para o formato de dataframe, algo comum no dia a dia da empresa. Após transformar esse Json em dataframe é possível perceber que a coluna "item_list" está como dicionário. Seu gestor pediu dois pontos de atenção nessa tarefa:

- Expandir a coluna num mesmo dataframe;
- Normalizar os itens dessa coluna de dicionário e dividí-los em dois dataframes separados, seguindo o modelo relacional.

> Arquivo data.json 
>> ~~~JSON
>> [
>>    {
>>       "CreateDate":"2021-05-24T20:21:34.79",
>>       "EmissionDate":"2021-05-24T00:00:00",
>>       "Discount":0.0,
>>       "NFeNumber":501,
>>       "NFeID":1,
>>       "ItemList":[
>>          {
>>             "ProductName":"Rice",
>>             "Value":35.55,
>>             "Quantity":2
>>          },
>>          {
>>             "ProductName":"Flour",
>>             "Value":11.55,
>>             "Quantity":5
>>          },
>>          {
>>             "ProductName":"Bean",
>>             "Value":27.15,
>>             "Quantity":7
>>          }
>>       ]
>>    },
>>    {
>>       "CreateDate":"2021-05-24T20:21:34.79",
>>       "EmissionDate":"2021-05-24T00:00:00",
>>       "Discount":0.0,
>>       "NFeNumber":502,
>>       "NFeID":2,
>>       "ItemList":[
>>          {
>>             "ProductName":"Tomate",
>>             "Value":12.25,
>>             "Quantity":10
>>          },
>>          {
>>             "ProductName":"Pasta",
>>             "Value":7.55,
>>             "Quantity":5
>>          }
>>       ]
>>    },
>>    {
>>       "CreateDate":"2021-05-24T20:21:34.79",
>>       "EmissionDate":"2021-05-24T00:00:00",
>>       "Discount":0.0,
>>       "NFeNumber":503,
>>       "NFeID":3,
>>       "ItemList":[
>>          {
>>             "ProductName":"Beer",
>>             "Value":9.00,
>>             "Quantity":6
>>          },
>>          {
>>             "ProductName":"French fries",
>>             "Value":10.99,
>>             "Quantity":2
>>          },
>>          {
>>             "ProductName":"Ice cream",
>>             "Value":27.15,
>>             "Quantity":1
>>          }
>>       ]
>>    }
>> ]
>> ~~~

<a href ="main.py">Resposta</a>

> # Código
>> ~~~python
>> from pyspark.sql import SparkSession
>> from pyspark.sql.types import *
>> from pyspark.sql.functions import *
>> 
>> 
>> class entregavel3:
>>     def __init__(self, json) -> None:  # Passa os dados no construtor
>>         # Configura o Python para o Spark usar
>>         import os
>>         os.environ['PYSPARK_PYTHON'] = 'C:/Program Files/Python310/python.exe'
>> 
>>         # Configuração da sessão Spark
>>         self.spark = SparkSession.builder \
>>             .appName("Processando data.json") \
>>             .getOrCreate()
>> 
>>         # Armazena os dados de transações
>>         self.json = json
>> 
>>     def main(self):  # Remove o argumento transacoes
>>         try:
>>             # Criação do DataFrame          
>>             full_invoice = self.spark.read.option("multiline", "true").json(self.json) 
>> 
>>             # Expansão do ItemList e visualização da expansão em um mesmo DataFrame
>>             full_invoice = full_invoice.select('*', posexplode("ItemList").alias("ProductID", "Products_Items")).drop("ItemList")
>>             full_invoice.show()
>> 
>>             # Criação de um Dataframe somente de Notas Fiscais
>>             invoice = full_invoice.select("NFeID", "NFeNumber", "CreateDate", "EmissionDate")\
>>                                 .withColumnRenamed("NFeId", "NFe_Id")\
>>                                 .withColumnRenamed("NFeNumber", "NFe_Number")\
>>                                 .withColumnRenamed("CreateDate", "NFe_Create_Date")\
>>                                 .withColumnRenamed("EmissionDate", "NFe_Emission_Date").distinct().sort("NFe_Id")
>>             invoice.show()
>> 
>>             # Criação de um Dataframe somente com os produtos e seu relacionamento com notas fiscais.
>>             detail_invoice = full_invoice.withColumn("ProductName", col("Products_Items.ProductName"))\
>>                                         .withColumn("Value", col("Products_Items.Value"))\
>>                                         .withColumn("Quantity", col("Products_Items.Quantity"))\
>>                                         .withColumn("Final_Value", round(col("Value") * (1 - col("Discount")), 2))\
>>                                         .withColumnRenamed("NFeId", "NFe_Id")\
>>                                         .select("NFe_Id", "ProductID", "ProductName", "Quantity", "Value", "Discount", "Final_Value")\
>>                                         .sort("NFe_Id", asc("ProductID"))
>>             detail_invoice.show()
>> 
>>         except FileExistsError as e:
>>             print(f"Erro ao conectar com o arquivo JSON: {str(e)}")
>> 
>>         except Exception as e:
>>             print(f'Erro ao normalizar: {str(e)}')
>> 
>>     def __del__(self):
>>         # Parar a SparkSession
>>         self.spark.stop()
>> 
>> if __name__ == '__main__':
>>     # Cria um objeto da classe e chama o método main
>>     entregavel = entregavel3("./data.json")
>>     entregavel.main() 