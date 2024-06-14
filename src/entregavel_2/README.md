2) A segunda tarefa consiste em calcular o total líquido da empresa. Esse total é calculado da seguinte forma total_liquido = soma(total_bruto – desconto_percentual). O cálculo é baseado no conjunto de dados da <a href="https://drive.google.com/file/d/1vekbII5FYAB57mMTwU9I64XRCATD_XqF/view?usp=sharing">Figura 3</a>

![Figura 3](../../assets/Figura%203.png)

O resultado esperado é uma código com pyspark que retorne o total liquido da empresa que é 59973.46. 

## <a href="main.py">Resposta</a>
~~~python
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, when


class entregavel2:
    def __init__(self, transacoes_data) -> None:  # Passa os dados no construtor
        # Configura o Python para o Spark usar
        import os
        os.environ['PYSPARK_PYTHON'] = 'C:/Program Files/Python310/python.exe'

        # Configuração da sessão Spark
        self.spark = SparkSession.builder \
            .appName("Calculando Total Líquido") \
            .getOrCreate()

        # Armazena os dados de transações
        self.transacoes_data = transacoes_data

    def main(self):  # Remove o argumento transacoes
        # Criação do DataFrame
        df_transacoes = self.spark.createDataFrame(self.transacoes_data)

        # Cálculo do total líquido
        df_transacoes = df_transacoes.withColumn(
            "total_liquido",
            when(col("desconto_percentual").isNull(), col("total_bruto")).otherwise(
                col("total_bruto") * (1 - col("desconto_percentual") / 100)
            )
        )

        # Cálculo da soma do total líquido através de agregação
        total_liquido_empresa = df_transacoes.agg({"total_liquido": "sum"}).collect()[0][0]

        print(f"Total Líquido da Empresa:{round(total_liquido_empresa, 2)}")

    def __del__(self):
        # Parar a SparkSession
        self.spark.stop()

if __name__ == '__main__':
    # Dados de transações
    transacoes_data = [
        {'transacao_id': 1, 'total_bruto': 3000, 'desconto_percentual': 6.99},
        {'transacao_id': 2, 'total_bruto': 57989, 'desconto_percentual': 1.45},
        {'transacao_id': 4, 'total_bruto': 1, 'desconto_percentual': None},
        {'transacao_id': 5, 'total_bruto': 34, 'desconto_percentual': 0.0}
    ]

    # Cria um objeto da classe e chama o método main
    entregavel = entregavel2(transacoes_data)
    entregavel.main()  

~~~
