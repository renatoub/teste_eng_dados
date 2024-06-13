import pandas as pd
import pyodbc

# Configurações de conexão com o SQL Server
server = 'seu_servidor'
database = 'seu_banco_de_dados'
username = 'seu_usuario'
password = 'sua_senha'

# String de conexão
conn_str = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Query SQL para executar
sql_query = '''
            EXEC pr_sel_lucro_contrato_ativo
            '''

try:
    # Conectar ao banco de dados
    conn = pyodbc.connect(conn_str)
    
    # Executar a consulta SQL e carregar os resultados em um DataFrame
    df = pd.read_sql(sql_query, conn)
    
    # Fechar a conexão
    conn.close()
    
    # Exibir o DataFrame
    print(df)

except Exception as e:
    print(f'Erro ao executar a consulta SQL: {e}')