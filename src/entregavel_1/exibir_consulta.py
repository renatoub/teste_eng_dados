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
            SELECT
                c.contrato_id,
                cl.nome,
                CAST(SUM(t.valor_total * (1 - COALESCE(t.percentual_desconto, 0) / 100) * c.percentual / 100) AS DECIMAL(10, 2)) AS lucro
            FROM contrato c
            JOIN cliente cl ON cl.cliente_id = c.cliente_id
            JOIN transacao t ON c.contrato_id = t.contrato_id
            WHERE c.ativo = 1
            GROUP BY c.contrato_id, cl.nome
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