## Entregáveis

1) Sua primeira tarefa consiste em escrever uma aplicação para calcular o ganho total da empresa, o qual é obtido a partir da taxa administrativa do serviço de cartão de crédito para seus clientes. Esse ganho é calculado sobre um percentual das transações de cartão de crédito realizadas por eles. O cálculo é baseado no conjunto de dados abaixo, transacao, contrato e cliente da <a href="https://drive.google.com/file/d/1lA2eLHNMoMpApPGz6h7WQpphT9URWxB1/view?usp=sharing">Figura 1</a>.

![Figura 1](../assets/Figura%201.png)

O resultado esperado é uma consulta que retorne o ganho total da empresa por cliente que é 1.198,77 para o cliente A e 1,08 para o cliente B, conforme a <a href="https://drive.google.com/file/d/1KJ9SvkcRX94YQDyKI01ivG-5N3lZp3T1/view?usp=sharing">Figura 2</a>.

![Figura 2](../assets/Figura%202.png)

Assim sendo, seguem <a href="https://drive.google.com/file/d/1lqZZb9WgkyyL7qBZ5ZAPENVYoioK2hMs/view?usp=sharing">snippet de código</a> para criação da base de dados e dos dados exemplos (via SQL Server).

~~~SQL
create database desafio_engenheiro
go
use desafio_engenheiro
go
create table cliente (
cliente_id bigint primary key not null identity(1,1),
nome varchar(30) not null
)
go
insert cliente values ('Cliente A')
insert cliente values ('Cliente B')
insert cliente values ('Cliente C')
insert cliente values ('Cliente D')
go
create table contrato (
contrato_id bigint primary key not null identity(1,1),
ativo bit not null,
percentual numeric(10,2) not null,
cliente_id bigint not null foreign key references cliente(cliente_id)
)
go
insert into contrato values (1, 2, 1)
insert into contrato values (0, 1.95, 1)
insert into contrato values (1, 1, 2)
insert into contrato values (1, 3, 4)
go

create table transacao (
transacao_id bigint primary key not null identity(1,1),
contrato_id bigint not null foreign key references contrato(contrato_id),
valor_total money not null,
percentual_desconto numeric(10,2) null
)
go
insert into transacao values (1, 3000, 6.99)
insert into transacao values (2, 4500, 15)
insert into transacao values (1, 57989, 1.45)
insert into transacao values (4, 1, 0)
insert into transacao values (4, 35, null)
go
~~~

## Resposta
Como nesse entregavel temos um acesso direto ao banco de dados, escolhi utilizar SQL, podendo ser adicionado a um script python com facilidade (Ao final das respostas adiciono um código Python imprimindo a informação pedida em um DataFrame) e dependendo do contexto eu utilizaria 3 abordagens diferentes.

> ### <a href="contexto_1.sql">Primeira abordagem</a>
> Caso meu contexto seja execução única, utilizaria uma abordagem de consulta simples SQL, conforme descrito abaixo:
>>  ~~~SQL
>> USE [desafio_engenheiro];
>> GO
>> 
>> SELECT
>>     c.contrato_id,
>>     cl.nome,
>>     CAST(SUM(t.valor_total * (1 - COALESCE(t.percentual_desconto, 0) / 100) * c.percentual / 100) AS DECIMAL(10, 2)) AS lucro
>> FROM contrato c
>> JOIN cliente cl ON cl.cliente_id = c.cliente_id
>> JOIN transacao t ON c.contrato_id = t.contrato_id
>> WHERE c.ativo = 1
>> GROUP BY c.contrato_id, cl.nome;
>> GO
>> ~~~
>> 
> ### <a href="contexto_2.sql">Segunda abordagem</a>
> Caso meu contexto seja para mais de uma execução criaria uma procedure que pudesse ser chamada quando necessária:
>> ~~~SQL
>> USE [desafio_engenheiro];
>> GO
>> 
>> CREATE OR ALTER PROCEDURE [dbo].[pr_sel_lucro_contrato_ativo]
>> AS
>> BEGIN
>>     -- Usando uma CTE para evitar a criação de tabela temporária
>>     WITH CTE_Lucro AS (
>>         SELECT
>>             c.contrato_id,
>>             cl.nome,
>>             CAST(SUM(t.valor_total * (1 - COALESCE(t.percentual_desconto, 0) / 100) * c.percentual / 100) AS DECIMAL(10, 2)) AS lucro
>>         FROM contrato c
>>         JOIN cliente cl ON cl.cliente_id = c.cliente_id
>>         JOIN transacao t ON c.contrato_id = t.contrato_id
>>         WHERE c.ativo = 1
>>         GROUP BY c.contrato_id, cl.nome
>>     )
>>     -- Selecionando os dados da CTE
>>     SELECT
>>         nome,
>>         lucro
>>     FROM CTE_Lucro;
>> END;
>> GO
>> ~~~
>>
> ### <a href="contexto_3.sql">Terceira abordagem</a>
> Caso precisar de rastreabilidade entre as subconsultas, muito bom quando se tem subconsultas complexas, segue um exemplo abaixo de como seria uma procedure
>> ~~~SQL
>> USE [desafio_engenheiro];
>> GO
>> 
>> CREATE OR ALTER PROCEDURE [dbo].[pr_sel_lucro_contrato_ativo]
>> AS
>> BEGIN
>>     -- Elimina a tabela temporária se ela existir, também pode 
>>     -- optar por outro nome na tabela temporária, para que possa 
>>     -- ser facilmente achada no DB.
>>     DROP TABLE IF EXISTS #temp1;
>> 
>>     -- Criar tabela temporária para armazenar dados intermediários
>>     -- O create table é opcional neste caso, pois podemos fazer 
>>     -- apenas INSERT INTO #temp1 FROM (-- subconsulta --), porém é
>>     -- altamente recomendado criar a tabela antes com as 
>>     -- especificações necessárias, assim evitando bug
>>     CREATE TABLE #temp1 (
>>         contrato_id INT,
>>         valor_com_desconto DECIMAL(18, 2)
>>     );
>> 
>>     -- Inserir dados na tabela temporária
>>     INSERT INTO #temp1 (contrato_id, valor_com_desconto)
>>     SELECT
>>         contrato_id,
>>         valor_total * (1 - COALESCE(percentual_desconto, 0) / 100) AS valor_com_desconto
>>     FROM transacao;
>> 
>>     -- Selecionar e calcular lucro
>>     SELECT
>>         cl.nome,
>>         CAST(SUM(t.valor_com_desconto * c.percentual / 100) AS DECIMAL(18, 2)) AS lucro
>>     FROM contrato c
>>     JOIN cliente cl ON cl.cliente_id = c.cliente_id
>>     JOIN #temp1 t ON c.contrato_id = t.contrato_id
>>     WHERE c.ativo = 1
>>     GROUP BY cl.nome;
>>
>>     -- Está linha é opcional se quiser tratar a criação de subconsultas
>>     --intemediárias não deverás utilizá-la
>>     DROP TABLE #temp1;
>> END;
>> ~~~
>>
> ### Por fim
> Se utilizou a criação de procedure, utilize abaixo o código para selecioná-la no SGDB
>> ~~~SQL
>> USE [desafio_engenheiro];
>> GO
>> 
>> EXEC [dbo].[pr_sel_lucro_contrato_ativo];
>> GO
>> ~~~

Exemplo de código Python para exibir os dados solicitados:

> Buscando pelo consulta SQL:
>> ~~~python
>> import pandas as pd
>> import pyodbc
>> 
>> # Configurações de conexão com o SQL Server
>> server = 'seu_servidor'
>> database = 'seu_banco_de_dados'
>> username = 'seu_usuario'
>> password = 'sua_senha'
>> 
>> # String de conexão
>> conn_str = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}'
>> 
>> # Query SQL para executar
>> sql_query = '''
>>             EXEC [dbo].[pr_sel_lucro_contrato_ativo];
>>             '''
>> 
>> try:
>>     # Conectar ao banco de dados
>>     conn = pyodbc.connect(conn_str)
>>     
>>     # Executar a consulta SQL e carregar os resultados em um DataFrame
>>     df = pd.read_sql(sql_query, conn)
>>     
>>     # Fechar a conexão
>>     conn.close()
>>     
>>     # Exibir o DataFrame
>>     print(df)
>> 
>> except Exception as e:
>>     print(f'Erro ao executar a consulta SQL: {e}')
>> 
>> ~~~
> Buscando pelo procedure:
>> ~~~python
>> import pandas as pd
>> import pyodbc
>> 
>> # Configurações de conexão com o SQL Server
>> server = 'seu_servidor'
>> database = 'seu_banco_de_dados'
>> username = 'seu_usuario'
>> password = 'sua_senha'
>> 
>> # String de conexão
>> conn_str = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}'
>> 
>> # Query SQL para executar
>> sql_query = '''
>>             EXEC pr_sel_lucro_contrato_ativo;
>>             '''
>> 
>> try:
>>     # Conectar ao banco de dados
>>     conn = pyodbc.connect(conn_str)
>>     
>>     # Executar a consulta SQL e carregar os resultados em um DataFrame
>>     df = pd.read_sql(sql_query, conn)
>>     
>>     # Fechar a conexão
>>     conn.close()
>>     
>>     # Exibir o DataFrame
>>     print(df)
>> 
>> except Exception as e:
>>     print(f'Erro ao executar a consulta SQL: {e}')
>> 
>> ~~~