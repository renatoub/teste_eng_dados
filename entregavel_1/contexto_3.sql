USE [desafio_engenheiro];
GO

CREATE OR ALTER PROCEDURE [dbo].[pr_sel_lucro_contrato_ativo]
AS
BEGIN
    -- Elimina a tabela temporária se ela existir, também pode 
    -- optar por outro nome na tabela temporária, para que possa 
    -- ser facilmente achada no DB.
    DROP TABLE IF EXISTS #temp1;

    -- Criar tabela temporária para armazenar dados intermediários
    -- O create table é opcional neste caso, pois podemos fazer 
    -- apenas INSERT INTO #temp1 FROM (-- subconsulta --), porém é
    -- altamente recomendado criar a tabela antes com as 
    -- especificações necessárias, assim evitando bug
    CREATE TABLE #temp1 (
        contrato_id INT,
        valor_com_desconto DECIMAL(18, 2)
    );

    -- Inserir dados na tabela temporária
    INSERT INTO #temp1 (contrato_id, valor_com_desconto)
    SELECT
        contrato_id,
        valor_total * (1 - COALESCE(percentual_desconto, 0) / 100) AS valor_com_desconto
    FROM transacao;

    -- Selecionar e calcular lucro
    SELECT
        cl.nome,
        CAST(SUM(t.valor_com_desconto * c.percentual / 100) AS DECIMAL(18, 2)) AS lucro
    FROM contrato c
    JOIN cliente cl ON cl.cliente_id = c.cliente_id
    JOIN #temp1 t ON c.contrato_id = t.contrato_id
    WHERE c.ativo = 1
    GROUP BY cl.nome;

    -- Está linha é opcional se quiser tratar a criação de subconsultas
    --intemediárias não deverás utilizá-la
    DROP TABLE #temp1;
END;