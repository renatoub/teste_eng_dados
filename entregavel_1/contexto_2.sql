USE [desafio_engenheiro];
GO

CREATE OR ALTER PROCEDURE [dbo].[pr_sel_lucro_contrato_ativo]
AS
BEGIN
    -- Usando uma CTE para evitar a criação de tabela temporária
    WITH CTE_Lucro AS (
        SELECT
            c.contrato_id,
            cl.nome,
            CAST(SUM(t.valor_total * (1 - COALESCE(t.percentual_desconto, 0) / 100) * c.percentual / 100) AS DECIMAL(10, 2)) AS lucro
        FROM contrato c
        JOIN cliente cl ON cl.cliente_id = c.cliente_id
        JOIN transacao t ON c.contrato_id = t.contrato_id
        WHERE c.ativo = 1
        GROUP BY c.contrato_id, cl.nome
    )
    -- Selecionando os dados da CTE
    SELECT
        nome,
        lucro
    FROM CTE_Lucro;
END;
GO