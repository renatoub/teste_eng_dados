USE [desafio_engenheiro];
GO

SELECT
    c.contrato_id,
    cl.nome,
    CAST(SUM(t.valor_total * (1 - COALESCE(t.percentual_desconto, 0) / 100) * c.percentual / 100) AS DECIMAL(10, 2)) AS lucro
FROM contrato c
JOIN cliente cl ON cl.cliente_id = c.cliente_id
JOIN transacao t ON c.contrato_id = t.contrato_id
WHERE c.ativo = 1
GROUP BY c.contrato_id, cl.nome;
GO