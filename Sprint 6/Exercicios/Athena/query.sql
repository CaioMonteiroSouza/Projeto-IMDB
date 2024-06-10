WITH top_nomes AS (
    SELECT
        nome,
        ano,
        total,
        (ano / 10) * 10 AS decada,
        ROW_NUMBER() OVER (PARTITION BY (ano / 10) * 10 ORDER BY total DESC) AS top
    FROM
        meubanco.nomes
    WHERE ano > 1950
)
SELECT
    decada,
    nome,
    total
FROM
    top_nomes
WHERE
    top = 1
ORDER BY
    decada;