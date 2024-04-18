SELECT
    tbvendas.cdpro,
    tbvendas.nmpro
FROM
    tbvendas
WHERE
    tbvendas.status = 'Concluído'
    AND tbvendas.dtven BETWEEN '2014-02-03' AND '2018-02-02'
GROUP BY
    tbvendas.cdpro
ORDER BY
    COUNT(*) DESC
LIMIT 1;
