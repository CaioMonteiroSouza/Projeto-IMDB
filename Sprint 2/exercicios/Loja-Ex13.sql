select
	cdpro,
	nmcanalvendas,
	nmpro,
	sum(qtd) as quantidade_vendas
from tbvendas as vd
where status = 'Concluído'
group by vd.cdpro, nmcanalvendas 
order by quantidade_vendas
limit 10