select
	vend.nmvdd as vendedor,
	Sum(vd.qtd * vd.vrunt) as valor_total_vendas,
	ROUND((Sum(vd.qtd * vd.vrunt) * vend.perccomissao/100),2)  as comissao
from tbvendas as vd
left join tbvendedor as vend 
	on vd.cdvdd = vend.cdvdd 
where vd.status = 'Concluído'
group by vend.nmvdd 
order by comissao desc