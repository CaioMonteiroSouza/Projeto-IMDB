with vendas as (SELECT
	sum(vd2.qtd * vd2.vrunt) as valor_total_vendas2,
	vd2.cdvdd as codigo
	from tbvendas  as vd2
	where vd2.status = 'Concluído'
	group by vd2.cdvdd
	)




SELECT 
	dep.cddep,
	dep.nmdep,
	dep.dtnasc,
	min(v.valor_total_vendas2) as valor_total_vendas
from tbdependente as dep
left join tbvendas as vd
	on dep.cdvdd = vd.cdvdd
left join vendas as v
	on dep.cdvdd = v.codigo