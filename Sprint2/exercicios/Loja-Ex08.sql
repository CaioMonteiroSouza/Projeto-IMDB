with quantia as (
	select
	count(*) as quantidade,
	tbvendas.cdvdd as codigo
	from tbvendas
	where tbvendas.status = 'Concluído'
	group by tbvendas.cdvdd 
)

select
	tbvendas.cdvdd,
	tbvendedor.nmvdd 
from tbvendas 
left join tbvendedor
	on tbvendas.cdvdd = tbvendedor.cdvdd 
left join quantia
	on tbvendas.cdvdd = quantia.codigo
where quantia.quantidade = (
	select 
		max(quantia.quantidade)
	from quantia
)
limit 1
