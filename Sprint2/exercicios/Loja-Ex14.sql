select
	estado,
	round(avg(qtd * vrunt),2) as gastomedio
from tbvendas t
where status = 'Concluído' 
group by estado
order by gastomedio desc