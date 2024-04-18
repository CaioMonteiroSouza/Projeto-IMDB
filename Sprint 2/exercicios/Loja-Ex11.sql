select
	vd.cdcli,
	vd.nmcli,
	sum(vd.qtd * vd.vrunt) as gasto
from tbvendas as vd
where vd.status  = 'Concluído'
group by vd.cdcli 
order by gasto DESC
limit 1