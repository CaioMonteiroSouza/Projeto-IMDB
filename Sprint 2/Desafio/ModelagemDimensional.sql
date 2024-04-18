create view dim_cliente as
select 
	tc.idCliente,
	tc.nomeCliente,
	te.nomeCidade as cidadeCliente,
	te.nomeEstado as estadoCliente,
	te.nomePais as paisCliente
from tb_cliente tc
left join tb_endereco te 
	on tc.idEndereco = te.idEndereco
		
create view dim_carro as
select
	tc.idCarro,
	tc.classiCarro,
	tc.marcaCarro,
	tc.modeloCarro,
	tc.anoCarro,
	tc2.idcombustivel
from tb_carro tc 
left join tb_combustivel tc2 
	on tc.idCombustivel = tc2.idCombustivel 
	
create view dim_tempoLocacao as 
select 
	tl.dataLocacao as idTempolocacao,
	STRFTIME('%Y', tl.dataLocacao) AS ano,
	STRFTIME('%m', tl.dataLocacao) AS mes,
    STRFTIME('%d', tl.dataLocacao) AS dia,
    tl.HoraLocacao
from tb_locacao tl

create view dim_tempoEntrega as 
select
	tl.dataEntrega as idTempoEntrega,
	STRFTIME('%Y', tl.dataEntrega) AS ano,
	STRFTIME('%m', tl.dataEntrega) AS mes,
    STRFTIME('%d', tl.dataEntrega) AS dia,
    tl.HoraEntrega
from tb_locacao tl

create view dim_vendedor as
select
	tv.idVendedor,
	tv.nomeVendedor,
	tv.sexoVendedor,
	tv.estadoVendedor
from tb_vendedor tv 

create view fato as 
select
	tl.idLocacao,
	tl.idCliente,
	tl.idCarro,
	tl.DataLocacao as idTempoLocacao,
	tl.qtdDiaria as quantidade,
	tl.vlrDiaria as ValorUnitario,
	tl.kmLocacao as kmLocacao,
	tl.dataEntrega as idTempoEntrega,
	tl.idVendedor as idVendedor
from tb_locacao tl 

drop view fato

select * from fato 

select * from dim_vendedor 

select * from dim_tempoEntrega 

select * from dim_tempoLocacao 

select * from dim_carro

select * from dim_cliente 