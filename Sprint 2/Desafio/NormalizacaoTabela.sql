CREATE TABLE tb_carro (
	idCarro int PRIMARY KEY,
	classiCarro varchar(50),
	marcaCarro varchar(20),
	modeloCarro varchar(20),
	anoCarro date,
	idCombustivel int,
	FOREIGN KEY (idCombustivel) REFERENCES tb_combustivel(idCombustivel)
)

INSERT INTO tb_carro (idCarro, classiCarro, marcaCarro, modeloCarro, anoCarro, idCombustivel)
SELECT DISTINCT
	tl.idCarro,
	tl.classiCarro,
	tl.marcaCarro,
	tl.modeloCarro,
	tl.anoCarro,
	tc.idCombustivel
FROM tb_locacao tl 
LEFT JOIN tb_combustivel tc 
	ON tl.idcombustivel = tc.idCombustivel 



CREATE TABLE tb_combustivel(
	idCombustivel int PRIMARY KEY,
	tipoCombustivel varchar(10)
)

INSERT INTO tb_combustivel (idCombustivel, tipoCombustivel)
	SELECT DISTINCT 
		tl.idcombustivel,
		tl.tipoCombustivel 
	FROM tb_locacao tl 

	
CREATE TABLE tb_endereco (
	idEndereco INTEGER PRIMARY KEY,
	nomeCidade varchar(30),
	nomeEstado varchar(15),
	nomePais varchar(30)
)

INSERT INTO tb_endereco (nomeCidade, nomeEstado, nomePais)
SELECT DISTINCT 
	cidadeCliente,
	estadoCliente,
	paisCliente
FROM tb_locacao tl 

CREATE TABLE tb_cliente ( 
	idCliente INTEGER PRIMARY KEY,
	nomeCliente varchar(30),
	idEndereco int,
	FOREIGN KEY (idEndereco) REFERENCES tb_endereco(idEndereco)
)

INSERT INTO tb_cliente (idCliente, nomeCliente, idEndereco)
SELECT DISTINCT 
	tl.idCliente,
	tl.nomeCliente,
	te.idEndereco
FROM tb_locacao tl 
JOIN tb_endereco te 
	ON tl.cidadeCliente = te.nomeCidade 
	
CREATE TABLE tb_vendedor (
	idVendedor int PRIMARY KEY,
	nomeVendedor varchar(30),
	sexoVendedor int,
	estadoVendedor varchar(15)
)

INSERT INTO tb_vendedor (idVendedor, nomeVendedor, sexoVendedor, estadoVendedor)
SELECT DISTINCT 
	idVendedor,
	nomeVendedor,
	sexoVendedor,
	estadoVendedor
FROM tb_locacao

ALTER TABLE tb_locacao RENAME TO tb_locacao_backup

CREATE TABLE tb_locacao (
	idLocacao int PRIMARY KEY,
	idCliente int,
	idCarro int,
	dataLocacao date,
	horaLocacao int,
	qtdDiaria int,
	vlrDiaria int, 
	kmLocacao int,
	dataEntrega date,
	horaEntrega int,
	idVendedor int,
	FOREIGN KEY (idCliente) REFERENCES tb_cliente(idCliente),
	FOREIGN KEY (idCarro) REFERENCES tb_carro(idCarro),
	FOREIGN KEY (idVendedor) REFERENCES tb_vendedor(idVendedor)	
)

INSERT INTO tb_locacao (idLocacao, idCliente, idCarro, dataLocacao, horaLocacao, qtdDiaria, vlrDiaria, kmLocacao, dataEntrega, horaEntrega, idVendedor)
SELECT 
	tlb.idLocacao,
	tc.idCliente,
	tc2.idCarro,
	DATE(substr(tlb.dataLocacao, 1, 4) || '-' || substr(tlb.dataLocacao, 5, 2) || '-' || substr(tlb.dataLocacao, 7, 2)) AS dataLocacao,
	tlb.horaLocacao,
	tlb.qtdDiaria,
	tlb.vlrDiaria,
	tlb.kmCarro,
	DATE(substr(tlb.dataEntrega, 1, 4) || '-' || substr(tlb.dataEntrega, 5, 2) || '-' || substr(tlb.dataEntrega, 7, 2)) AS dataEntrega,
	tlb.horaEntrega,
	tv.idVendedor 
FROM tb_locacao_backup tlb 
LEFT JOIN tb_cliente tc ON tlb.idCliente = tc.idCliente 
LEFT JOIN tb_carro tc2 ON tlb.idCarro = tc2.idCarro
LEFT JOIN tb_vendedor tv ON tlb.idVendedor = tv.idVendedor 
