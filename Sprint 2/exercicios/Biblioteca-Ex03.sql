with editora_endereco as (
SELECT 
	edi.nome,
	ende.estado,
	ende.cidade,
	edi.codEditora
from editora as edi
left join endereco as ende
	on edi.endereco = ende.codendereco 
)
SELECT 
	count(*) as quantidade,
	edi2.nome,
	edi2.estado,
	edi2.cidade
from livro
left join editora_endereco as edi2
	on edi2.codEditora = livro.editora
group by edi2.nome
