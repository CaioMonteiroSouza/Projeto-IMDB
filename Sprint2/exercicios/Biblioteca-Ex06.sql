with public_max  as (
	select
		count(*) as pub_qnt,
		autor.nome as nome
	from autor 
	left join livro 
		on autor.codautor = livro.autor 
	GROUP by autor.nome 
)



select
	autor.codautor,
	autor.nome,
	max(public_max.pub_qnt) as quantidade_publicacoes
from public_max
left join autor
	on autor.nome = public_max.nome