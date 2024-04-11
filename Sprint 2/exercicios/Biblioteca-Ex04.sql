select
	autor.nome as nome,
	autor.codautor,
	autor.nascimento,
	COALESCE(count(livro.cod), 0) as quantidade
from autor
left join livro
	on livro.autor = autor.codautor 
group by nome
order by nome
