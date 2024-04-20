select
	autor.nome
from autor 
left join livro 
	on codautor = livro.autor 
where livro.autor is NULL 
order by autor.nome 