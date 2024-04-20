select 
	l.cod as CodLivro,
	l.titulo,
	l.autor as CodAutor,
	a.nome as NomeAutor,
	l.valor,
	l.editora as CodEditora,
	e.nome as NomeEditora
from livro l
left join autor a
	on l.autor = a.codautor 
left join editora e 
	on l.editora = e.codeditora 
order by valor desc
limit 10
