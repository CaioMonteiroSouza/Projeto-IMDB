SELECT DISTINCT 
	autor.nome
from autor
left join livro
	on autor.codautor = livro.autor 
left join editora 
	on livro.editora = editora.codeditora 
left join endereco 
	on endereco.codendereco = editora.endereco 
where endereco.estado not in ('PARANÁ', 'RIO GRANDE DO SUL', 'SANTA CATARINA')
group by autor.nome 