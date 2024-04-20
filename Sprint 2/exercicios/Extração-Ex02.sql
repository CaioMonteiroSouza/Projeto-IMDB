select 
	e.codeditora,
	e.nome as nomeEditora,
	count(*) as quantidadeLivros
from livro l
left join editora e 
	on l.editora = e.codeditora 
group by e.codeditora 
order by quantidadeLivros desc
limit 5 