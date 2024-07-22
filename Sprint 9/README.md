# Perguntas:
    * Qual os 10 series de animação mais bem avaliadas
    * Qual o ano que mais sairam series de animação
    * media de episodios por serie de animação
    * media de episodios por serie de animação com notas altas
    * quantidade de series de animação lançadas por ano 
    * Porcetagem de linguagem das series de animação
    * Nota media por linguagem nas series de animação
    * Quantidade media de temporadas nas series com alta avaliação
    * Quantidade media de temporadas geral
    * Nota media por país de produção
    * Quantidade de series por país
    * pais com mais series no top 10
    * genero dos atores para cada profissão

# Desafio

Para começar o desafio analisei meus dados, que ja haviam sido catalogados na sprint anterior.

![dados](/Sprint%209/Evidencias/Desafio%20-%20Vizualização%20dos%20dados.png)

com isso comecei a definir meu modelo multi dimensional.
(Aqui tive um pouco de dificuldade para entender a diferença do dimensional e do multidimensional)
Fiz toda a modelagem no star UML, uma ferramenta que ja estou acostumado a utilizar.

A modelagem da primeira tabela fato ficou assim:

![FatoSerie](/Sprint%209/Evidencias/Desafio%20-%20Modelagem%20Fato%20Serie.png)

e segui para a segunda tabela fato que seria das series de animação mais bem avaliadas

![FatoSerieTop](/Sprint%209/Evidencias/Desafio%20-%20Modelagem%20Fato%20Serie%20Top%20Rated.png)

e por ultimo, a modelagem da tabela de fato dos atores

![FatoAtor](/Sprint%209/Evidencias/Desafio%20-%20Modelagem%20Fato%20Ator.png)

com isso parti para a criação do script python que seria o responsavel por fazer a criação da mogelagem no banco de dados.

primeiro defini ja no data lake como as tabelas ficariam armazenadas

![estrutura](/Sprint%209/Evidencias/Desafio%20-%20Estrutura%20de%20pastas%20.png)

essa estrutura se repete a todas as tabelas fato, apenas se adaptando a suas dimensões e nome.

