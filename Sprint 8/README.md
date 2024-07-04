# Perguntas:
    * Qual os 10 series de animação mais bem avaliados
    * Qual o ano que mais sairam series de animação
    * Nota media de series de animação
    * primeira series de animação
    * media de episodios por serie de animação
    * quantidade de series de animação lançadas por ano 
    * Porcetagem de linguagem das series de animação
    * Nota media por linguagem nas series de animação
    * Quantidade media de temporadas nas series com alta avaliação
    * Quantidade media de temporadas geral

# Exercicios

## geração de dados

nesse exercicios deveriamos criar um script para gerar um arquivo de nome aleatorios. para isso usei o script que desenvolvi seguindo as instruções na Udemy, ([clique aqui para acessar o script](/Sprint%208/Exercicios/Gerador%20de%20dados/Gerador_de_dados.py)), e que tive de resultado o seguindo arquivo: [nomes_aleatorios.txt](/Sprint%208/Exercicios/Gerador%20de%20dados/nomes_aleatorios.txt)

para isso eu deveria instalar a biblioteca names:
![names](/Sprint%208/Evidencias/Instalando%20Biblioteca%20Name%20-%20Exercicio%20Name.png)

e seguindo as instruções tive esse resultado (este é apenas o começo do arquivo, há bem mais registros.):

![nomes_aleatorios](/Sprint%208/Evidencias/Resultado%20arquivo%20-%20Exercicio%20Name.png)

## Exercicio Spark

nesse exercicio deveriamos utilizar o Spark no arquivo que geramos na etapa anterior.

para isso deveriamos fazer o que fosse pedido em cada etapa nos Slides da Udemy.

### Etapa 1

![alt text](/Sprint%208/Evidencias/image.png)

fiz o codigo conforme a etapa 1 pedia e segui para a etapa 2

### Etapa 2

![alt text](/Sprint%208/Evidencias/image-1.png)

Ja fiz com que o schema tivesse o nome da coluna correto no começo definindo o schema, e assim tive esse resultado

![resultado-etapa-2](/Sprint%208/Evidencias/Execução%20com%20Schema%20alterado%20-%20exercicio%20Spark.png)

### Etapa 3

![alt text](/Sprint%208/Evidencias/image-2.png)

Utilizei a função lit para que a escolaridade fosse aleatoria e junto com o WithColumn() tenho uma nova coluna com os dados aleatorias como se pode ver nessa print:

![resultado](/Sprint%208/Evidencias/Execução%20com%20coluna%20de%20escolaridade.png)

### Etapa 4

![alt text](/Sprint%208/Evidencias/image-3.png)

Utilizo algo parecido com a etapa anterior apenas alterando os valores que devem ser aleatorios

![resultado](/Sprint%208/Evidencias/Execução%20com%20coluna%20de%20pais%20-%20exercicio%20Spark.png)

### Etapa 5

![alt text](/Sprint%208/Evidencias/image-4.png)

Utilizo essa conta matematica para gerar as datas aleatoriamente

![resultado](/Sprint%208/Evidencias/Execução%20com%20coluna%20de%20ano%20-%20exercicio%20spark.png)

### Etapa 6

![alt text](/Sprint%208/Evidencias/image-5.png)

Utilizo um filter e logo após o select para filtre as pessoas que nasceram neste século

![resultado](/Sprint%208/Evidencias/pessoas%20que%20nasceram%20neste%20seculo%20-%20exercicio%20spark.png)

### Etapa 7

![alt text](/Sprint%208/Evidencias/image-6.png)

Na etapa 7 deveria utilizar o Spark Sql para fazer o mesmo que na Etapa 6

![resultado](/Sprint%208/Evidencias/Spark%20SQL%20para%20quem%20nasceu%20neste%20seculo%20-%20exercicio%20spark.png)

### Etapa 8

![alt text](/Sprint%208/Evidencias/image-7.png)

Na etapa 8 deveria contas quantos millenials existiam no dataset, utilizei um .count em uma variavel que criei contando os millenials

![resultado](/Sprint%208/Evidencias/Contagem%20de%20millenials.png)

### Etapa 9

![alt text](/Sprint%208/Evidencias/image-8.png)

Na etapa 9 deveria fazer o mesmo que na anterior, porem utilizando o Spark SQL

![resultado](/Sprint%208/Evidencias/resultado%20contagem%20de%20millenial%20com%20spark%20SQL%20-%20exercicio%20SPARK.png)

### Etapa 10

![alt text](/Sprint%208/Evidencias/image-9.png)

Na etapa 10, deveria utilizar o sparkSQL para obter a quantidade que cada pais tem de cada uma das gerações, para isso fiz uma consulta SQL que ja ordenava e agrupava, e em seguida armazenei para um novo dataset e utilizei o metodo show para mostrar o resultado

![resultado](/Sprint%208/Evidencias/Resultado%20ultima%20query%20-%20exercicio%20Spark.png)

e assim finalizei todas as etapas deste exercicio