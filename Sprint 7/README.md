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

## Spark 

Primeiro desafio consistia em criar um job Spark para contar as palavras do README.md principal do repositorio.

para isso começo a criar a imagem
![imagem](/Sprint%207/Evidencias/Criação%20da%20imagem.png)

logo em seguido executo o container e utilizo o comando docker exec para acessar o terminal
![imagem-exec](/Sprint%207/Evidencias/Docker%20exec.png)

e sigo a sequencia de comandos encontrada no arquivo [comandos para execução](/Sprint%207/Evidencias/Comandos%20para%20execução.txt)

![comando0](/Sprint%207/Evidencias/comandos%20para%20execução%20-%200.png)
![comando1](/Sprint%207/Evidencias/comandos%20para%20execução%20-%201.png)
![comando2](/Sprint%207/Evidencias/comandos%20para%20execução%20-%202.png)
![pastaComResultados](/Sprint%207/Evidencias/Pasta%20com%20resultados.png)

e assim conclui esse exercicios

## AWS Glue

esse exercicio tinha o objetivo de nos ensinar sobre o AWS Glue

apos configurar as permissões pro glue tive esse resultado:
![permissões](/Sprint%207/Evidencias/permissões%20AWS%20Glue.png)

após isso necessito criar um Job para execuçao do codigo spark, e esse foi o script que usei: [scripty.py](/Sprint%207/Exercicios/AWS%20Glue/script.py)

e aqui estão os resultados que obtive:

Schema
![resultado1](/Sprint%207/Evidencias/schema.png)

Linhas
![linhas](/Sprint%207/Evidencias/quantidade%20linhas.png)

contagem nomes
![contagem](/Sprint%207/Evidencias/Contagem%20nomes.png)

Nomes
![nomes](/Sprint%207/Evidencias/Nomes.png)

Arquivos salvos
![1](/Sprint%207/Evidencias/partificionamento%20lab%20Glue.png)
![2](/Sprint%207/Evidencias/partificionamento%20lab%20Glue2.png)

Logo após isso deveriamos criar o crawler

![crawler1](/Sprint%207/Evidencias/Crawler%20Criado.png)

Execução do crawler

![crawler2](/Sprint%207/Evidencias/execução%20bem%20sucedida%20-%20Crawler.png)

Tabela Criada Crawler

![crawler3](/Sprint%207/Evidencias/tabela%20formada-%20crawler.png)

Visualização da tabela

![crawler4](/Sprint%207/Evidencias/Visualição%20no%20athena%20após%20o%20crawler.png)

# Exercicio

No exercicio deveriamos consumir os dados da API para o nosso data lake

com isso desenvolvi os 2 scripts da pasta [Desafio](/Sprint%207/Desafio/)

logo após isso desenvolvi a camada que esta na mesma pasta, subi os arquivos para o S3 e executei a lambda (vale lembrar que uma lambda tem uso continuo)

Cron da lambda de uso continuo
![cron](/Sprint%207/Evidencias/cron%20expression.png)

![resultado lambda](/Sprint%207/Evidencias/resultados%20lambda.PNG)
![resultado lambda](/Sprint%207/Evidencias/resultados%20lambda-2.png)