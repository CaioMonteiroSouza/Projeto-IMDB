# Perguntas:
    * Qual os 10 filmes/series mais bem avaliados
    * Qual o ano que mais sairam filmes/series
    * Nota media dos filmes/series
    * Atores que mais fizeram filmes/series
    * Atores vivos com as melhores notas medias
    * filmes/series mais votados
    * primeiro filme/serie de cada um dos generos
    * media de duração de filmes do genero
    * participação de genero nos filmes
    * diferença de nota media por genero
    * quantidade de filmes por ano
    * nota media por ano

# Exercicios:

### S3:
![criação_bucket](/Sprint%206/Evidencias/Criação%20do%20Bucket.png)

Com o bucket criado segui as instruções na udemy e habilitei o site estatico, subi os arquivos e liberei o acesso tendo assim o site hospedado, como na imagem seguinte:

![site_hospedado](/Sprint%206/Evidencias/site%20hospedado.png)

### Athena:
Seguindo as instruções e as etapas anteriores acesso o console do athena e crio o banco de dados, em seguida executo essa querry para criar a tabela:
![query_tabela](/Sprint%206/Evidencias/criação%20tabela%20athenas.png)

Em seguido faço as consultas pedidas:
![consulta1](/Sprint%206/Evidencias/consulta%20athena.png)

![consulta2](/Sprint%206/Evidencias/resultado%20query.png)
a query completa da segunda consulta pode ser encontrada [clicando aqui](/Sprint%206/Exercicios/Athena/query.sql)

### Lambda:

Seguindo o passo a passo crio a função lambda, quando a executo vejo que ela tem um erro, como explicado no passo a passo. Por isso uma camada deveria ser criada, porem a imagem que estava no passo a passo esta desatualizada e por isso tive alguns erros, mas os consegui resolver no final. Apenas apaguei a ultima linha que fazia com que um erro ocorresse e troquei a imagem para uma mais atualizada, assim como alterei as versão do python para o 3.9 em todos os lugares necessarios.
Assim ficou a imagem:
![imagem](/Sprint%206/Evidencias/imagem%20do%20docker.png)

Com isso seguindo o passo a passo, sem muitos problemas ja tinha minha camada pronta, ([clique aqui](/Sprint%206/Exercicios/Lambda/minha-camada-pandas.zip) para acessar a camada) com isso a vinculei a lambda como mostra o passo a passo e não tive muitos problemas na execução.
![execução](/Sprint%206/Evidencias/lambda%20executada%20com%20sucesso.png)

# Desafio:

a documentação completa do desafio esta nesse [README](/Sprint%206/Desafio/README.md).

# Certificados:

[certificado1](/Sprint%206/Certificados/13655_3_5302791_1717700036_AWS%20Course%20Completion%20Certificate.pdf)
[certificado2](/Sprint%206/Certificados/14908_3_5302791_1717779835_AWS%20Course%20Completion%20Certificate.pdf)
[certificado3](/Sprint%206/Certificados/19345_5_5302791_1717527865_AWS%20Skill%20Builder%20Course%20Completion%20Certificate.pdf)
[certificado4](/Sprint%206/Certificados/19359_5_5302791_1717611381_AWS%20Skill%20Builder%20Course%20Completion%20Certificate.pdf)
[certificado5](/Sprint%206/Certificados/5838_3_5302791_1717616355_AWS%20Course%20Completion%20Certificate.pdf)
[certificado6](/Sprint%206/Certificados/6256_3_5302791_1717531830_AWS%20Course%20Completion%20Certificate.pdf)
[certificado7](/Sprint%206/Certificados/6339_3_5302791_1717701035_AWS%20Course%20Completion%20Certificate.pdf)
[certificado8](/Sprint%206/Certificados/8171_3_5302791_1717694320_AWS%20Course%20Completion%20Certificate.pdf)
[certificado9](/Sprint%206/Certificados/8827_5_5302791_1717697203_AWS%20Skill%20Builder%20Course%20Completion%20Certificate.pdf)