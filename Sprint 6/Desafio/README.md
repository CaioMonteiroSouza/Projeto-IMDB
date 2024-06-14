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

# Preparação

Primeiro analisei os arquivos [movies.csv](/Sprint%206/Desafio/data/movies.csv) e [series.csv](/Sprint%206/Desafio/data/series.csv) que estavam disponiveis para download para uma maior compreesensão de como seria o desafio. 

# Processo

No desafio deverias criar uma imagem que enviasse a um datalake criado na AWS utilizando o S3 os 2 arquivos [movies.csv](/Sprint%206/Desafio/data/movies.csv) e [series.csv](/Sprint%206/Desafio/data/series.csv), para isso primeiro criei o bucket que ficaria o datalake:
![bucket](/Sprint%206/Evidencias/criação%20datalake.png)

com isso criei a estrutura de pastas pelo console e partir para o codigo e imagem.
Comecei criando a imagem que seria utilizada ([clique aqui](/Sprint%206/Desafio/Dockerfile) para acessar a imagem)
![imagem](/Sprint%206/Evidencias/imagem%20desafio.png)
note que usei o copy no arquivo [requirements.txt](/Sprint%206/Desafio/requirements.txt), nesse arquivo deixo o nome de todas as bibliotecas necessarias e o dockerfile as executa na linha seguinte de forma recursiva, assim para que não precise adicionar uma linha a cada biblioteca usada apenas uso o arquivo.
Com isso criado o [docker-compose.yml](/Sprint%206/Desafio/docker-compose.yml) onde defino o volume.

com isso pronto desenvolo o codigo [.py](/Sprint%206/Desafio/envia_arquivo.py)

logo após buildo a imagem
![build da imagem](/Sprint%206/Evidencias/Buildando%20Imagem%20desafio.png)
![imagem salva](/Sprint%206/Evidencias/imagens%20salvas.png)

e então executo o container
![execução container](/Sprint%206/Evidencias/execução%20do%20container.png)

e como pode ver as imagens são enviadas para o bucket:
![movies enviado](/Sprint%206/Evidencias/movies%20enviado.png)

![series enviado](/Sprint%206/Evidencias/series%20enviado.png)

vale ressaltar que na hora da autenticação das credenciais foi utilizado um arquivo .env que não foi enviado ao github por segurança, por isso o codigo utiliza a biblioteca dotenv do python, para utilizar o codigo um arquivo .env com as credenciais deve ser criado.