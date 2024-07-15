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

Utilizo essa conta matematica para gerar as datas aleatoriamente, que funciona da seguinte maneira, o rand() gera um numero aleatorio entre 0 e 1 e multiplica pela quantidade de anos possiveis entre a data minima e maxima. após isso somo 1945 no resultado e arredondo para assim obter um ano aleatorio.

![resultado](/Sprint%208/Evidencias/Execução%20com%20coluna%20de%20ano%20-%20exercicio%20spark.png)

### Etapa 6

![alt text](/Sprint%208/Evidencias/image-5.png)

Utilizo um filter e logo após o select para filtre as pessoas que nasceram neste século (nas primeiras tentativas tives problemas de utilizar as funções pois o select deve ser utilizado após o filter)

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

Na etapa 10, deveria utilizar o sparkSQL para obter a quantidade que cada pais tem de cada uma das gerações, para isso fiz uma consulta SQL que ja agrupava, e em seguida armazenei para um novo dataset e utilizei a função orderBy para agrupar conforme foi pedido no exercicio, e logo após utilizei o show para obter o resultado a seguir:

![resultado](/Sprint%208/Evidencias/Resultado%20ultima%20query%20-%20exercicio%20Spark.png)

e assim finalizei todas as etapas deste exercicio

## TMDB

Seguindo a instrução do monitor, por ja ter utilizado a API do TMDB na sprint anterior, não precisamos fazer este exercicio

# Desafio

Nessa estapa do desafio final deveria criar um Job no AWS Glue para promover os dados do CSV e dos arquivos que vieram da API do TMDB para a camada trusted. Para isso deveriamos criar 2 jobs, um para o csv e outro para os json. Comecei pela criação do Job do CSV.

## Job_Csv

O script completo pode ser encontrado clicando [aqui](/Sprint%208/Desafio/Job_Csv.py)

A primeira etapa a ser feita foi criar um Script Spark no AWS Glue, após isso configurei o Job como foi pedido nas instruções do desafio

![configurações](/Sprint%208/Evidencias/Desafio%20-%20Configurações%20Job%20CSV.png)

após isso comecei a desenvolver meu codigo e a primeira etapa foi definir os caminhos para acessar os arquivos e salvar os arquivos, os defini utilizando os job parameters disponiveis no AWS Glue

![job_parameter](/Sprint%208/Evidencias/Desafio%20-%20Job%20parameter%20CSV.png)

e defini esses jobs no proprio script:

```
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_TARGET_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

source_file = args['S3_INPUT_PATH']
target_path = args['S3_TARGET_PATH']
```

em seguida leio o arquivo em dynamic frame e o transformo em um dataframe para poder utilizar o SparkSQL com mais facilidade

```
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    "s3",
    {
        "paths": [
            source_file
        ]
    },
    "csv",
    {"withHeader": True, "separator":"|"},
    )

df = dynamic_frame.toDF()
```

em seguida eu aplico um drop em todas as linhas em que as colunas que eu utilizaria estariam vazias, e após isso aplico um filtro para que todos os resultados remanescentes no arquivo sejam do genero de animação.

```
df = df.na.drop(subset=["nomeArtista", "generoArtista", "anoNascimento", "anoFalecimento"])

genero = 'Animation'
df = df.filter(df['genero'] == genero)
```

Após isso faço 2 tabelas, para que eu consigo ter todas as profissões de um determinado artistia sem que passe uma lista para a camada trusted (Passar a lista em si não estaria errado, porém ao consultar o monitor da sprint, ele me recomendou ja passar os dados separados para facilitar nas proximas etapas.) A maneira como essas tabelas funcionas é da seguinte maneira: tenho um tabela com as profissões e seus codigos, e uma tabela relacionando o nome do artista com o codigo das profissões dele.

```
profissoes_df = df.select(explode(split(df['profissao'], ',')).alias('nome_profissao')).distinct()

profissoes_df = profissoes_df.filter(col("nome_profissao") != "")

windowSpec = Window.orderBy("nome_profissao") 

profissoes_df = profissoes_df.withColumn("profissao_id", row_number().over(windowSpec))

artista_profissao_df = (
    df.select(
        col("nomeArtista").alias("nome"),
        explode(split(col("profissao"), ",")).alias("nome_profissao"),
    )
    .join(profissoes_df, on="nome_profissao", how="inner")
    .select("nome", "profissao_id")
)
```

Após isso defini a tabela onde conteria os dados dos artistas, que conteria o nome do artista, o genero, ano de nascimento e falecimento (Tive que aplicar um filtro nas colunas Ano Nascimento e Ano falecimento pois alguns valores estavam como \N), após isso apliquei um filtro para que o genero ficasse no formato: M e F, ja que no csv original os dados estão como Actor e Actress

```
df_final = df.select(df['nomeArtista'], df['generoArtista'], df['anoNascimento'], df['anoFalecimento'])

df_final = df_final.filter(col("anoNascimento") != r"\N")
df_final = df_final.filter(col("anoFalecimento") != r"\N")

df_final = df_final.withColumn(
            "generoArtista",
            when((df_final["generoArtista"] == "actor"), "M")
            .when((df_final["generoArtista"] == "actress"), "F")
            )
```

Após isso salvei os arquivos como Parquet na camada trusted do bucket separados por uma pasta para cada uma das tabelas parquet, para evitar que os dados sejam sobreescritos por outros, além de ter melhor organização, após isso utilizo um commit para finalizar o Script

```
profissoes_df.write.parquet(target_path + "/profissoes_df")
artista_profissao_df.write.parquet(target_path + "/artista_profissao_df")
df_final.write.parquet(target_path + "/df_final")

job.commit()
```

![arquivos salvos](/Sprint%208/Evidencias/Desafio%20-%20Arquivos%20salvos%20em%20suas%20determinadas%20pastas.png)

![exemplo](/Sprint%208/Evidencias/Desafio%20-%20Exemplo%20de%20como%20os%20arquivos%20estão%20salvos.png)

## Job_Json

Nessa etapa deveria fazer algo parecido com o Job do Csv mas adaptado aos Json

### Observações:

* Não utilizei a pasta de popular, pois até a data que foi feita a promoção dos dados, nenhuma serie de animação tinha ficado entre as series populares, portanto não havia nenhum dado nos Jsons vindos da API

### Etapa 1

A primeira coisa a se fazer foi importar as bibliotecas e definir as fontes de dados (neste caso eu utilizo uma maneira diferente de ler os dados, pois estava tendo problemas para ler todos os dados das pastas de uma vez com o método que utilizei no passado)

```
import sys
from awsglue.transforms import * # type: ignore
from awsglue.utils import getResolvedOptions # type: ignore
from pyspark.context import SparkContext
from awsglue.context import GlueContext # type: ignore
from awsglue.job import Job # type: ignore
from pyspark.sql.functions import size, when, element_at, explode, avg, col
from pyspark.sql.types import IntegerType, StringType, DateType, FloatType, StructType, StructField


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH_COMP', 'S3_TARGET_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")
spark.conf.set("spark.sql.files.recursiveFileLookup", "true")

df_spark = spark.read.json("s3://datalakecaio/Raw/TMDB/JSON/Complementares/2024/7/10/serie_*")

df_spark_top = spark.read.json("s3://datalakecaio/Raw/TMDB/JSON/Top_Rated/2024/7/12/top_rated_2024-7-12.json")
```

### Etapa 2

Agora comecei a filtrar os dados que ia utlizar para garantir que os dados não estavam nulos/faltantes

```
df_spark = df_spark.na.drop(subset=['first_air_date']).filter((df_spark['first_air_date']) != '')

df_spark = df_spark.na.drop(subset=['name']).filter((df_spark['name']) != '')

df_spark = df_spark.filter(size(df_spark["languages"]) > 0)

df_spark = df_spark.withColumn("Production_country",
    when(df_spark["production_countries"].isNull() | (size(df_spark["production_countries"]) == 0), None)
    .otherwise(element_at(df_spark["production_countries"], 1).getField("iso_3166_1"))
)
```

### Etapa 3

Após isso calculei a nota media das temporadas e as uni ao dataframe

```
df_exploded = df_spark.withColumn("season", explode("seasons"))
df_with_avg = df_exploded.groupBy(df_spark.columns).agg(avg(col("season.vote_average")).alias("nota_media_temporadas"))

df_spark = df_with_avg.select(df_spark.columns + [col("nota_media_temporadas")])
```

### Etapa 4

Em seguida o que fiz foi definir um novo dataframe com apenas as colunas que utilizaria(Verifiquei todas as colunas utilizando a ferramenta de ler um parquet com todos os dados no pandas para garantir que não haveria importantes faltantes)

```
df_spark_comp = df_spark

df_spark_comp = df_spark_comp.select(
    'id',
    'name',
    'overview',
    'status',
    'first_air_date',
    'last_air_date',
    'Production_country',
    'original_language',
    'number_of_seasons',
    'nota_media_temporadas',
    'number_of_episodes',
    'vote_average',
    'vote_count',
    'poster_path'
)
```
### Etapa 5

e logo em seguida fiz o mesmo para os dados do top rated

```
df_spark_top = df_spark_top.withColumn("origin_country",
    element_at(df_spark_top["origin_country"], 1)
)

df_spark_top = df_spark_top.select(
    "id", 
    "name",
    "overview",
    "origin_country",
    "first_air_date", 
    "original_name", 
    "popularity", 
    "vote_average",
    "vote_count", 
    "poster_path"
    )
```


### Etapa 6

E para finalizar salvei os dados me assegurando que seriam salvos em uma estrutura que o crawler poderia catalogar todos no database

```
df_spark_comp.repartition(1).write.mode("overwrite").parquet("s3://datalakecaio/Trusted/Complementares/dt=2024-07-15/complementar_1/")

df_spark_top.repartition(1).write.mode("overwrite").parquet("s3://datalakecaio/Trusted/top_rated/dt=2024-07-15/top_rated_1/")

job.commit()
```

## Data base e crawlers

Após conversar com colegas de squad e recomendação ao monitor criei um crawler que ja salva-va os dados em um banco de dados Athena

![crawler](/Sprint%208/Evidencias/Desafio%20-%20criação%20e%20execução%20do%20crawler.png)

![athena](/Sprint%208/Evidencias/Desafio-%20tabelas%20no%20athena.png)