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

logo em seguida começo a desenvolver o script

```
import sys
from awsglue.transforms import * # type: ignore
from awsglue.utils import getResolvedOptions # type: ignore
from pyspark.context import SparkContext
from awsglue.context import GlueContext # type: ignore
from awsglue.job import Job # type: ignore
from pyspark.sql.functions import monotonically_increasing_id, year, month, dayofmonth

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH_COMP', 'S3_INPUT_PATH_TOP', 'S3_TARGET_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

source_file_comp = args['S3_INPUT_PATH_COMP']
source_file_top= args['S3_INPUT_PATH_TOP']
target_path = args['S3_TARGET_PATH']
```

a primeira etapa é definir as funções que utilizaria assim como os caminhos dos arquivos que irei utilizar

```
def adicionaID(df_dim, name):
    """
    Adiciona uma coluna ID no dataframe

    :param df_dim: Dataframe de entrada
    :param name: Nome da coluna que deve ser adicionada com o sufixo ID na frente
    :return: DataFrame com a coluna de ID
    """
    df_dim = df_dim.withColumn(f"{name}ID", monotonically_increasing_id())
    return df_dim 

def transformar_dataframe(df, col_data, nome_col_id):
    """
    Adiciona uma coluna de ID e separa a coluna de data em ano, mês e dia.

    :param df: DataFrame de entrada.
    :param col_data: Nome da coluna com a data a ser separada.
    :param nome_col_id: Nome da nova coluna ID.
    :return: DataFrame transformado.
    """
    df = adicionaID(df, nome_col_id)
    
    df = df.withColumn("Ano", year(df[col_data].cast("date"))) \
        .withColumn("Mes", month(df[col_data].cast("date"))) \
        .withColumn("Dia", dayofmonth(df[col_data].cast("date")))

    return df
```

em seguida defino duas funções que serão bastante utilizadas, como adicionar id nas tabelas e ja separar a data delas em Ano, Mês e Dia

```
dynamicFrame_comp = glueContext.create_dynamic_frame.from_options(
    connection_type = "s3", 
    connection_options = {"paths": [source_file_comp]}, 
    format = "parquet"
)

df = dynamicFrame_comp.toDF()

df_dim_serie_comp = df.select("name", "overview").distinct()
df_dim_serie_comp = adicionaID(df_dim_serie_comp, "Serie")
df_dim_serie_comp = df_dim_serie_comp.select("SerieID", "name", "overview")

df_dim_lancamento_comp = df.select("first_air_date").distinct()
df_dim_lancamento_comp = transformar_dataframe(df_dim_lancamento_comp, "first_air_date", "Lancamento")
df_dim_lancamento_comp = df_dim_lancamento_comp.select("LancamentoID", "first_air_date", "Ano", "Mes", "Dia")

df_dim_ultimo_ep_comp = df.select("last_air_date").distinct()
df_dim_ultimo_ep_comp = transformar_dataframe(df_dim_ultimo_ep_comp, "last_air_date", "UltimoEp")
df_dim_ultimo_ep_comp = df_dim_ultimo_ep_comp.select("UltimoEpID", "last_air_date", "Ano", "Mes", "Dia")

df_dim_status_comp = df.select("status").distinct()
df_dim_status_comp = adicionaID(df_dim_status_comp, "Status")
df_dim_status_comp = df_dim_status_comp.select("StatusID", "status")

df_dim_pais_comp = df.select("production_country").distinct()
df_dim_pais_comp = adicionaID(df_dim_pais_comp, "Pais")
df_dim_pais_comp = df_dim_pais_comp.select("PaisID", "production_country")

df_dim_linguagem_comp = df.select("original_language").distinct()
df_dim_linguagem_comp = adicionaID(df_dim_linguagem_comp, "Linguagem")
df_dim_linguagem_comp = df_dim_linguagem_comp.select("LinguagemID", "original_language")

df_dim_temporadas_comp = df.select("number_of_seasons").distinct()
df_dim_temporadas_comp = adicionaID(df_dim_temporadas_comp, "Temporadas")
df_dim_temporadas_comp = df_dim_temporadas_comp.select("TemporadasID", "number_of_seasons")

df_dim_poster_comp = df.select("poster_path").distinct()
df_dim_poster_comp = adicionaID(df_dim_poster_comp, "Poster")
df_dim_poster_comp = df_dim_poster_comp.select("PosterID", "poster_path")
```

agora crio a tabela com um dynamic frame e passo para um dataframe e começo a selecionar quais colunas deveriam constar em cada dimensão que foi criada e adicionando o ID com a função que criei anteriormente

```
tabela_fato_comp = (
    df
    .join(df_dim_serie_comp, df["name"] == df_dim_serie_comp["name"], "left")
    .join(df_dim_lancamento_comp, df["first_air_date"] == df_dim_lancamento_comp["first_air_date"], "left")
    .join(df_dim_ultimo_ep_comp, df["last_air_date"] ==  df_dim_ultimo_ep_comp["last_air_date"], "left")
    .join(df_dim_status_comp, df["status"] == df_dim_status_comp["status"], "left")
    .join(df_dim_pais_comp, df["production_country"] == df_dim_pais_comp["production_country"], "left")
    .join(df_dim_linguagem_comp, df["original_language"] == df_dim_linguagem_comp["original_language"], "left")
    .join(df_dim_temporadas_comp, df["number_of_seasons"] == df_dim_temporadas_comp["number_of_seasons"], "left")
    .join(df_dim_poster_comp, df["poster_path"] ==  df_dim_poster_comp["poster_path"], "left")
    .filter(
        df_dim_serie_comp["SerieID"].isNotNull() &
        df_dim_lancamento_comp["LancamentoID"].isNotNull() &
        df_dim_ultimo_ep_comp["UltimoEpID"].isNotNull() &
        df_dim_status_comp["StatusID"].isNotNull() &
        df_dim_pais_comp["PaisID"].isNotNull() &
        df_dim_linguagem_comp["LinguagemID"].isNotNull() &
        df_dim_temporadas_comp["TemporadasID"].isNotNull() &
        df_dim_poster_comp["PosterID"].isNotNull()
    )
    .select(
        df["id"],
        df_dim_serie_comp["SerieID"],
        df_dim_lancamento_comp["LancamentoID"],
        df_dim_ultimo_ep_comp["UltimoEpID"],
        df_dim_status_comp["StatusID"],
        df_dim_pais_comp["PaisID"],
        df_dim_linguagem_comp["LinguagemID"],
        df_dim_temporadas_comp["TemporadasID"],
        df["number_of_episodes"],
        df["vote_average"],
        df["vote_count"],
        df_dim_poster_comp["PosterID"]
    )
)
```

e repito esse mesmo processo para as duas outras tabelas fato, o codigo completo esta nesse script: [Script_Series.py](/Sprint%209/Desafio/Script_Series.py)

e em seguida salvo os arquivos nos seus determinados diretorios

```
tabela_fato_comp.repartition(1).write.mode("overwrite").parquet(target_path + "Series/FatoSerie/")
df_dim_serie_comp.repartition(1).write.mode("overwrite").parquet(target_path + "Series/DimSerie/")
df_dim_lancamento_comp.repartition(1).write.mode("overwrite").parquet(target_path + "Series/DimLancamento/")
df_dim_ultimo_ep_comp.repartition(1).write.mode("overwrite").parquet(target_path + "Series/DimUltimoEp/")
df_dim_status_comp.repartition(1).write.mode("overwrite").parquet(target_path + "Series/DimStatus/")
df_dim_pais_comp.repartition(1).write.mode("overwrite").parquet(target_path + "Series/DimPais/")
df_dim_linguagem_comp.repartition(1).write.mode("overwrite").parquet(target_path + "Series/DimLinguagem/")
df_dim_temporadas_comp.repartition(1).write.mode("overwrite").parquet(target_path + "Series/DimTemporadas/")
df_dim_poster_comp.repartition(1).write.mode("overwrite").parquet(target_path + "Series/DimPoster/")

tabela_fato_top.repartition(1).write.mode("overwrite").parquet(target_path + "serie_top_rated/Fato_Top_Rated/")
df_dim_serie_top.repartition(1).write.mode("overwrite").parquet(target_path + "serie_top_rated/Dim_Serie_Top/")
df_dim_lancamento_top.repartition(1).write.mode("overwrite").parquet(target_path + "serie_top_rated/Dim_Lancamento_Top/")
df_dim_pais_top.repartition(1).write.mode("overwrite").parquet(target_path + "serie_top_rated/Dim_Pais_Top/")
df_dim_poster_top.repartition(1).write.mode("overwrite").parquet(target_path + "serie_top_rated/Dim_Poster_Top/")

tabela_fato_ator_profissao.repartition(1).write.mode("overwrite").parquet(target_path + "Atores/Fator_Ator_Profissão/")
df_dim_profissao.repartition(1).write.mode("overwrite").parquet(target_path + "Atores/Dim_Profissão/")
df_dim_ator.repartition(1).write.mode("overwrite").parquet(target_path + "Atores/Dim_Ator/")
```