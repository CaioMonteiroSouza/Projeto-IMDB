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

dynamicFrame_top = glueContext.create_dynamic_frame.from_options(
    connection_type = "s3", 
    connection_options = {"paths": [source_file_top]}, 
    format = "parquet"
)

df_top = dynamicFrame_top.toDF()

df_dim_serie_top = df_top.select("name", "overview").distinct()
df_dim_serie_top = adicionaID(df_dim_serie_top, "SerieTop")
df_dim_serie_top = df_dim_serie_top.select("SerieTopID", "name", "overview")

df_dim_lancamento_top = df_top.select("first_air_date").distinct()
df_dim_lancamento_top = transformar_dataframe(df_dim_lancamento_top, "first_air_date", "LancamentoTop")
df_dim_lancamento_top = df_dim_lancamento_top.select("LancamentoTopID", "First_air_date", "Ano", "Mes", "Dia")

df_dim_pais_top = df_top.select("origin_country").distinct()
df_dim_pais_top = adicionaID(df_dim_pais_top, "PaisTop")
df_dim_pais_top = df_dim_pais_top.select("PaisTopID", "origin_country")

df_dim_poster_top = df_top.select("poster_path").distinct()
df_dim_poster_top = adicionaID(df_dim_poster_top, "PosterTop")
df_dim_poster_top = df_dim_poster_top.select("PosterTopID", "poster_path")

tabela_fato_top = (
    df_top
    .join(df_dim_serie_top, df_top["name"] == df_dim_serie_top["name"], "left")
    .join(df_dim_lancamento_top, df_top["first_air_date"] == df_dim_lancamento_top["first_air_date"], "left")
    .join(df_dim_pais_top, df_top["origin_country"] == df_dim_pais_top["origin_country"], "left")
    .join(df_dim_poster_top, df_top["poster_path"] ==  df_dim_poster_top["poster_path"], "left")
    .filter(
        df_dim_serie_top["SerieTopID"].isNotNull() &
        df_dim_lancamento_top["LancamentoTopID"].isNotNull() &
        df_dim_pais_top["PaisTopID"].isNotNull() &
        df_dim_poster_top["PosterTopID"].isNotNull()
    )
    .select(
        df_top["id"],
        df_dim_serie_top["SerieTopID"],
        df_dim_lancamento_top["LancamentoTopID"],
        df_dim_pais_top["PaisTopID"],
        df_top["popularity"],
        df_top["vote_average"],
        df_top["vote_count"],
        df_dim_poster_top["PosterTopID"]
    )
)


df_final = spark.read.parquet("s3://datalakecaio/Trusted/CSV/df_final/")

df_profissoes = spark.read.parquet("s3://datalakecaio/Trusted/CSV/profissoes_df/")

df_artista_profissao = spark.read.parquet("s3://datalakecaio/Trusted/CSV/artista_profissao_df/")

df_artista_profissao = adicionaID(df_artista_profissao, "ArtistaProfissao")

df_dim_profissao = df_profissoes

df_dim_ator = df_final
df_dim_ator = adicionaID(df_dim_ator, "Ator")
df_dim_ator = df_dim_ator.select("AtorID", "nomeartista", "generoartista", "anonascimento", "anofalecimento")

tabela_fato_ator_profissao = (
    df_artista_profissao
    .join(df_dim_ator, df_artista_profissao["nome"] == df_dim_ator["nomeartista"], "left")
    .join(df_dim_profissao, df_artista_profissao["profissao_id"] == df_dim_profissao["profissao_id"], "left")
    .select(
        df_artista_profissao["ArtistaProfissaoID"],
        df_dim_ator["AtorID"],
        df_dim_profissao["profissao_id"]
    )
)

tabela_fato_ator_profissao_filtered = tabela_fato_ator_profissao.filter(tabela_fato_ator_profissao["AtorID"].isNotNull())

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


spark.stop()


