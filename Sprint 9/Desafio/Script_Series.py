from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, year, month, dayofmonth

spark = SparkSession.builder \
    .appName("ExemploPySpark") \
    .getOrCreate()

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

df = spark.read.parquet("../Arquivos/Series_para_modelar/part-00000-a0677587-cf84-4b6c-842e-55ea74bd0efd-c000.snappy.parquet")

df_dim_serie = df.select("name", "overview").distinct()
df_dim_serie = adicionaID(df_dim_serie, "Serie")
df_dim_serie = df_dim_serie.select("SerieID", "name", "overview")

df_dim_lancamento = df.select("first_air_date").distinct()
df_dim_lancamento = transformar_dataframe(df_dim_lancamento, "first_air_date", "Lancamento")
df_dim_lancamento = df_dim_lancamento.select("LancamentoID", "first_air_date", "Ano", "Mes", "Dia")

df_dim_ultimo_ep = df.select("last_air_date").distinct()
df_dim_ultimo_ep = transformar_dataframe(df_dim_ultimo_ep, "last_air_date", "UltimoEp")
df_dim_ultimo_ep = df_dim_ultimo_ep.select("UltimoEpID", "last_air_date", "Ano", "Mes", "Dia")

df_dim_status = df.select("status").distinct()
df_dim_status = adicionaID(df_dim_status, "Status")
df_dim_status = df_dim_status.select("StatusID", "status")

df_dim_pais = df.select("production_country").distinct()
df_dim_pais = adicionaID(df_dim_pais, "Pais")
df_dim_pais = df_dim_pais.select("PaisID", "production_country")

df_dim_linguagem = df.select("original_language").distinct()
df_dim_linguagem = adicionaID(df_dim_linguagem, "Linguagem")
df_dim_linguagem = df_dim_linguagem.select("LinguagemID", "original_language")

df_dim_temporadas = df.select("number_of_seasons").distinct()
df_dim_temporadas = adicionaID(df_dim_temporadas, "Temporadas")
df_dim_temporadas = df_dim_temporadas.select("TemporadasID", "number_of_seasons")

df_dim_poster = df.select("poster_path").distinct()
df_dim_poster = adicionaID(df_dim_poster, "Poster")
df_dim_poster = df_dim_poster.select("PosterID", "poster_path")

# Filtragem para garantir que apenas registros com valores não nulos sejam incluídos
df = df.filter(
    df["name"].isNotNull() &
    df["first_air_date"].isNotNull() &
    df["last_air_date"].isNotNull() &
    df["status"].isNotNull() &
    df["production_country"].isNotNull() &
    df["original_language"].isNotNull() &
    df["number_of_seasons"].isNotNull() &
    df["poster_path"].isNotNull()
)

tabela_fato = (
    df
    .join(df_dim_serie, df["name"] == df_dim_serie["name"], "left")
    .join(df_dim_lancamento, df["first_air_date"] == df_dim_lancamento["first_air_date"], "left")
    .join(df_dim_ultimo_ep, df["last_air_date"] ==  df_dim_ultimo_ep["last_air_date"], "left")
    .join(df_dim_status, df["status"] == df_dim_status["status"], "left")
    .join(df_dim_pais, df["production_country"] == df_dim_pais["production_country"], "left")
    .join(df_dim_linguagem, df["original_language"] == df_dim_linguagem["original_language"], "left")
    .join(df_dim_temporadas, df["number_of_seasons"] == df_dim_temporadas["number_of_seasons"], "left")
    .join(df_dim_poster, df["poster_path"] ==  df_dim_poster["poster_path"], "left")
    .filter(
        df_dim_serie["SerieID"].isNotNull() &
        df_dim_lancamento["LancamentoID"].isNotNull() &
        df_dim_ultimo_ep["UltimoEpID"].isNotNull() &
        df_dim_status["StatusID"].isNotNull() &
        df_dim_pais["PaisID"].isNotNull() &
        df_dim_linguagem["LinguagemID"].isNotNull() &
        df_dim_temporadas["TemporadasID"].isNotNull() &
        df_dim_poster["PosterID"].isNotNull()
    )
    .select(
        df["id"],
        df_dim_serie["SerieID"],
        df_dim_lancamento["LancamentoID"],
        df_dim_ultimo_ep["UltimoEpID"],
        df_dim_status["StatusID"],
        df_dim_pais["PaisID"],
        df_dim_linguagem["LinguagemID"],
        df_dim_temporadas["TemporadasID"],
        df["number_of_episodes"],
        df["vote_average"],
        df["vote_count"],
        df_dim_poster["PosterID"]
    )
)

tabela_fato.show()

spark.stop()
