import sys
from awsglue.transforms import * # type: ignore
from awsglue.utils import getResolvedOptions # type: ignore
from pyspark.context import SparkContext
from awsglue.context import GlueContext # type: ignore
from awsglue.job import Job # type: ignore
from pyspark.sql.functions import col, explode, split, row_number, when, trim, regexp_replace
from pyspark.sql.window import Window

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_TARGET_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

source_file = args['S3_INPUT_PATH']
target_path = args['S3_TARGET_PATH']

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

df = df.na.drop(subset=["nomeArtista", "generoArtista", "anoNascimento", "anoFalecimento"])

genero = 'Animation'
df = df.filter(df['genero'] == genero)

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


df_final = df.select(df['nomeArtista'], df['generoArtista'], df['anoNascimento'], df['anoFalecimento'])

df_final = df_final.filter(col("anoNascimento") != r"\N")
df_final = df_final.filter(col("anoFalecimento") != r"\N")

df_final = df_final.withColumn(
            "generoArtista",
            when((df_final["generoArtista"] == "actor"), "M")
            .when((df_final["generoArtista"] == "actress"), "F")
            )

profissoes_df.write.parquet(target_path + "/profissoes_df")
artista_profissao_df.write.parquet(target_path + "/artista_profissao_df")
df_final.write.parquet(target_path + "/df_final")

job.commit()