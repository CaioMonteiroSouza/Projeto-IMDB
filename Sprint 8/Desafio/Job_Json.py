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

df_spark = df_spark.na.drop(subset=['first_air_date']).filter((df_spark['first_air_date']) != '')

df_spark = df_spark.na.drop(subset=['name']).filter((df_spark['name']) != '')

df_spark = df_spark.filter(size(df_spark["languages"]) > 0)

df_spark = df_spark.withColumn("Production_country",
    when(df_spark["production_countries"].isNull() | (size(df_spark["production_countries"]) == 0), None)
    .otherwise(element_at(df_spark["production_countries"], 1).getField("iso_3166_1"))
)

def count_elements(array_col):
    return size(array_col)

df_exploded = df_spark.withColumn("season", explode("seasons"))
df_with_avg = df_exploded.groupBy(df_spark.columns).agg(avg(col("season.vote_average")).alias("nota_media_temporadas"))

df_spark = df_with_avg.select(df_spark.columns + [col("nota_media_temporadas")])

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

df_spark_comp.repartition(1).write.mode("overwrite").parquet("s3://datalakecaio/Trusted/Complementares/dt=2024-07-15/complementar_1/")

df_spark_top.repartition(1).write.mode("overwrite").parquet("s3://datalakecaio/Trusted/top_rated/dt=2024-07-15/top_rated_1/")

job.commit()