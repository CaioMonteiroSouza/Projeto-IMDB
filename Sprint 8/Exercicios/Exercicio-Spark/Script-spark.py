from pyspark.sql import SparkSession
from pyspark import SparkContext, SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import rand, lit, floor

spark = SparkSession \
                .builder \
                .master("local[*]") \
                .appName("Exercicio Spark") \
                .getOrCreate()


## ETAPA 2


schema = StructType([StructField("Nomes", StringType(), True)])

df_nomes = spark.read.csv('./Sprint 8/Exercicios/Gerador de dados/nomes_aleatorios.txt', schema=schema)

df_nomes.printSchema()

df_nomes.show(10)


## ETAPA 3


escolaridade = ["Fundamental", "Medio", "Superior"]

escolaridade_lit = lit(escolaridade)

df_nomes = df_nomes.withColumn("Escolaridade",
                              escolaridade_lit.getItem(floor(rand() * len(escolaridade))))


## ETAPA 4


paises = ["Argentina", "Bolívia", "Brasil", "Chile", "Colômbia", "Equador", "Guiana", "Paraguai", "Peru", "Suriname", "Uruguai", "Venezuela", "Guiana Francasa"]

paises_lit = lit(paises)

df_nomes = df_nomes.withColumn("Pais",
                               paises_lit.getItem(floor(rand() * len(paises))))


## ETAPA 5


df_nomes = df_nomes.withColumn("AnoNascimento",
                               floor(rand() * (2010 - 1945 + 1)) + 1945)


## ETAPA 6


df_select = df_nomes.filter(df_nomes["AnoNascimento"] >= 2000).select("Nomes", "Escolaridade", "Pais", "AnoNascimento")


# ETAPA 7


df_nomes.createOrReplaceTempView("pessoas")

spark.sql("""
SELECT nomes, AnoNascimento
FROM pessoas
WHERE AnoNascimento >= 2000
LIMIT 10
""")


## ETAPA 8


millenials = df_nomes.filter((df_nomes["AnoNascimento"] >= 1980) & (df_nomes["AnoNascimento"] <= 1994)).select("Nomes")

numero_millenials = millenials.count()

print(f'numero de millenials: {numero_millenials}')


## ETAPA 9


spark.sql("""
SELECT count(*)
FROM pessoas
WHERE AnoNascimento >= 1980 AND
      AnoNascimento <= 1994
""")


## ETAPA 10


df_geracao = spark.sql("""
            SELECT Pais, 
                CASE 
                    WHEN AnoNascimento BETWEEN 1944 AND 1964 THEN 'Baby Boomers'
                    WHEN AnoNascimento BETWEEN 1965 AND 1979 THEN 'Geração X'
                    WHEN AnoNascimento BETWEEN 1980 AND 1994 THEN 'Millennials (Geração Y)'
                    WHEN AnoNascimento BETWEEN 1995 AND 2015 THEN 'Geração Z'
                END AS Geracao,
                COUNT(*) AS Quantidade
            FROM pessoas
            GROUP BY Pais, Geracao
            ORDER BY Pais, Geracao, Quantidade
                    """)

df_geracao.show()
