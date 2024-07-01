import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, count, upper, desc, max, when

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
    {"withHeader": True, "separator":","},
    )
    
df = dynamic_frame.toDF()

df.printSchema()

df = df.withColumn("nome", upper(col("nome")))

num_rows = df.select(count('*')).first()[0]

print(num_rows)

new_df = df.groupBy("ano", "sexo").agg(count("*").alias("count_names"))

new_df = new_df.orderBy(col("ano").desc())

new_df.show()

female_count = df.filter(col("sexo") == "F").groupBy("nome").agg(count("*").alias("count"))

row_count_max = female_count.orderBy(desc("count")).first()
popular_name = row_count_max["nome"]
max_count = row_count_max["count"]

year_with_max = df.filter((col("nome") == popular_name) & (col("sexo") == "F")) \
                        .groupBy("ano").agg(count("*").alias("count")) \
                        .orderBy(desc("count")).first()["ano"]
                        
print(f"O nome feminino com mais registros foi '{popular_name}' com {max_count} registros.")
print(f"Isso ocorreu no ano de {year_with_max}.")

male_count = df.filter(col("sexo") == "M").groupBy("nome").agg(count("*").alias("count2"))

row_count_max2 = male_count.orderBy(desc("count2")).first()
popular_name_male = row_count_max2["nome"]
max_count2 = row_count_max2["count2"]

year_with_max2 = df.filter((col("nome") == popular_name_male) & (col("sexo") == "M")) \
                        .groupBy("ano").agg(count("*").alias("count2")) \
                        .orderBy(desc("count2")).first()["ano"]
                        
print(f"O nome feminino com mais registros foi '{popular_name_male}' com {max_count2} registros.")
print(f"Isso ocorreu no ano de {year_with_max2}.")

total_registros_por_ano = df.groupBy("ano").agg(count("*").alias("total_registros")).orderBy("ano").limit(10)

df.write.partitionBy("sexo", "ano").mode("overwrite").json(target_path)

job.commit()