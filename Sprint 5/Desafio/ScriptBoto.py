import boto3

def query_s3_csv(bucket_name, object_key, sql_expression):
    s3 = boto3.client('s3')
    request_params = {
        'Bucket': bucket_name,
        'Key': object_key,
        'ExpressionType': 'SQL',
        'Expression': sql_expression,
        'InputSerialization': {
            'CSV': {
                'FileHeaderInfo': 'USE',
                'RecordDelimiter': '\n',
                'FieldDelimiter': ';',
                'AllowQuotedRecordDelimiter': True
            }
        },
        'OutputSerialization': {
            'CSV': {}
        }
    }

    response = s3.select_object_content(**request_params)

    for event in response['Payload']:
        if 'Records' in event:
            print(event['Records']['Payload'].decode('utf-8'))

bucket_name = 'caio-desafiosprint5'
object_key = 'V_OCORRENCIA_AMPLA.csv'
sql_expression = """
                SELECT
                    SUBSTRING(CAST(UTCNOW() AS STRING), 0, 11),
                    COUNT(*) AS VOOS_COM_FATALIDADES,  
                    SUM((CASE 
                         WHEN s.Lesoes_Fatais_Passageiros IS NULL OR s.Lesoes_Fatais_Passageiros = 'null' OR s.Lesoes_Fatais_Passageiros = '0' THEN 0 
                        ELSE 1
                        END)) AS Soma_Lesoes_Fatais_Passageiros
                FROM s3object s
                    WHERE 
                        (s.Lesoes_Fatais_Tripulantes <> '' AND (s.Lesoes_Fatais_Tripulantes <> '0' AND s.Lesoes_Fatais_Tripulantes <> 'null')) 
                        OR (s.Lesoes_Fatais_Passageiros <> '' AND (s.Lesoes_Fatais_Passageiros <> '0' AND s.Lesoes_Fatais_Passageiros <> 'null')) 
                        OR (s.Lesoes_Fatais_Terceiros <> '' AND (s.Lesoes_Fatais_Terceiros <> '0' AND s.Lesoes_Fatais_Terceiros <> 'null'))
"""
query_s3_csv(bucket_name, object_key, sql_expression)