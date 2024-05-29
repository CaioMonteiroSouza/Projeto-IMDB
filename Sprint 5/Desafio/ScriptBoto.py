import boto3
import os

os.environ['AWS_ACCESS_KEY_ID'] = 'ASIA47CRY4E5UQMU2Z4E'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'JaHr1bXw4BBcYe5JZKGKetoXnk9uUN7FtHbH/pT5'
os.environ['AWS_SESSION_TOKEN'] = 'IQoJb3JpZ2luX2VjEJP//////////wEaCXVzLWVhc3QtMSJHMEUCIEh0vneODFL4TJgYRd3dT3IayjzwWYj8tTFXdITgNt7PAiEAnV6nhnqVCET6oX2T7AmDuM4G7Tc/UxhPP/S/eGs/E34qmQMIHBAAGgw4OTEzNzcyMTM3NTUiDBVTsh8/JId7s9OYUyr2Au9FSmw5GEkp5kmjwN7zI+g97y7gvgzBJudCN43W7gOc89WjSAfnokQRs7ShV2voUjhupIEYIAYoZFgcXHViJl/dL2rWHQIi+G4xEh3sbG9MiadrJoCJ9sHrMrKA5nJXbDIpizw6x3aUJqrr+xE831ijjWOJzbRT80syJjzpHQo+Ee3ewDjqEap7ZgoeBNS3PJuqFgQ8X6y6Zx6pvjgdfTPmQ8ppql9BNPnK7PUnYLTL6N50y+s9A1am77F7wo6pPfbC0l1QOMavFw2agVmikmleeSlc057HX9npGlUQT8dYAKHtxJ4ZKkTGwWJNDI1nwctcADH1bC1heRp8H+Qqg9vw2/yWk+gzdrmDAD55eczZjZCbGdhY+25pO/DICE00Sfg4389WYm3Qq0JBeKl9WhSJqy1k7nvizJAp5peUj+VR7wNvFbxeoR0Jj+/+5G36bw037vm45R6wXUHBuBvdd5Pnyo7BROBawsZQShN6y0Hgl5rLlfNrMLPn3bIGOqYBmG6EuJ6BrbKXOIXpTK8L67XnS7s9zWeCItzPPQCmrsoHC6RKCjZhh87yfXdka/pG8AOTPmQW3pOGePtWhZIofvdLDCwvs7wEMhDLbiA9UdNxOEePFfLyao9cvJed2Iqk5ecRFvIbLARjioVrtwoCqQtIX7Bslna3BENAmJS9fFmVGkySvaajw1e3k57ghHEDVa6EMqmJancF97dJeo5sVlL5R77DQg=='

def query_s3_csv(bucket_name, object_key, sql_expression):
    # Create a session using Boto3
    s3 = boto3.client('s3')

    # Define the request parameters for S3 Select
    request_params = {
        'Bucket': bucket_name,
        'Key': object_key,
        'ExpressionType': 'SQL',
        'Expression': sql_expression,
        'InputSerialization': {
            'CSV': {
                'FileHeaderInfo': 'USE',  # 'USE' if the CSV file contains a header row
                'RecordDelimiter': '\n',
                'FieldDelimiter': ';',
                'AllowQuotedRecordDelimiter': True  # Allows quoted record delimiters
            }
        },
        'OutputSerialization': {
            'CSV': {}
        }
    }

    # Execute the query
    response = s3.select_object_content(**request_params)

    # Process the response
    for event in response['Payload']:
        if 'Records' in event:
            print(event['Records']['Payload'].decode('utf-8'))
        elif 'Stats' in event:
            print("Processed {} bytes".format(event['Stats']['Details']['BytesProcessed']))
        elif 'End' in event:
            print("Query finished")

# Example usage
bucket_name = 'caio-desafiosprint5'
object_key = 'V_OCORRENCIA_AMPLA.csv'
sql_expression = "SELECT \
                    CASE WHEN (s.Operacao) = 'Operação Pública' OR (s.Operacao) = 'Operação Especializada' OR (s.Operacao) = 'Operação Militar' OR (s.Operacao) = 'Operação Policial'\
                        THEN 'Voo governamental'\
                        ELSE 'Voo não governamental' \
                    END AS GOVERNAMENTAL, \
                    (CASE \
                         WHEN s.Lesoes_Fatais_Tripulantes IS NULL OR s.Lesoes_Fatais_Tripulantes = 'null' THEN 0 \
                        ELSE CAST(s.Lesoes_Fatais_Tripulantes AS INT)\
                        END) +\
                    (CASE \
                        WHEN s.Lesoes_Fatais_Passageiros IS NULL OR s.Lesoes_Fatais_Passageiros = 'null' THEN 0\
                        ELSE CAST(s.Lesoes_Fatais_Passageiros AS INT)\
                    END) +\
                    (CASE \
                        WHEN s.Lesoes_Fatais_Terceiros IS NULL OR s.Lesoes_Fatais_Terceiros = 'null' THEN 0\
                        ELSE CAST(s.Lesoes_Fatais_Terceiros AS INT)\
                    END) AS Soma_Lesoes_Fatais \
                FROM s3object s \
                    WHERE (s.Lesoes_Fatais_Tripulantes <> '' AND (s.Lesoes_Fatais_Tripulantes <> '0' AND s.Lesoes_Fatais_Tripulantes <> 'null')) \
                        OR (s.Lesoes_Fatais_Passageiros <> '' AND (s.Lesoes_Fatais_Passageiros <> '0' AND s.Lesoes_Fatais_Passageiros <> 'null')) \
                        OR (s.Lesoes_Fatais_Terceiros <> '' AND (s.Lesoes_Fatais_Terceiros <> '0' AND s.Lesoes_Fatais_Terceiros <> 'null'))"

query_s3_csv(bucket_name, object_key, sql_expression)