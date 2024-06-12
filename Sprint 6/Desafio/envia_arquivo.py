import boto3
import os
from dotenv import load_dotenv
from botocore.exceptions import NoCredentialsError

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

s3 = boto3.client('s3',
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                    region_name=AWS_DEFAULT_REGION,
                    aws_session_token=AWS_SESSION_TOKEN
                    )

def upload_file(file_name, bucket_name, path):

    try:
        s3.upload_file(file_name, bucket_name, path)
        print(f'arquivo: {file_name} enviado com sucesso')
        return True
    except FileNotFoundError:
        print("Arquivo não encontrado")
        return False
    except NoCredentialsError:
        print("Sem credenciais")
        return False
    
local_files = './data/'
s3_folder_movies = 'Raw/Local/CSV/Movies/2024/06/12/movies.csv'
s3_folder_series = 'Raw/Local/CSV/Series/2024/06/12/series.csv'
bucket_name = 'datalakecaio'

for file in os.listdir(local_files):
    file_path = os.path.join(local_files, file)
    if os.path.isfile(file_path):
        if file == "movies.csv":
            upload_file(file_path, bucket_name, s3_folder_movies)
        elif file == "series.csv":
            upload_file(file_path, bucket_name, s3_folder_series)