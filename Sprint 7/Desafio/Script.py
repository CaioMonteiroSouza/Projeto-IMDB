import requests
from dotenv import load_dotenv
import os
import pandas as pd
import json
from datetime import datetime
import boto3

load_dotenv()


API_KEY = os.getenv("API_KEY")
TOKEN_LEITURA = os.getenv("TOKEN_LEITURA")

s3 = boto3.client('s3')

now = datetime.now()
day = now.day
month = now.month
year = now.year


headers = {
"accept": "application/json",
"Authorization": f'Bearer {TOKEN_LEITURA}'
}


def get_movie_internal_id(data):
    if 'tv_results' in data:
        if not data['tv_results']: 
            return None
        for movie in data['tv_results']:
            filme_id = movie.get('id', 'id não encontrado')
            return filme_id
    else:
        print("O campo 'tv_results' não foi encontrado na resposta da API.")
        return None


def turn_to_json(data):
    return data.json()


def send_serie_external_id_request(serie_id):
    return requests.get(f'https://api.themoviedb.org/3/find/{serie_id}?external_source=imdb_id&language={serie_id}', headers=headers)


def send_serie_internal_id_request(serie_id):
    return requests.get(f'https://api.themoviedb.org/3/tv/{serie_id}?language=en-US', headers=headers)


def get_serie_details(serie_id, file_index):
    response = send_serie_external_id_request(serie_id)
    if response.status_code == 200:
        data = turn_to_json(response)
        internal_id = get_movie_internal_id(data)
        response_details = send_serie_internal_id_request(internal_id)
        data_details = turn_to_json(response_details)
        if data_details.get("success", True):
            save_to_s3(data_details, 'datalakecaio', f'Raw/TMDB/JSON/Complementares/{year}/{month}/{day}/serie_{file_index}.json')
            return True
        else:
            return False


def send_requisitions(ids_filtrados):
    file_index = 1
    count = 0
    for serie_id in ids_filtrados:
        if (get_serie_details(serie_id, file_index)):
            count += 1
            if count >= 99:
                file_index += 1
                count = 0


def get_top_rated_series():
    response = requests.get('https://api.themoviedb.org/3/tv/top_rated?language=en-US&page=1', headers=headers)
    if response.status_code == 200:
        data = turn_to_json(response)
        filtered_results = [item for item in data["results"] if 16 in item["genre_ids"]]
        
        save_to_s3(filtered_results, 'datalakecaio', f'Raw/TMDB/JSON/Top_Rated/{year}/{month}/{day}/top_rated_{year}-{month}-{day}.json')
        return True
    return False


def get_popular_series():
    response = requests.get('https://api.themoviedb.org/3/tv/popular?language=en-US&page=1', headers=headers)
    if response.status_code == 200:
        data = turn_to_json(response)
        filtered_results = [item for item in data["results"] if 16 in item["genre_ids"]]
        
        save_to_s3(filtered_results, 'datalakecaio', f'Raw/TMDB/JSON/Popular/{year}/{month}/{day}/popular_{year}-{month}-{day}.json')
        return True
    return False


def save_to_s3(data, bucket_name, key):
     s3.put_object(Body=json.dumps(data), Bucket=bucket_name, Key=key)


def lambda_handler(event, context):
    bucket_name = 'datalakecaio'
    csv_key = 'Raw/Local/CSV/Series/2024/06/12/series.csv'
    csv_file = s3.get_object(Bucket=bucket_name, Key=csv_key)
    df = pd.read_csv(csv_file['Body'], sep='|', low_memory=False)

    s3.put_object(Bucket=bucket_name, Key=f'Raw/TMDB/JSON/Top_Rated/{year}/{month}/{day}/')
    s3.put_object(Bucket=bucket_name, Key=f'Raw/TMDB/JSON/Popular/{year}/{month}/{day}/')
    s3.put_object(Bucket=bucket_name, Key=f'Raw/TMDB/JSON/Complementares/{year}/{month}/{day}/')

    genero = 'Animation'
    ids_filtrados = df[df['genero'] == genero]['id']
    ids_filtrados = list(set(ids_filtrados))

    get_top_rated_series()

    get_popular_series()