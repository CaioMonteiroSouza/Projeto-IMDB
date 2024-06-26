import requests
from dotenv import load_dotenv
import os
import pandas as pd

df = pd.read_csv('Sprint 6\\Desafio\\data\\series.csv', sep='|', low_memory=False)

genero = 'Animation'
ids_filtrados = df[df['genero'] == genero]['id']

load_dotenv()

API_KEY = os.getenv("API_KEY")
TOKEN_LEITURA = os.getenv("TOKEN_LEITURA")


headers = {
    "accept": "application/json",
    "Authorization": f'Bearer {TOKEN_LEITURA}'
}

def get_movie_internal_id(data):
    if 'tv_results' in data:
        if not data['tv_results']: 
            print("tv_results está vazio.")
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


def get_serie_details(serie_id):
    response = send_serie_external_id_request(serie_id)
    if response:
        data = turn_to_json(response)
        internal_id = get_movie_internal_id(data)
        response_details = send_serie_internal_id_request(internal_id)
        data_details = turn_to_json(response_details)
        print(data_details)
    else: 
        print('filme não encontrado')
        return

def send_requisitions(ids_filtrados):
    ids_filtrados = ids_filtrados.tolist()
    for serie_id in ids_filtrados:
        get_serie_details(serie_id)


send_requisitions(ids_filtrados)