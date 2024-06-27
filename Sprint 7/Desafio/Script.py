import requests
from dotenv import load_dotenv
import os
import pandas as pd
import json
from datetime import datetime


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
    if response:
        data = turn_to_json(response)
        internal_id = get_movie_internal_id(data)
        response_details = send_serie_internal_id_request(internal_id)
        data_details = turn_to_json(response_details)
        if data_details.get("success", True):
            filename = f'Sprint 7\\Desafio\\Jsons\\Complementar_Csv\\series_{file_index}.json'
            with open(filename, 'a') as f:
                json.dump(data_details, f)
                f.write('\n')
                return True
        else:
            return False


def send_requisitions(ids_filtrados):
    file_index = proximo_numero_serie('Sprint 7\\Desafio\\Jsons\\Complementar_Csv')
    count = 0
    for serie_id in ids_filtrados:
        if (get_serie_details(serie_id, file_index)):
            count += 1
            if count >= 99:
                file_index += 1
                count = 0


def proximo_numero_serie(diretorio):
    numbers = []
    if len(os.listdir(diretorio)):
        for name in os.listdir(diretorio):
            if name.startswith('series_') and name.endswith('.json'):
                    number = int(name[7:-5])
                    numbers.append(number)
    else: return 1
    return max(numbers) + 1


def get_top_rated_series():
    response = requests.get('https://api.themoviedb.org/3/tv/top_rated?language=en-US&page=1', headers=headers)
    if response:
        data = turn_to_json(response)
        filtered_results = [item for item in data["results"] if 16 in item["genre_ids"]]
        
        filename = f'Sprint 7\\Desafio\\Jsons\\Top_Rated\\{year}\\{month}\\{day}\\top_rated_{year}-{month}-{day}.json'
        with open(filename, 'a') as f:
            for item in filtered_results:
                json.dump(item, f)
                f.write('\n')
        return True


def get_popular_series():
    response = requests.get('https://api.themoviedb.org/3/tv/popular?language=en-US&page=1', headers=headers)
    if response:
        data = turn_to_json(response)
        filtered_results = [item for item in data["results"] if 16 in item["genre_ids"]]
        
        filename = f'Sprint 7\\Desafio\\Jsons\\Popular\\{year}\\{month}\\{day}\\Popular_{year}-{month}-{day}.json'
        with open(filename, 'a') as f:
            for item in filtered_results:
                json.dump(item, f)
                f.write('\n')
        return True

if __name__ == "__main__":
    now = datetime.now()
    day = now.day
    month = now.month
    year = now.year

    df = pd.read_csv('Sprint 6\\Desafio\\data\\series.csv', sep='|', low_memory=False)

    directory_path_top_rated = f'Sprint 7/Desafio/Jsons/Top_Rated/{year}/{month}/{day}/'
    os.makedirs(directory_path_top_rated, exist_ok=True)

    directory_path_popular = f'Sprint 7/Desafio/Jsons/Popular/{year}/{month}/{day}/'
    os.makedirs(directory_path_popular, exist_ok=True)

    genero = 'Animation'
    ids_filtrados = df[df['genero'] == genero]['id']
    ids_filtrados = list(set(ids_filtrados))


    load_dotenv()


    API_KEY = os.getenv("API_KEY")
    TOKEN_LEITURA = os.getenv("TOKEN_LEITURA")

    headers = {
    "accept": "application/json",
    "Authorization": f'Bearer {TOKEN_LEITURA}'
    }

    
    send_requisitions(ids_filtrados)

    get_top_rated_series()

    get_popular_series()