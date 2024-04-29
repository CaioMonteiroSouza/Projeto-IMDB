import random

random_list = random.sample(range(500), 50)

random_list.sort()


mediana = random_list[len(random_list) // 2] if len(random_list) % 2 != 0 else ((random_list[len(random_list) // 2] + random_list[(len(random_list) // 2) - 1]) / 2)
media = sum(random_list) / len(random_list)
valor_minimo = min(random_list)
valor_maximo = max(random_list)

print(f'Media: {media}, Mediana: {mediana}, Mínimo: {valor_minimo}, Máximo: {valor_maximo}')