import random
import time
import os
import names

random.seed(40)
qtd_nomes_unicos = 3000
qtd_nomes_aleatorios = 10000000

aux=[]

for i in range(0, qtd_nomes_unicos):
    aux.append(names.get_full_name())

print("Gerando {} nomes aleatorios".format(qtd_nomes_aleatorios))

dados=[]

for i in range(0, qtd_nomes_aleatorios):
    dados.append(random.choice(aux))

with open('.\\Sprint 8\\Exercicios\\Gerador de dados\\nomes_aleatorios.txt', 'w') as file:
    for nome in dados:
        file.write(nome + '\n')