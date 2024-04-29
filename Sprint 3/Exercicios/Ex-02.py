import random as rd

'''
Como pensei em fazer na primeira vez
uso randint para gerar numeros aleatorios em toda execução
'''
numeros = []

for x in range(3):
    numeros.append(rd.randint(1,100))


for x in numeros:
    print(f"{'Par' if x % 2 == 0 else 'Ímpar'}: {x}")

'''
Como a udemy aceitou


numeros = []

for x in range(3):
    numeros.append(x)

for x in numeros:
    if x % 2 == 0:
        print(f'Par: {x}')
    else:
        print(f'Ímpar: {x}')
'''