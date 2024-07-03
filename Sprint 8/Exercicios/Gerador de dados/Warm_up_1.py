import random

numeros_int = []

for x in range(1,251):
    numeros_int.append(random.randint(0,1000))

print('Numeros antes do reverse:')
print(numeros_int)

numeros_int.reverse()

print('Numeros após o reverse:')
print(numeros_int)