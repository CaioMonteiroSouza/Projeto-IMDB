numeros = []

with open('number.txt') as data:
    numeros = map(int, data.readlines())

maiores_numeros_pares = list(sorted(filter(lambda x: x % 2 == 0, numeros), reverse=True)[:5])
soma = sum(maiores_numeros_pares)

print(maiores_numeros_pares)
print(soma)