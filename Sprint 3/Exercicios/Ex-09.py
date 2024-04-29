primeirosNomes = ['Joao', 'Douglas', 'Lucas', 'José']
sobreNomes = ['Soares', 'Souza', 'Silveira', 'Pedreira']
idades = [19, 28, 25, 31]

zip_completo = zip(primeirosNomes, sobreNomes, idades)

for index, (primeiroNome, sobreNome, idade) in enumerate(zip_completo):
    print(f'{index} - {primeiroNome} {sobreNome} está com {idade} anos')