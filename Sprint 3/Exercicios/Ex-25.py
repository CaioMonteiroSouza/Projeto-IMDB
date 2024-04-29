class Aviao:
    def __init__(self, modelo, velocidade_maxima, capacidade, cor = 'Azul'):
        self.modelo = modelo
        self.velocidade_maxima = velocidade_maxima
        self.capacidade = capacidade
        self.cor = cor

    def __str__(self):
        return f'O avião de modelo {self.modelo} possui uma velocidade máxima de {self.velocidade_maxima}, capacidade para {self.capacidade} passageiros e é da cor {self.cor}'

aviao1 = Aviao('BOIENG456', '1500 km/h', '400', 'Azul')  
aviao2 = Aviao('Embraer Praetor 600', '863km/h', '14', 'Azul')
aviao3 = Aviao('Antonov An-2', '258', '12', 'Azul')

lista_avioes = []
lista_avioes.append(aviao1)
lista_avioes.append(aviao2)
lista_avioes.append(aviao3)

for x in lista_avioes:
    print(x)

