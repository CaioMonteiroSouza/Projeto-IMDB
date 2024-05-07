from functools import reduce

def calcula_saldo(lancamentos) -> float:
    lista = list(map(lambda x: -x[0] if x[1] == 'D' else x[0], lancamentos))
    return reduce(lambda x, y: x + y, lista)

lancamentos = [
    (200, 'D'),
    (300, 'C'),
    (100, 'C')
]

print(calcula_saldo(lancamentos))