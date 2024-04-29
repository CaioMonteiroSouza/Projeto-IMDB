def is_primo(numero):
    '''
    numero: recebe um inteiro e verifica se é primo
    numero_type: INT
    retorna: se o numero for primo um print do numero é realizado
    '''
    if numero <= 1:
        return 
    elif numero == 2:
        return print(numero)
    elif numero % 2 == 0:
        return False
    for i in range(3, numero):
        if numero % i == 0:
            return False
    return print(numero)

for x in range (1,101):
    is_primo(x)