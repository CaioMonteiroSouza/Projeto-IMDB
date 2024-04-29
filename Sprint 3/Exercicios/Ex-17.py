lista = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

def split_lista(lista):
    length = len(lista)
    if length % 3 != 0:
        ind = length // 3
        primeira_lista = lista[:ind]
        segunda_lista = lista[ind:(2 * ind)]
        terceira_lista = lista[(2* ind):(3 * ind + 1)]
    else:
        ind = length // 3
        primeira_lista = lista[:ind]
        segunda_lista = lista[ind:(2 * ind)]
        terceira_lista = lista[(2* ind):(3 * ind)]
    print(primeira_lista, segunda_lista, terceira_lista)


split_lista(lista)
