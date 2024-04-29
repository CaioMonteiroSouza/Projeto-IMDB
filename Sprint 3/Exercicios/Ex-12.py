list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
new_list = []

def my_map(list, f):
    for x in list:
        new_list.append(f(x))


def potencia_2(x):
    return x ** 2

my_map(list, potencia_2)
print(new_list)
