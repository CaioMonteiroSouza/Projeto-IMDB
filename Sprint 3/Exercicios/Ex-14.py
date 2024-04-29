def func(*args, **kwargs):
    for x in args: print(x)
    for x, y in kwargs.items(): print(y)

func(1, 3, 4, 'hello', parametro_nomeado='alguma coisa', x=20)  