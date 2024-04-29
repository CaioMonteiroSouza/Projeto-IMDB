numbers = "1,3,4,6,10,76"
numbers_list = numbers.split(',')
numbers_list = [int(x) for x in numbers_list]
soma_total = 0

for x in numbers_list: 
    if x: 
        soma_total += x

print(soma_total)