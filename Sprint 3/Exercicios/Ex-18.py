speed = {'jan':47, 'feb':52, 'march':47, 'April':44, 'May':52, 'June':53, 'july':54, 'Aug':44, 'Sept':54}
new_list = set()


for x, y in speed.items():
    if y not in new_list:
        new_list.add(y)

print(list(new_list))