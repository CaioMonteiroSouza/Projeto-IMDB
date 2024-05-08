lines = []

with open('estudantes.csv') as data:
    for x in data:
        lines.append(x.strip().split(','))

def get_nome(line):
    return line[0]

def get_maiores_notas(line):
    return sorted(map(int, line[1:]), reverse=True)[:3]

def media(line):
    return round(sum(get_maiores_notas(line)) / 3, 2)

for line in sorted(lines, key=lambda x: x[0]):
    print(f'Nome: {get_nome(line)} Notas: {get_maiores_notas(line)} Média: {media(line)}')