animais = ['Leão', 'Zebra', 'Girafa', 'Elefante', 'Rinoceronte', 'Hipopótamo', 'Crocodilo', 'Tigre', 'Onça', 'Guepardo', 'Cachorro', 'Gato', 'Cavalo', 'Vaca', 'Ovelha', 'Cabra', 'Galinha', 'Pato', 'Ganso', 'Peru']

animais.sort()

[print(animal) for animal in animais]

with open('.\\Sprint 8\\Exercicios\\Gerador de dados\\animais.csv', 'w', encoding='utf-8') as file:
    for animal in animais:
        file.write(animal + '\n')