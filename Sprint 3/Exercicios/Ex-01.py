from datetime import datetime

"""
Idade: Numero deve ser menor que 100
Idade_Type: INT
Nome: String com o nome
Nome_Type: STR
Retorna o ano em que a pessoa fara seu 100° aniversario
Uso a função datetime.now() para que funcione independente do ano que este codigo seja executado
"""
idade = 70
nome = 'José'

idade_para_100 = 100 - idade
aniversario_100 = datetime.now().year + idade_para_100

print(aniversario_100)
