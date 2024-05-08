def calcular_valor_maximo(operadores,operandos) -> float:
   return max(map(lambda x: eval(f'{str(x[1][0])}{x[0]}{str(x[1][1])}'),zip(operadores, operandos)))

operadores = ['+','-','*','/','+']
operandos  = [(3,6), (-7,4.9), (8,-8), (10,2), (8,4)]

print(calcular_valor_maximo(operadores, operandos))