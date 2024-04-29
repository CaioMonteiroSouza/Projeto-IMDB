class Calculo():
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def soma(self):
        return self.x + self.y
    
    def subtracao(self):
        return self.x - self.y
    
calculo = Calculo(4,5)
print(calculo.soma())
print(calculo.subtracao())
