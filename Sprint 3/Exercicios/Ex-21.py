class Passaro:
    def __init__(self, voando='Voando...', som='Piu Piu'):
        self.voando = voando
        self.som = som

    def voar(self):
        print(self.voando)

    def emitir_som(self):
        print(self.som)

class Pato(Passaro):
    def __init__(self, voando='Voando...', som='Quack Quack', nome="Pato"):
        super().__init__(voando, som)
        self.nome = nome 
        print(nome)

    def emitir_som(self):
        print(f'{self.nome} emitindo som... \n{self.som}')
    

class Pardal(Passaro):
    def __init__(self, voando='Voando...', som='Piu Piu', nome="Pardal"):
        super().__init__(voando, som)
        self.nome = nome 
        print(nome)

    def emitir_som(self):
        print(f'{self.nome} emitindo som... \n{self.som}')
    


pato = Pato()
pato.voar()
pato.emitir_som()
pardal = Pardal()
pardal.voar()
pardal.emitir_som()
