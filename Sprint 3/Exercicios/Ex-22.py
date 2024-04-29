class Pessoa():
    def __init__(self, id, *nome):
        self.__nome = nome
        self.id = id
    
    @property
    def nome(self):
        return self.__nome

    @nome.setter
    def nome(self, novo):
        self.__nome = novo

pessoa = Pessoa(0)
pessoa.nome = 'Fulano De Tal'
print(pessoa.nome)
