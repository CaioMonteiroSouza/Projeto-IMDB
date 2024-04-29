class Lampada:
    def __init__(self, ligada):
        self.ligada = ligada

    def liga(self):
        self.ligada = True
        return self.ligada

    def desliga(self):
        self.ligada = False
        return self.ligada
    
    def esta_ligada(self):
        if self.ligada == True:
            return True
        else: 
            return False
        
    def __str__(self):
        status = 'A lâmpada está ligada? '
        if self.ligada == True:
            status += 'True'
        else:
            status += 'False'

        return status

    
        
lampada = Lampada(ligada=False)
lampada.liga()
lampada.esta_ligada()
print(lampada)
lampada.desliga()
print(lampada)
lampada.esta_ligada()


