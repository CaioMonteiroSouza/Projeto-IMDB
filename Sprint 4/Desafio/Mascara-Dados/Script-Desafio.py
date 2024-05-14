import hashlib

control = True
while(control):
    print('0 - Encerrar Script\n1 - Inserir string')
    ope = input('Digite o codigo da operação: ')
    if ope == '1':
        string = input('Digite a string a ser mascarada: ')
        objeto_hash = hashlib.sha1(string.encode())
        print(objeto_hash.hexdigest())
    elif ope == '0':
        control = False
    else:
        print('Valor Não aceito, digite novamente')