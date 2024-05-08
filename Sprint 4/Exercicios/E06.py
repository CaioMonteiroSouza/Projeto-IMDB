def maiores_que_media(conteudo:dict)->list:
    media = sum(conteudo.values()) / len(conteudo)
    return list(sorted(filter(lambda x: x[1] > media, conteudo.items()), key=lambda x: x[1]))

conteudo = {
    "arroz": 4.99,
    "feijão": 3.49,
    "macarrão": 2.99,
    "leite": 3.29,
    "pão": 1.99
}
