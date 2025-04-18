# read json 

import json

# read json file
with open('./data/private/assessoriaContabil/accounts_payable_data.json', encoding='utf-8') as f:
    data = json.load(f)

# calcular valores somados de uma chave especifica

def calc(data, key):
    total = 0
    for item in data:
        if key in item:
            total += item[key]
    return total

print("Total de valores somados: ", calc(data, 'nao_pago') - calc(data, 'pago'))