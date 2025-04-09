import json
from pathlib import Path

from flask import Blueprint, jsonify
from auth import require_auth
from flask import request

from config import DATA_PATH

token_actions_bp = Blueprint('token_actions_bp', __name__)
       
@token_actions_bp.route('/get-tokens')
@require_auth
def get_tokens():
    with open('customers.json', 'r') as f:
       customers = json.load(f)
    
    access_tokens = []
    
    for user in customers['users']:
        token_file = Path(DATA_PATH / user['folder'] / 'access_token.json')
        token_file_new = Path(DATA_PATH / user['folder'] / 'access_token_new.json')
        if token_file.exists():
            with open(token_file, 'r', encoding='utf-8') as f:
                token_data = json.load(f)
                access_tokens.append({
                    'customer_folder': user['folder'],
                    'access_token': token_data.get('access_token'),
                    'expires_at': token_data.get('expires_at'),
                    'refresh_token': token_data.get('refresh_token'),
                    'type_token': 'old'
                })

        if token_file_new.exists():
            with open(token_file_new, 'r', encoding='utf-8') as f:
                token_data = json.load(f)
                access_tokens.append({
                    'customer_folder': user['folder'],
                    'access_token': token_data.get('access_token'),
                    'expires_at': token_data.get('expires_at'),
                    'refresh_token': token_data.get('refresh_token'),
                    'type_token': 'new'
                })
    
    return jsonify(access_tokens)

@token_actions_bp.route('/insert-tokens', methods=['POST'])
def insert_tokens():
    data = request.get_json()
    customers = data.get('customers', [])
    customers_json_path = Path("customers.json")

    # Garantir que o diretório de dados exista
    DATA_PATH.mkdir(parents=True, exist_ok=True)

    # Verificar se o arquivo customers.json existe, senão criar um novo
    if not customers_json_path.exists():
        customers_data = {"users": []}
    else:
        with open(customers_json_path, 'r', encoding='utf-8') as f:
            customers_data = json.load(f)

    for customer in customers:
        customer_folder = customer.get('customer_folder')
        access_token = customer.get('access_token')
        expires_at = customer.get('expires_at')
        refresh_token = customer.get('refresh_token')
        type_token = customer.get('type_token')

        customer_folder_path = DATA_PATH / customer_folder
        if type_token == 'new':
            token_file = customer_folder_path / 'access_token_new.json'
        else:
            token_file = customer_folder_path / 'access_token.json'

        # Criar a pasta do cliente se não existir
        customer_folder_path.mkdir(parents=True, exist_ok=True)

        # Verificar se o cliente já está no customers.json, se não, adicioná-lo
        if not any(user['folder'] == customer_folder for user in customers_data.get('users', [])):
            customers_data['users'].append({
                'id': customer.get('customer_id') or customer_folder, 
                'folder': customer_folder
            })

            # Salvar o customers.json atualizado
            with open(customers_json_path, 'w', encoding='utf-8') as f:
                json.dump(customers_data, f, ensure_ascii=False, indent=4)

        # Criar ou atualizar o access_token.json
        with open(token_file, 'w', encoding='utf-8') as f:
            json.dump({
                'access_token': access_token,
                'expires_at': expires_at,
                'refresh_token': refresh_token,
                'expires_in': 3600,
                'token_type': 'bearer'
            }, f, ensure_ascii=False, indent=4)

    return jsonify({"message": "Tokens inserted successfully"})