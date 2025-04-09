from flask import Blueprint, jsonify
from pathlib import Path
import json
from datetime import datetime
from functools import wraps

from config import DATA_PATH
from auth import require_auth

# Create a Blueprint for data routes
data_bp = Blueprint('data', __name__)

@data_bp.route('/read/<customer_id>/<data_type>')
@require_auth
def read_data(customer_id, data_type):
    """
    Lê dados de um cliente específico com base no tipo de dado solicitado.
    
    Args:
        customer_id (str/int): ID do cliente
        data_type (str): Tipo de dado a ser lido (ex: 'orders', 'profile')
    
    Returns:
        Response: JSON com os dados ou mensagem de erro
    """
    try:
        # Passo 1: Localizar o cliente no arquivo customers.json
        customers_file = Path('customers.json')
        if not customers_file.exists():
            return jsonify({"error": "Arquivo customers.json não encontrado"}), 404

        with open(customers_file, 'r', encoding='utf-8') as f:
            customers = json.load(f)
        
        # Encontrar a pasta do cliente
        customer_folder = None
        for user in customers.get('users', []):
            if str(user.get('id')) == str(customer_id):
                customer_folder = user.get('folder')
                break
        
        if not customer_folder:
            return jsonify({"error": f"Cliente {customer_id} não encontrado"}), 404

        # Passo 2: Construir o caminho para o arquivo de dados
        data_file = DATA_PATH / customer_folder / f"{data_type}_data.json"
        
        if not data_file.exists():
            return jsonify({"error": f"Arquivo {data_type} não encontrado para o cliente"}), 404

        # Passo 3: Ler e retornar os dados
        with open(data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        return jsonify({
            "customer_id": customer_id,
            "data_type": data_type,
            "data": data
        })

    except json.JSONDecodeError:
        return jsonify({"error": "Erro ao decodificar arquivo JSON"}), 500
    except Exception as e:
        return jsonify({
            "error": f"Error reading {data_type} data",
            "details": str(e)
        }), 500

@data_bp.route('/list/<customer_id>')
@require_auth
def list_data(customer_id):
    try:
        # Get customer-specific folder
        with open('customers.json', 'r') as f:
            customers = json.load(f)
        
        customer_folder = None
        for user in customers['users']:
            if user['id'] == customer_id:
                customer_folder = user['folder']
                break
        
        if not customer_folder:
            return jsonify({
                "error": f"Customer {customer_id} not found"
            }), 404
        
        # Get all JSON files in the data directory
        files = list(Path(DATA_PATH / customer_folder).glob("*.json"))
        
        file_info = []
        for file in files:
            if file.name != "access_token.json":  # Skip the token file
                file_info.append({
                    "name": file.name,
                    "size": file.stat().st_size,
                    "modified": datetime.fromtimestamp(file.stat().st_mtime).isoformat()
                })
                
        return jsonify({
            "message": "Successfully listed data files",
            "files": file_info
        })

    except Exception as e:
        return jsonify({
            "error": "Error listing data files",
            "details": str(e)
        }), 500
