import base64
import json
from datetime import datetime, timedelta
from pathlib import Path
import requests
from flask import Blueprint, jsonify
from config import DATA_PATH

from config import (
    CLIENT_NEW_ID, CLIENT_NEW_SECRET, REDIRECT_NEW_URI,
    TOKEN_FILE, TOKEN_NEW_URL
)

token_new_bp = Blueprint('token_new', __name__)

class TokenNewManager:
    def __init__(self, customer_id):
        self.customer_id = customer_id

    def _get_customer_folder(self):
        with open('customers.json', 'r') as f:
            customers = json.load(f)
        for user in customers['users']:
            if user['id'] == self.customer_id:
                return user['folder']
        return None

    def _get_token_file_path(self):
        folder = self._get_customer_folder()
        if not folder:
            return None
        return Path(DATA_PATH) / folder / 'access_token_new.json'  # Corrigido aqui

    def _read_token_file(self):
        token_file = self._get_token_file_path()
        if not token_file.exists():
            return None
        
        with open(token_file, 'r', encoding='utf-8') as f:
            return json.load(f)

    def _write_token_file(self, token_info):
        folder = self._get_customer_folder()
        if not folder:
            return False
        
        token_file = self._get_token_file_path()
        if not token_file:
            return False
            
        token_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(token_file, 'w', encoding='utf-8') as f:
            json.dump(token_info, f, ensure_ascii=False, indent=2)
        return True

    def _get_basic_auth(self):
        """Get Basic Auth header for token requests."""
        credentials = f"{CLIENT_NEW_ID}:{CLIENT_NEW_SECRET}"
        encoded = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
        return f"Basic {encoded}"

    def is_token_expired(self):
        token_info = self._read_token_file()
        if not token_info:
            return True
        
        expiry_time = datetime.fromisoformat(token_info['expires_at'])
        return datetime.now() > (expiry_time - timedelta(minutes=5))

    def refresh_token(self):
        token_info = self._read_token_file()
        if not token_info or 'refresh_token' not in token_info:
            return None, "No refresh token found in token file"
        
        try:
            # 1. Preparar autenticação Basic
            auth_string = f"{CLIENT_NEW_ID}:{CLIENT_NEW_SECRET}"
            basic_auth = "Basic " + base64.b64encode(auth_string.encode()).decode('utf-8')
            
            # 2. Headers conforme documentação
            headers = {
                "Authorization": basic_auth,
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json"
            }
            
            # 3. Dados no formato que a API espera
            payload = {
                "grant_type": "refresh_token",
                "refresh_token": token_info['refresh_token'],
                "client_id": CLIENT_NEW_ID,
                "client_secret": CLIENT_NEW_SECRET,
            }
            
            # 4. Converter payload para formato x-www-form-urlencoded
            data = "&".join([f"{k}={v}" for k, v in payload.items()])
            
            # 5. Fazer a requisição
            response = requests.post(
                "https://auth.contaazul.com/oauth2/token",
                headers=headers,
                data=data,
                timeout=30
            )
            
            # 6. Verificar resposta
            if response.status_code != 200:
                error_detail = response.json().get('error_description', response.text)
                return None, f"API Error {response.status_code}: {error_detail}"
            
            new_token_info = response.json()

            # Se não veio um novo refresh_token, mantém o antigo
            if 'refresh_token' not in new_token_info:
                new_token_info['refresh_token'] = token_info['refresh_token']

            # Calcular nova data de expiração
            new_token_info['expires_at'] = (
                datetime.now() + timedelta(seconds=new_token_info['expires_in'])
            ).isoformat()

            # Salvar token atualizado
            if not self._write_token_file(new_token_info):
                return None, "Failed to save new token"
            
        except requests.exceptions.RequestException as e:
            return None, f"Request Failed: {str(e)}"
        except Exception as e:
            return None, f"Unexpected Error: {str(e)}"


@token_new_bp.route('/refresh_token-new/<customer_id>')
def refresh_token_endpoint(customer_id):
    """Endpoint to refresh the access token."""
    manager = TokenNewManager(customer_id)
    
    if not manager.is_token_expired():
        return jsonify({
            "message": "Token is still valid",
        })

    token_info, error = manager.refresh_token()
    
    if error:
        return jsonify({
            "error": "Failed to refresh token",
            "details": error
        }), 500

    return jsonify({
        "message": "Token refreshed successfully",
    })
