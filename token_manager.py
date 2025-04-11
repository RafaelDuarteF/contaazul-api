import base64
import json
from datetime import datetime, timedelta
from pathlib import Path
import requests
from flask import Blueprint, jsonify
from config import DATA_PATH

from config import (
    CLIENT_ID, CLIENT_SECRET, REDIRECT_URI,
    TOKEN_FILE, TOKEN_URL
)

token_bp = Blueprint('token', __name__)

class TokenManager:
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
        return Path(DATA_PATH) / folder / 'access_token.json'  # Corrigido aqui

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
        credentials = f"{CLIENT_ID}:{CLIENT_SECRET}"
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
        if not token_info:
            return None, "No token found"
        
        try:
            response = requests.post(
                TOKEN_URL,
                headers={
                    "Authorization": self._get_basic_auth(),
                    "Content-Type": "application/x-www-form-urlencoded"
                },
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": token_info['refresh_token']
                }
            )
            response.raise_for_status()
            
            new_token_info = response.json()
            new_token_info['expires_at'] = (
                datetime.now() + timedelta(seconds=new_token_info['expires_in'])
            ).isoformat()
            
            if not self._write_token_file(new_token_info):
                return None, "Failed to save token"
            
            return new_token_info, None
        except requests.exceptions.RequestException as e:
            return None, str(e)

@token_bp.route('/refresh_token/<customer_id>')
def refresh_token_endpoint(customer_id):
    """Endpoint to refresh the access token."""
    manager = TokenManager(customer_id)
    
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
