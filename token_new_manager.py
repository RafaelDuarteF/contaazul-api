import base64
import json
from datetime import datetime, timedelta
from pathlib import Path
import requests
from flask import Blueprint, jsonify
from config import DATA_PATH
from config import (CLIENT_NEW_ID, CLIENT_NEW_SECRET, REDIRECT_NEW_URI, TOKEN_FILE, TOKEN_NEW_URL)
from mysql_token_store import get_token, upsert_token

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
        return Path(DATA_PATH) / folder / 'access_token_new.json'

    def _mirror_to_json(self, token_info):
        if not token_info:
            return
        token_file = self._get_token_file_path()
        if not token_file:
            return
        token_file.parent.mkdir(parents=True, exist_ok=True)
        with open(token_file, 'w', encoding='utf-8') as f:
            json.dump(token_info, f, ensure_ascii=False, indent=2)

    def _read_token_record(self):
        rec = get_token(str(self.customer_id), 'new')
        if not rec:
            return None
        token_info = {
            "access_token": rec["access_token"],
            "refresh_token": rec["refresh_token"],
            "expires_at": rec["expires_at"],
            "token_type": "bearer"
        }
        self._mirror_to_json(token_info)
        return token_info

    def _write_token_record(self, token_info):
        folder = self._get_customer_folder()
        if not folder:
            return False
        upsert_token(
            customer_id=str(self.customer_id),
            customer_folder=folder,
            type_token='new',
            access_token=token_info.get('access_token'),
            refresh_token=token_info.get('refresh_token'),
            expires_at_iso=token_info.get('expires_at')
        )
        self._mirror_to_json(token_info)
        return True

    def _get_basic_auth(self):
        credentials = f"{CLIENT_NEW_ID}:{CLIENT_NEW_SECRET}"
        return "Basic " + base64.b64encode(credentials.encode()).decode('utf-8')

    def is_token_expired(self):
        token_info = self._read_token_record()
        if not token_info:
            return True
        expiry_time = datetime.fromisoformat(token_info['expires_at'])
        return datetime.now() > (expiry_time - timedelta(minutes=5))

    def refresh_token(self):
        token_info = self._read_token_record()
        if not token_info or 'refresh_token' not in token_info:
            return "No refresh token found in storage"
        try:
            headers = {
                "Authorization": self._get_basic_auth(),
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json"
            }
            payload = {
                "grant_type": "refresh_token",
                "refresh_token": token_info['refresh_token'],
                "client_id": CLIENT_NEW_ID,
                "client_secret": CLIENT_NEW_SECRET,
            }
            data = "&".join([f"{k}={v}" for k, v in payload.items()])
            response = requests.post(
                "https://auth.contaazul.com/oauth2/token",
                headers=headers,
                data=data,
                timeout=30
            )
            if response.status_code != 200:
                error_detail = response.json().get('error_description', response.text)
                return f"API Error {response.status_code}: {error_detail}"
            new_token_info = response.json()
            if 'refresh_token' not in new_token_info:
                new_token_info['refresh_token'] = token_info['refresh_token']
            new_token_info['expires_at'] = (
                datetime.now() + timedelta(seconds=new_token_info['expires_in'])
            ).isoformat()
            if not self._write_token_record(new_token_info):
                return "Failed to save new token"
        except requests.exceptions.RequestException as e:
            return f"Request Failed: {str(e)}"
        except Exception as e:
            return f"Unexpected Error: {str(e)}"

@token_new_bp.route('/refresh_token-new/<customer_id>')
def refresh_token_endpoint(customer_id):
    manager = TokenNewManager(customer_id)
    if not manager.is_token_expired():
        return jsonify({"message": "Token is still valid"})
    error = manager.refresh_token()
    if error:
        return jsonify({"error": "Failed to refresh token", "details": error}), 500
    return jsonify({"message": "Token refreshed successfully"}), 200
