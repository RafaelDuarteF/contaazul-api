import base64
import json
from datetime import datetime, timedelta

import requests
from flask import Blueprint, jsonify

from config import (
    CLIENT_ID, CLIENT_SECRET, TOKEN_FILE,
    TOKEN_URL
)

token_bp = Blueprint('token', __name__)

class TokenManager:
    def __init__(self):
        self.token_file = TOKEN_FILE

    def _get_basic_auth(self):
        """Get Basic Auth header for token requests."""
        credentials = f"{CLIENT_ID}:{CLIENT_SECRET}"
        encoded = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
        return f"Basic {encoded}"

    def _read_token_file(self):
        """Read the current token information."""
        try:
            with open(self.token_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error reading token file: {e}")
            return None

    def _save_token_info(self, token_info):
        """Save updated token information."""
        token_info["created_at"] = datetime.now().isoformat()
        with open(self.token_file, 'w', encoding='utf-8') as f:
            json.dump(token_info, f, ensure_ascii=False, indent=2)

    def refresh_token(self):
        """Refresh the access token using the refresh token."""
        current_token = self._read_token_file()
        if not current_token or "refresh_token" not in current_token:
            return None, "No refresh token found"

        headers = {
            "Authorization": self._get_basic_auth(),
            "Content-Type": "application/x-www-form-urlencoded"
        }

        data = {
            "grant_type": "refresh_token",
            "refresh_token": current_token["refresh_token"]
        }

        try:
            response = requests.post(TOKEN_URL, headers=headers, data=data)
            response.raise_for_status()
            
            new_token_data = response.json()
            token_info = {
                "access_token": new_token_data["access_token"],
                "token_type": new_token_data.get("token_type", "Bearer"),
                "expires_in": new_token_data.get("expires_in"),
                "refresh_token": new_token_data.get("refresh_token", current_token["refresh_token"]),
                "scope": new_token_data.get("scope")
            }
            
            self._save_token_info(token_info)
            return token_info, None

        except requests.exceptions.RequestException as e:
            return None, f"Error refreshing token: {str(e)}"

    def is_token_expired(self):
        """Check if the current token is expired or about to expire."""
        token_info = self._read_token_file()
        if not token_info or "created_at" not in token_info:
            return True

        created_at = datetime.fromisoformat(token_info["created_at"])
        expires_in = token_info.get("expires_in", 3600)  # Default 1 hour
        expiry_time = created_at + timedelta(seconds=expires_in)
        
        # Consider token as expired if it's within 5 minutes of expiration
        return datetime.now() > (expiry_time - timedelta(minutes=5))

@token_bp.route('/refresh_token')
def refresh_token_endpoint():
    """Endpoint to refresh the access token."""
    manager = TokenManager()
    
    if not manager.is_token_expired():
        return jsonify({
            "message": "Token is still valid",
            "token_info": manager._read_token_file()
        })

    token_info, error = manager.refresh_token()
    
    if error:
        return jsonify({
            "error": "Failed to refresh token",
            "details": error
        }), 500

    return jsonify({
        "message": "Token refreshed successfully",
        "token_info": token_info
    })
