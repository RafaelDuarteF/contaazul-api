import base64
import json
from datetime import datetime

import requests
from flask import Blueprint, request
import secrets

from config import (
    CLIENT_ID, CLIENT_SECRET, REDIRECT_URI,
    AUTH_URL, TOKEN_URL, TOKEN_FILE
)

auth_bp = Blueprint('auth', __name__)
state_store = {}

@auth_bp.route("/")
def home():
    """OAuth authorization initiation endpoint."""
    state = secrets.token_urlsafe(16)
    state_store[state] = True
    
    auth_url = (
        f"{AUTH_URL}?client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}"
        f"&scope=sales&state={state}"
    )
    return f'<h1>ContaAzul Integration</h1><p><a href="{auth_url}">Click here to authorize</a></p>'

@auth_bp.route("/callback")
def callback():
    """OAuth callback endpoint."""
    auth_code = request.args.get("code")
    state = request.args.get("state")

    if not state or state not in state_store:
        return "Error: Invalid state - possible CSRF attack.", 401
    
    del state_store[state]

    if not auth_code:
        return "Error: Authorization code not found.", 400

    credentials = f"{CLIENT_ID}:{CLIENT_SECRET}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    
    headers = {
        'Authorization': f'Basic {encoded_credentials}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    data = {
        'grant_type': 'authorization_code',
        'code': auth_code,
        'redirect_uri': REDIRECT_URI
    }

    try:
        response = requests.post(TOKEN_URL, headers=headers, data=data)
        response.raise_for_status()
        token_data = response.json()

        token_info = {
            "access_token": token_data["access_token"],
            "token_type": token_data.get("token_type", "Bearer"),
            "expires_in": token_data.get("expires_in"),
            "refresh_token": token_data.get("refresh_token"),
            "scope": token_data.get("scope"),
            "created_at": datetime.now().isoformat()
        }

        with open(TOKEN_FILE, 'w', encoding='utf-8') as f:
            json.dump(token_info, f, ensure_ascii=False, indent=2)

        return "Token obtained and saved successfully! You can close this window."

    except requests.exceptions.RequestException as e:
        return f"Error obtaining token: {str(e)}", 500
