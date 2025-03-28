import base64
import json
from datetime import datetime, timedelta
from flask import jsonify
from functools import wraps
from pathlib import Path
import os
import requests
from flask import Blueprint, request
import secrets

from config import (
    CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, DATA_PATH,
    AUTH_URL, TOKEN_URL, API_USERNAME, API_PASSWORD
)

auth_bp = Blueprint('auth', __name__)
state_store = {}

@auth_bp.route("/oauth")
def home():
    state = secrets.token_urlsafe(16)
    state_store[state] = True
    
    auth_url = (
        f"{AUTH_URL}?response_type=code"
        f"&client_id={CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
        f"&state={state}"
    )
    
    return f'<h1>ContaAzul Integration</h1><p><a href="{auth_url}">Click here to authorize</a></p>'

@auth_bp.route("/callback")
def callback():
    state = request.args.get("state")
    auth_code = request.args.get("code")

    if not state:
        return jsonify({"error": "Invalid state"}), 401

    try:
        response = requests.post(
            TOKEN_URL,
            headers={
                "Authorization": f"Basic {base64.b64encode(f'{CLIENT_ID}:{CLIENT_SECRET}'.encode()).decode()}",
                "Content-Type": "application/x-www-form-urlencoded"
            },
            data={
                "grant_type": "authorization_code",
                "code": auth_code,
                "redirect_uri": REDIRECT_URI
            }
        )
        response.raise_for_status()

        token_info = response.json()
        token_info['expires_at'] = (
            datetime.now() + timedelta(seconds=token_info['expires_in'])
        ).isoformat()

        return jsonify(token_info)
    except requests.exceptions.RequestException as e:
        return f"Error obtaining token: {str(e)}", 500


def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated

def check_auth(username, password):
    """Check if a username/password combination is valid."""
    return username == API_USERNAME and password == API_PASSWORD

def authenticate():
    """Send a 401 response that enables basic auth."""
    return (
        jsonify({"error": "Authentication required"}),
        401,
        {'WWW-Authenticate': 'Basic realm="Login Required"'}
    )