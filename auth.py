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
    AUTH_URL, TOKEN_URL, API_USERNAME, API_PASSWORD,
    CLIENT_NEW_ID, CLIENT_NEW_SECRET, REDIRECT_NEW_URI,
    AUTH_NEW_URL, TOKEN_NEW_URL
)

auth_bp = Blueprint('auth', __name__)
state_store = {}

# Old API routes
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
    
    return f'<h1>ContaAzul Integration</h1><p><a href="{auth_url}">Click here to authorize (Old API)</a></p>' + \
           f'<p><a href="/oauth-new">Or click here for New API authorization</a></p>'

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

# New API routes
@auth_bp.route("/oauth-new")
def home_new():
    state = secrets.token_urlsafe(16)
    state_store[state] = True
    
    auth_url = (
        f"{AUTH_NEW_URL}?response_type=code"
        f"&client_id={CLIENT_NEW_ID}"
        f"&redirect_uri={REDIRECT_NEW_URI}"
        f"&state={state}"
        f"&scope=openid+profile+aws.cognito.signin.user.admin"
    )
    
    return f'<h1>ContaAzul New API Integration</h1><p><a href="{auth_url}">Click here to authorize (New API)</a></p>'

@auth_bp.route("/callback-new")
def callback_new():
    state = request.args.get("state")
    auth_code = request.args.get("code")
    error = request.args.get("error")
    error_description = request.args.get("error_description")

    if error:
        return jsonify({
            "error": error,
            "error_description": error_description,
            "message": "Authentication failed. Please check your credentials and try again."
        }), 401

    if not state:
        return jsonify({"error": "Invalid state"}), 401

    try:
        # Prepare basic auth header
        auth_string = f"{CLIENT_NEW_ID}:{CLIENT_NEW_SECRET}"
        basic_auth = base64.b64encode(auth_string.encode()).decode()
        
        response = requests.post(
            TOKEN_NEW_URL,
            headers={
                "Authorization": f"Basic {basic_auth}",
                "Content-Type": "application/x-www-form-urlencoded"
            },
            data={
                "grant_type": "authorization_code",
                "code": auth_code,
                "redirect_uri": REDIRECT_NEW_URI
            }
        )
        
        response.raise_for_status()
        token_info = response.json()
        
        # Add expiration time
        token_info['expires_at'] = (
            datetime.now() + timedelta(seconds=token_info['expires_in'])
        ).isoformat()

        return jsonify(token_info)
    
    except requests.exceptions.HTTPError as e:
        return jsonify({
            "error": "http_error",
            "message": f"HTTP error occurred: {str(e)}"
        }), 500
    except requests.exceptions.RequestException as e:
        return jsonify({
            "error": "request_error",
            "message": f"Error obtaining token: {str(e)}"
        }), 500
    except Exception as e:
        return jsonify({
            "error": "unknown_error",
            "message": f"An unexpected error occurred: {str(e)}"
        }), 500

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