import os
from functools import wraps
from pathlib import Path
import json
from datetime import datetime

from flask import Flask, request, jsonify
from werkzeug.security import check_password_hash

from config import FLASK_SECRET_KEY, DATA_PATH, API_USERNAME, API_PASSWORD
from auth import auth_bp
from etl import etl_bp
from token_manager import token_bp

app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY

# Register blueprints
app.register_blueprint(auth_bp)
app.register_blueprint(etl_bp)
app.register_blueprint(token_bp)

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

@app.route('/read_data/<data_type>')
@require_auth
def read_data(data_type):
    """Read JSON data files with basic authentication."""
    try:
        # List all files of the specified type
        files = list(DATA_PATH.glob(f"{data_type}_data_*.json"))
        
        if not files:
            return jsonify({
                "error": f"No {data_type} data files found"
            }), 404

        # Get the most recent file
        latest_file = max(files, key=lambda x: x.stat().st_mtime)
        
        with open(latest_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        return jsonify({
            "message": f"Successfully read {data_type} data",
            "file": str(latest_file),
            "data": data
        })

    except Exception as e:
        return jsonify({
            "error": f"Error reading {data_type} data",
            "details": str(e)
        }), 500

@app.route('/list_data')
@require_auth
def list_data():
    """List all available data files with basic authentication."""
    try:
        # Get all JSON files in the data directory
        files = list(DATA_PATH.glob("*.json"))
        
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

if __name__ == "__main__":
    # Only for local development
    app.run(port=8000, debug=True)
