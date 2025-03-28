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
    try:
        # Get customer-specific folder
        with open('customers.json', 'r') as f:
            customers = json.load(f)
        
        customer_folder = None
        for user in customers['users']:
            if user['id'] == customer_id:
                customer_folder = DATA_PATH / user['folder']
                break
        
        if not customer_folder:
            return jsonify({
                "error": f"Customer {customer_id} not found"
            }), 404
        
        # List all files of the specified type
        files = list(Path(customer_folder).glob(f"{data_type}_data.json"))
        
        if not files:
            return jsonify({
                "error": f"No {data_type} data files found"
            }), 404

        # Get the most recent file
        latest_file = max(files, key=lambda x: x.stat().st_mtime)
        
        with open(latest_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        return jsonify(data)

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
        files = list(Path(customer_folder).glob("*.json"))
        
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
