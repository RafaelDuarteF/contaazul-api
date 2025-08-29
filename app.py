import os
from functools import wraps
from pathlib import Path
import json
from datetime import datetime

from flask import Flask, request, jsonify
from werkzeug.security import check_password_hash

from config import FLASK_SECRET_KEY
from auth import auth_bp
from etl import etl_bp
from token_manager import token_bp
from data import data_bp
from token_actions import token_actions_bp
from token_new_manager import token_new_bp

from mysql_token_store import get_all_tokens

app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY

# Register blueprints
app.register_blueprint(auth_bp)
app.register_blueprint(etl_bp)
app.register_blueprint(token_bp)
app.register_blueprint(data_bp)
app.register_blueprint(token_actions_bp)
app.register_blueprint(token_new_bp)

# Endpoint para gerar customers.json
@app.route('/generate-customers-json', methods=['GET'])
def generate_customers_json():
    tokens = get_all_tokens()
    customers = {}
    for t in tokens:
        customers[t['customer_id']] = t['customer_folder']
    customers_list = [
        {'customer_id': cid, 'customer_folder': folder}
        for cid, folder in customers.items()
    ]
    with open('customers.json', 'w', encoding='utf-8') as f:
        json.dump(customers_list, f, ensure_ascii=False, indent=2)
    return jsonify({'status': 'ok', 'count': len(customers_list)})

if __name__ == "__main__":
    # Only for local development
    app.run(port=8000, debug=True)
