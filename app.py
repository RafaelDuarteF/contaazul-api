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
    users = []
    seen = set()
    for t in tokens:
        cid = t['customer_id']
        folder = t['customer_folder']
        if cid not in seen:
            users.append({'id': cid, 'folder': folder})
            seen.add(cid)
    customers_json = {'users': users}
    with open('customers.json', 'w', encoding='utf-8') as f:
        json.dump(customers_json, f, ensure_ascii=False, indent=2)
    return jsonify({'status': 'ok', 'count': len(users)})



@app.route('/test-bigquery')
def test_bigquery():
    """Test BigQuery connection"""
    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account
        import json
        import os
        
        credentials_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
        if not credentials_json:
            return jsonify({"error": "Variável GOOGLE_APPLICATION_CREDENTIALS_JSON não encontrada"})
        
        try:
            credentials_info = json.loads(credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            client = bigquery.Client(credentials=credentials, project=credentials_info['project_id'])
        except json.JSONDecodeError as e:
            return jsonify({"error": "JSON inválido", "message": str(e)})
        
        # Testa listando datasets
        datasets = list(client.list_datasets())
        
        return jsonify({
            "status": "✅ Conexão com BigQuery estabelecida!",
            "project": credentials_info['project_id'],
            "datasets": [dataset.dataset_id for dataset in datasets]
        })
        
    except Exception as e:
        return jsonify({
            "error": "❌ Falha na conexão",
            "message": str(e)
        })

if __name__ == "__main__":
    # Only for local development
    app.run(port=8000, debug=True)
