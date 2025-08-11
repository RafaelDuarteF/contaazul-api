import json
from pathlib import Path
from flask import Blueprint, jsonify
from auth import require_auth
from flask import request
from config import DATA_PATH
from mysql_token_store import get_all_tokens, upsert_token

token_actions_bp = Blueprint('token_actions_bp', __name__)

def _mirror_token_json(customer_folder, type_token, token_row):
    """Mirror a DB token to legacy JSON file."""
    if not token_row:
        return
    fname = 'access_token_new.json' if type_token == 'new' else 'access_token.json'
    folder_path = DATA_PATH / customer_folder
    folder_path.mkdir(parents=True, exist_ok=True)
    file_path = folder_path / fname
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump({
            'access_token': token_row.get('access_token'),
            'expires_at': token_row.get('expires_at'),
            'refresh_token': token_row.get('refresh_token'),
            'token_type': 'bearer'
        }, f, ensure_ascii=False, indent=4)

@token_actions_bp.route('/get-tokens')
@require_auth
def get_tokens():
    rows = get_all_tokens()
    # Mirror each to JSON for backward compatibility
    for r in rows:
        _mirror_token_json(r['customer_folder'], r['type_token'], r)
    # Return in same shape previously used
    access_tokens = []
    for r in rows:
        access_tokens.append({
            'customer_folder': r['customer_folder'],
            'access_token': r['access_token'],
            'expires_at': r['expires_at'],
            'refresh_token': r['refresh_token'],
            'type_token': r['type_token']
        })
    return jsonify(access_tokens)

@token_actions_bp.route('/insert-tokens', methods=['POST'])
def insert_tokens():
    data = request.get_json()
    customers = data.get('customers', [])
    customers_json_path = Path("customers.json")
    DATA_PATH.mkdir(parents=True, exist_ok=True)
    if not customers_json_path.exists():
        customers_data = {"users": []}
    else:
        with open(customers_json_path, 'r', encoding='utf-8') as f:
            customers_data = json.load(f)

    changed_customers = False

    for customer in customers:
        customer_folder = customer.get('customer_folder')
        customer_id = customer.get('customer_id') or customer_folder
        access_token = customer.get('access_token')
        expires_at = customer.get('expires_at')
        refresh_token = customer.get('refresh_token')
        type_token = customer.get('type_token', 'old')

        # Upsert DB
        upsert_token(
            customer_id=str(customer_id),
            customer_folder=customer_folder,
            type_token=type_token,
            access_token=access_token,
            refresh_token=refresh_token,
            expires_at_iso=expires_at
        )

        # Mirror JSON
        _mirror_token_json(customer_folder, type_token, {
            "access_token": access_token,
            "expires_at": expires_at,
            "refresh_token": refresh_token
        })

        if not any(user['folder'] == customer_folder for user in customers_data.get('users', [])):
            customers_data['users'].append({
                'id': customer_id,
                'folder': customer_folder
            })
            changed_customers = True

    if changed_customers:
        with open(customers_json_path, 'w', encoding='utf-8') as f:
            json.dump(customers_data, f, ensure_ascii=False, indent=4)

    return jsonify({"message": "Tokens inserted successfully"})