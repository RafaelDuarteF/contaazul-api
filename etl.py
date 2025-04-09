import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import requests
from flask import Blueprint, jsonify, request
from tqdm import tqdm

from config import DATA_PATH, TOKEN_FILE

etl_bp = Blueprint('etl', __name__)

class BaseETL:
    def __init__(self, customer_id, endpoint):
        self.customer_id = customer_id
        self.base_url = "https://api-v2.contaazul.com/v1"
        self.data_path = DATA_PATH
        self.token_file = TOKEN_FILE
        self.endpoint = endpoint

    def _get_customer_folder(self):
        with open('customers.json', 'r') as f:
            customers = json.load(f)
        for user in customers['users']:
            if user['id'] == self.customer_id:
                return user['folder']
        return None

    def _get_headers(self, access_token: str) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

    def _get_token(self) -> Optional[str]:
        folder = self._get_customer_folder()
        if not folder:
            return None
        
        token_file = Path(self.data_path) / folder / 'access_token_new.json'
        if not token_file.exists():
            return None
        
        with open(token_file, 'r', encoding='utf-8') as f:
            token_data = json.load(f)
            return token_data.get("access_token")

    def _search_items(
        self,
        access_token: str,
        filters: Dict,
        page: int = 0,
        page_size: int = 100,
        ascending_field: Optional[str] = None,
        descending_field: Optional[str] = None
    ) -> Optional[Dict]:
        """Common search method for both accounts payable and receivable."""
        try:
            params = {
                "pagina": page,
                "tamanho_pagina": page_size
            }
            
            if ascending_field:
                params["campo_ordenado_ascendente"] = ascending_field
            if descending_field:
                params["campo_ordenado_descendente"] = descending_field
            
            response = requests.post(
                f"{self.base_url}{self.endpoint}/buscar",
                headers=self._get_headers(access_token),
                params=params,
                json=filters
            )
            
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error searching items: {e}")
            return None

    def _save_items(self, items: List[Dict], filename: str):
        """Save items to a JSON file."""
        folder = self._get_customer_folder()
        if not folder:
            return None
        
        folder_path = Path(self.data_path) / folder
        folder_path.mkdir(parents=True, exist_ok=True)
        
        filepath = folder_path / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
        
        return filepath

    def _get_default_date_range(self) -> Dict[str, str]:
        """Get default date range for searches."""
        today = datetime.now()
        three_months_ago = today - timedelta(days=90)
        return {
            "data_vencimento_de": three_months_ago.strftime("%Y-%m-%d"),
            "data_vencimento_ate": today.strftime("%Y-%m-%d")
        }

    def _validate_date_range(self, date_range: Dict) -> Dict:
        """Validate and normalize date range."""
        # Get default range if not provided or if any date is missing
        if not date_range or \
           not date_range.get("data_vencimento_de") or \
           not date_range.get("data_vencimento_ate"):
            return self._get_default_date_range()
        
        try:
            # Parse and format start date
            start_date = date_range["data_vencimento_de"]
            if isinstance(start_date, str):
                date_range["data_vencimento_de"] = datetime.strptime(
                    start_date, "%Y-%m-%d"
                ).strftime("%Y-%m-%d")
            else:
                return self._get_default_date_range()
                
            # Parse and format end date
            end_date = date_range["data_vencimento_ate"]
            if isinstance(end_date, str):
                date_range["data_vencimento_ate"] = datetime.strptime(
                    end_date, "%Y-%m-%d"
                ).strftime("%Y-%m-%d")
            else:
                return self._get_default_date_range()
                
            # Ensure dates are not too far in the past
            min_date = datetime.now() - timedelta(days=365)
            if datetime.strptime(date_range["data_vencimento_de"], "%Y-%m-%d") < min_date:
                date_range["data_vencimento_de"] = min_date.strftime("%Y-%m-%d")
                
        except (ValueError, TypeError):
            return self._get_default_date_range()
        
        return date_range


class AccountsPayableETL(BaseETL):
    def __init__(self, customer_id):
        super().__init__(customer_id, "/financeiro/eventos-financeiros/contas-a-pagar")

    def flatten_account_payable(self, account: Dict) -> Dict:
        """Flatten the account payable data structure."""
        return {
            "id": account.get("id"),
            "descricao": account.get("descricao"),
            "data_vencimento": account.get("data_vencimento"),
            "status": account.get("status"),
            "total": account.get("total"),
            "nao_pago": account.get("nao_pago"),
            "pago": account.get("pago"),
            "data_criacao": account.get("data_criacao"),
            "data_alteracao": account.get("data_alteracao")
        }

    def search_accounts_payable(
        self,
        access_token: str,
        filters: Dict,
        page: int = 0,
        page_size: int = 100,
        ascending_field: Optional[str] = None,
        descending_field: Optional[str] = None
    ) -> Optional[Dict]:
        """Search accounts payable with filters."""
        return self._search_items(
            access_token,
            filters,
            page,
            page_size,
            ascending_field,
            descending_field
        )

    def save_accounts_payable(self, accounts: List[Dict]):
        """Save accounts payable data to a JSON file."""
        return self._save_items(accounts, "accounts_payable_data.json")


class AccountsReceivableETL(BaseETL):
    def __init__(self, customer_id):
        super().__init__(customer_id, "/financeiro/eventos-financeiros/contas-a-receber")

    def flatten_account_receivable(self, account: Dict) -> Dict:
        """Flatten the account receivable data structure."""
        return {
            "id": account.get("id"),
            "descricao": account.get("descricao"),
            "data_vencimento": account.get("data_vencimento"),
            "status": account.get("status"),
            "total": account.get("total"),
            "nao_pago": account.get("nao_pago"),
            "pago": account.get("pago"),
            "data_criacao": account.get("data_criacao"),
            "data_alteracao": account.get("data_alteracao")
        }

    def search_accounts_receivable(
        self,
        access_token: str,
        filters: Dict,
        page: int = 0,
        page_size: int = 100,
        ascending_field: Optional[str] = None,
        descending_field: Optional[str] = None
    ) -> Optional[Dict]:
        """Search accounts receivable with filters."""
        return self._search_items(
            access_token,
            filters,
            page,
            page_size,
            ascending_field,
            descending_field
        )

    def save_accounts_receivable(self, accounts: List[Dict]):
        """Save accounts receivable data to a JSON file."""
        return self._save_items(accounts, "accounts_receivable_data.json")


@etl_bp.route('/contas-a-pagar/<customer_id>', methods=['POST'])
def search_accounts_payable(customer_id):
    """Endpoint to search and save accounts payable."""
    etl = AccountsPayableETL(customer_id)
    access_token = etl._get_token()
    
    if not access_token:
        return jsonify({
            "error": "No access token found",
            "message": "Please authenticate first using /auth-new"
        }), 401

    # Get search parameters from request
    try:
        request_data = request.get_json()
        if not request_data:
            request_data = {}
    except Exception:
        request_data = {}

    # Use fixed date range
    today = datetime.now().strftime("%Y-%m-%d")
    date_range = {
        "data_vencimento_de": "2023-01-01",
        "data_vencimento_ate": today
    }

    # Initialize pagination parameters
    page = 1
    page_size = 400
    ascending_field = None
    descending_field = None
    all_items = []

    while True:
        # Search accounts payable
        result = etl.search_accounts_payable(
            access_token,
            {**date_range, **request_data},
            page,
            page_size,
            ascending_field,
            descending_field
        )
        
        if not result:
            return jsonify({
                "error": "Search failed",
                "message": "Failed to retrieve accounts payable data"
            }), 500

        # If no items found, return empty list
        if not result.get('itens'):
            if page == 1:  # First page empty
                return jsonify({
                    "message": "No accounts payable found matching the criteria",
                    "total_items": 0,
                    "data": []
                }), 200
            break  # No more items to fetch

        # Add items to our list
        all_items.extend(result['itens'])

        # Check if we need to fetch more pages
        if len(result['itens']) < page_size:
            break  # Last page

        page += 1

    # Flatten and save all items
    accounts = [etl.flatten_account_payable(account) for account in all_items]
    output_file = etl.save_accounts_payable(accounts)
    
    return jsonify({
        "message": "Accounts payable data extracted successfully",
        "total_items": len(accounts),
        "output_file": str(output_file),
        "data": accounts
    })

@etl_bp.route('/contas-a-receber/<customer_id>', methods=['POST'])
def search_accounts_receivable(customer_id):
    """Endpoint to search and save accounts receivable."""
    etl = AccountsReceivableETL(customer_id)
    access_token = etl._get_token()
    
    if not access_token:
        return jsonify({
            "error": "No access token found",
            "message": "Please authenticate first using /auth-new"
        }), 401

    # Get search parameters from request
    try:
        request_data = request.get_json()
        if not request_data:
            request_data = {}
    except Exception:
        request_data = {}

    # Use fixed date range
    today = datetime.now().strftime("%Y-%m-%d")
    date_range = {
        "data_vencimento_de": "2023-01-01",
        "data_vencimento_ate": today
    }

    # Initialize pagination parameters
    page = 1
    page_size = 100
    ascending_field = None
    descending_field = None
    all_items = []

    while True:
        # Search accounts receivable
        result = etl.search_accounts_receivable(
            access_token,
            {**date_range, **request_data},
            page,
            page_size,
            ascending_field,
            descending_field
        )
        
        if not result:
            return jsonify({
                "error": "Search failed",
                "message": "Failed to retrieve accounts receivable data"
            }), 500

        # If no items found, return empty list
        if not result.get('itens'):
            if page == 1:  # First page empty
                return jsonify({
                    "message": "No accounts receivable found matching the criteria",
                    "total_items": 0,
                    "data": []
                }), 200
            break  # No more items to fetch

        # Add items to our list
        all_items.extend(result['itens'])

        # Check if we need to fetch more pages
        if len(result['itens']) < page_size:
            break  # Last page

        page += 1

    # Flatten and save all items
    accounts = [etl.flatten_account_receivable(account) for account in all_items]
    output_file = etl.save_accounts_receivable(accounts)
    
    return jsonify({
        "message": "Accounts receivable data extracted successfully",
        "total_items": len(accounts),
        "output_file": str(output_file),
        "data": accounts
    })
