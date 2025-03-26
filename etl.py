import json
from datetime import datetime
from typing import Dict, List, Optional

import requests
from flask import Blueprint, jsonify
from tqdm import tqdm

from config import DATA_PATH, TOKEN_FILE

etl_bp = Blueprint('etl', __name__)

class SalesETL:
    def __init__(self):
        self.base_url = "https://api.contaazul.com/v1"
        self.data_path = DATA_PATH
        self.token_file = TOKEN_FILE

    def _get_headers(self, access_token: str) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }

    def _get_token(self) -> Optional[str]:
        """Get access token from private JSON file."""
        try:
            if not self.token_file.exists():
                return None
                
            with open(self.token_file, 'r', encoding='utf-8') as f:
                token_data = json.load(f)
                return token_data.get("access_token")
        except Exception as e:
            print(f"Error reading token file: {e}")
            return None

    def flatten_sale(self, sale: Dict) -> Dict:
        """Transform nested sale object into flattened structure."""
        return {
            "id": sale.get("id"),
            "conta_azul_id": sale.get("contaAzulId"),
            "number": sale.get("number"),
            "emission": sale.get("emission"),
            "status": sale.get("status"),
            "scheduled": sale.get("scheduled"),
            "customer_id": sale.get("customer", {}).get("id"),
            "customer_name": sale.get("customer", {}).get("name"),
            "customer_company": sale.get("customer", {}).get("company_name"),
            "customer_email": sale.get("customer", {}).get("email"),
            "customer_type": sale.get("customer", {}).get("person_type"),
            "discount_type": sale.get("discount", {}) and sale["discount"].get("measure_unit"),
            "discount_rate": sale.get("discount", {}) and sale["discount"].get("rate"),
            "payment_type": sale.get("payment", {}).get("type"),
            "payment_method": sale.get("payment", {}).get("method"),
            "financial_account_id": sale.get("payment", {}).get("financial_account", {}).get("uuid"),
            "financial_account_name": sale.get("payment", {}).get("financial_account", {}).get("name"),
            "notes": sale.get("notes"),
            "shipping_cost": sale.get("shipping_cost"),
            "total": sale.get("total"),
            "seller_id": sale.get("seller", {}).get("id"),
            "seller_name": sale.get("seller", {}).get("name"),
            "installments_count": len(sale.get("payment", {}).get("installments", [])),
            "first_installment_value": (
                sale.get("payment", {}).get("installments", [{}])[0].get("value")
                if sale.get("payment", {}).get("installments") else None
            ),
            "first_installment_due_date": (
                sale.get("payment", {}).get("installments", [{}])[0].get("due_date")
                if sale.get("payment", {}).get("installments") else None
            )
        }

    def fetch_and_transform_sales(self, access_token: str, page: int = 0, size: int = 2000) -> Optional[List[Dict]]:
        """Fetch sales from ContaAzul API and transform them."""
        try:
            response = requests.get(
                f"{self.base_url}/sales",
                headers=self._get_headers(access_token),
                params={"page": page, "size": size}
            )
            response.raise_for_status()
            
            sales = response.json()
            if not sales:  # Empty page
                return None
                
            return [self.flatten_sale(sale) for sale in sales]
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching sales: {e}")
            return None

    def save_sales(self, sales: List[Dict]):
        """Save transformed sales data to JSON file. If file already exists, it will be overwritten."""
        filename = self.data_path / f"sales_data.json"

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(sales, f, ensure_ascii=False, indent=2)
        
        return filename

@etl_bp.route('/extract_sales')
def extract_sales():
    """Sales data extraction endpoint."""
    etl = SalesETL()
    access_token = etl._get_token()
    
    if not access_token:
        return jsonify({
            "error": "No access token found",
            "message": "Please authenticate first using /auth"
        }), 401

    all_sales = []
    page = 0
    
    with tqdm(desc="Fetching sales") as pbar:
        while True:
            sales_page = etl.fetch_and_transform_sales(access_token, page)
            if not sales_page:
                break
                
            all_sales.extend(sales_page)
            pbar.update(len(sales_page))
            page += 1

    if not all_sales:
        return jsonify({"error": "No sales data found"}), 404

    output_file = etl.save_sales(all_sales)
    return jsonify({
        "message": "Sales data extracted successfully",
        "total_sales": len(all_sales),
        "output_file": str(output_file)
    })
