import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import time
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
        descending_field: Optional[str] = None,
        max_retries: int = 1  # Adicionando parâmetro para controle de retentativas
    ) -> Optional[Dict]:
        params = {
            "pagina": page,
            "tamanho_pagina": page_size
        }
        
        if ascending_field:
            params["campo_ordenado_ascendente"] = ascending_field
        if descending_field:
            params["campo_ordenado_descendente"] = descending_field
        
        attempts = 0
        last_exception = None
        
        while attempts <= max_retries:
            try:
                response = requests.post(
                    f"{self.base_url}{self.endpoint}/buscar",
                    headers=self._get_headers(access_token),
                    params=params,
                    json=filters
                )
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.HTTPError as e:
                last_exception = e
                if e.response.status_code == 400 and attempts < max_retries:
                    print(f"Erro 400 encontrado, tentando novamente (tentativa {attempts + 1})")
                    attempts += 1
                    time.sleep(0.3)  # Pequena pausa antes de tentar novamente
                    continue
                elif e.response.status_code == 429:
                    print("Rate limit atingido, aguardando antes de tentar novamente")
                    time.sleep(5)  # Pausa maior para rate limit
                    continue
                else:
                    break
            except requests.exceptions.RequestException as e:
                last_exception = e
                break
        
        print(f"Error searching items após {attempts} tentativas: {last_exception}")
        return None

    def _get_categories_data(self) -> List[Dict]:
        folder = self._get_customer_folder()
        if not folder:
            return []
            
        categories_file = Path(self.data_path) / folder / "categories_data.json"
        if not categories_file.exists():
            return []
            
        with open(categories_file, 'r', encoding='utf-8') as f:
            return json.load(f)

    def _save_items(self, items: List[Dict], filename: str):
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
        today = datetime.now()
        three_months_ago = today - timedelta(days=90)
        return {
            "data_vencimento_de": three_months_ago.strftime("%Y-%m-%d"),
            "data_vencimento_ate": today.strftime("%Y-%m-%d")
        }

    def _validate_date_range(self, date_range: Dict) -> Dict:
        if not date_range or \
           not date_range.get("data_vencimento_de") or \
           not date_range.get("data_vencimento_ate"):
            return self._get_default_date_range()
        
        try:
            start_date = date_range["data_vencimento_de"]
            if isinstance(start_date, str):
                date_range["data_vencimento_de"] = datetime.strptime(
                    start_date, "%Y-%m-%d"
                ).strftime("%Y-%m-%d")
            else:
                return self._get_default_date_range()
                
            end_date = date_range["data_vencimento_ate"]
            if isinstance(end_date, str):
                date_range["data_vencimento_ate"] = datetime.strptime(
                    end_date, "%Y-%m-%d"
                ).strftime("%Y-%m-%d")
            else:
                return self._get_default_date_range()
                
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
        # Process dates to ensure yyyy-mm-dd format
        due_date = account.get("data_vencimento", "")
        creation_date = account.get("data_criacao", "")
        update_date = account.get("data_alteracao", "")
        
        return {
            "id": account.get("id"),
            "descricao": account.get("descricao"),
            "data_vencimento": due_date[:10] if due_date else "",  # Takes only yyyy-mm-dd part
            "status": account.get("status"),
            "total": account.get("total"),
            "nao_pago": account.get("nao_pago"),
            "pago": account.get("pago"),
            "data_criacao": creation_date[:10] if creation_date else "",
            "data_alteracao": update_date[:10] if update_date else ""
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
        return self._search_items(
            access_token,
            filters,
            page,
            page_size,
            ascending_field,
            descending_field,
            max_retries=1  # Permitir 1 retentativa (total de 2 tentativas)
        )

    def save_accounts_payable(self, accounts: List[Dict]):
        """Save accounts payable data to a JSON file."""
        return self._save_items(accounts, "accounts_payable_data.json")

    

class CategoriesETL(BaseETL):
    def __init__(self, customer_id):
        super().__init__(customer_id, "/categorias")

    def flatten_category(self, category: Dict) -> Dict:
        """Flatten the category data structure."""
        if not isinstance(category, dict):
            return {"id": str(category)}
            
        return {
            "id": category.get("id"),
            "nome": category.get("nome"),
            "versao": category.get("versao"),
            "categoria_pai": category.get("categoria_pai"),
            "tipo": category.get("tipo"),
            "entrada_dre": category.get("entrada_dre"),
            "considera_custo_dre": category.get("considera_custo_dre")
        }

    def fetch_all_categories(self, access_token: str) -> Optional[List[Dict]]:
        """Fetch all categories in a single request"""
        try:
            params = {
                "tamanho_pagina": 1000,
                "apenas_filhos": "false"
            }
            
            response = requests.get(
                f"{self.base_url}{self.endpoint}",
                headers=self._get_headers(access_token),
                params=params
            )
            response.raise_for_status()
            
            response_data = response.json()
            
            # Verifica se as categorias estão dentro de uma propriedade 'itens'
            if isinstance(response_data, dict) and 'itens' in response_data:
                categories = response_data['itens']
            else:
                categories = response_data
                
            if not categories or not isinstance(categories, list):
                return None
                
            # Filtra apenas os itens que são dicionários (objetos de categoria)
            return [c for c in categories if isinstance(c, dict)]
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching categories: {e}")
            return None

    def save_categories(self, categories: List[Dict]):
        """Save categories data to a JSON file."""
        return self._save_items(categories, "categories_data.json")


class AccountsReceivableETL(BaseETL):
    def __init__(self, customer_id):
        super().__init__(customer_id, "/financeiro/eventos-financeiros/contas-a-receber")

    def flatten_account_receivable(self, account: Dict) -> Dict:
        """Flatten the account receivable data structure."""
        # Process dates to ensure yyyy-mm-dd format
        due_date = account.get("data_vencimento", "")
        creation_date = account.get("data_criacao", "")
        update_date = account.get("data_alteracao", "")
        
        return {
            "id": account.get("id"),
            "descricao": account.get("descricao"),
            "data_vencimento": due_date[:10] if due_date else "",  # Takes only yyyy-mm-dd part
            "status": account.get("status"),
            "total": account.get("total"),
            "nao_pago": account.get("nao_pago"),
            "pago": account.get("pago"),
            "data_criacao": creation_date[:10] if creation_date else "",
            "data_alteracao": update_date[:10] if update_date else ""
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
        return self._search_items(
            access_token,
            filters,
            page,
            page_size,
            ascending_field,
            descending_field,
            max_retries=1  # Permitir 1 retentativa (total de 2 tentativas)
        )

    def save_accounts_receivable(self, accounts: List[Dict]):
        """Save accounts receivable data to a JSON file."""
        return self._save_items(accounts, "accounts_receivable_data.json")


class SalesETL:
    def __init__(self, customer_id):
        self.customer_id = customer_id
        self.base_url = "https://api.contaazul.com/v1"
        self.data_path = DATA_PATH
        self.token_file = TOKEN_FILE

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
            "Accept": "application/json"
        }

    def _get_token(self) -> Optional[str]:
        folder = self._get_customer_folder()
        if not folder:
            return None
        
        token_file = Path(self.data_path) / folder / 'access_token.json'
        if not token_file.exists():
            return None
        
        with open(token_file, 'r', encoding='utf-8') as f:
            token_data = json.load(f)
            return token_data.get("access_token")

    def flatten_sale(self, sale: Dict) -> Dict:
        # Process emission date
        emission = sale.get("emission", "")
        # Process installment due date
        first_installment_due_date = (
            sale.get("payment", {}).get("installments", [{}])[0].get("due_date", "")
            if sale.get("payment", {}).get("installments") else ""
        )
        
        return {
            "id": sale.get("id"),
            "number": sale.get("number"),
            "emission": emission[:10] if emission else "",  # Takes only yyyy-mm-dd part
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
        }

    def fetch_and_transform_sales(self, access_token: str, page: int = 0, size: int = 2000) -> Optional[List[Dict]]:
        try:
            response = requests.get(
                f"{self.base_url}/sales",
                headers=self._get_headers(access_token),
                params={"page": page, "size": size}
            )
            response.raise_for_status()
            
            sales = response.json()
            if not sales:
                return None
            
            return [self.flatten_sale(sale) for sale in sales]
        except requests.exceptions.RequestException as e:
            print(f"Error fetching sales: {e}")
            return None

    def save_sales(self, sales: List[Dict]):
        folder = self._get_customer_folder()
        if not folder:
            return None
        
        folder_path = Path(self.data_path) / folder
        folder_path.mkdir(parents=True, exist_ok=True)
        
        filename = folder_path / "sales_data.json"
        
        if not filename.exists():
            filename.touch()
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(sales, f, ensure_ascii=False, indent=2)
        
        return filename
    
class FinancialAccountsETL(BaseETL):
    def __init__(self, customer_id):
        super().__init__(customer_id, "/conta-financeira")

    def flatten_financial_account(self, account: Dict) -> Dict:
        """Flatten the financial account data structure."""
        return {
            "id": account.get("id"),
            "banco": account.get("banco"),
            "codigo_banco": account.get("codigo_banco"),
            "nome": account.get("nome"),
            "ativo": account.get("ativo"),
            "tipo": account.get("tipo"),
            "conta_padrao": account.get("conta_padrao"),
            "possui_config_boleto_bancario": account.get("possui_config_boleto_bancario"),
            "agencia": account.get("agencia"),
            "numero": account.get("numero"),
            # These will be calculated later
            "total_recebido": 0,
            "total_a_receber": 0,
            "total_pago": 0,
            "total_a_pagar": 0,
            "saldo_atual": 0
        }

    def fetch_all_financial_accounts(self, access_token: str) -> Optional[List[Dict]]:
        """Fetch all financial accounts in a single request"""
        try:
            params = {
                "tamanho_pagina": 1000,
            }
            
            response = requests.get(
                f"{self.base_url}{self.endpoint}",
                headers=self._get_headers(access_token),
                params=params
            )
            response.raise_for_status()
            
            accounts = response.json()
            accounts = accounts.get("itens", accounts)  # Check if 'itens' key exists
            
            if not accounts or not isinstance(accounts, list):
                return None
                
            return [self.flatten_financial_account(acc) for acc in accounts]
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching financial accounts: {e}")
            return None

    def calculate_account_totals(self, access_token: str, accounts: List[Dict]) -> List[Dict]:
        """Calculate totals for each financial account by searching payable/receivable"""
        date_range = {
            "data_vencimento_de": "2020-01-01",  # Wide date range to get all transactions
            "data_vencimento_ate": (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d")
        }
        page_size = 400
        
        for account in tqdm(accounts, desc="Calculating account totals"):
            account_id = account["id"]
            
            # Calculate for accounts receivable
            receivables = self._calculate_for_account_type(
                access_token,
                account_id,
                "receivable",
                date_range,
                page_size
            )
            account["total_recebido"] = receivables.get("pago", 0)
            account["total_a_receber"] = receivables.get("nao_pago", 0)
            
            # Calculate for accounts payable
            payables = self._calculate_for_account_type(
                access_token,
                account_id,
                "payable",
                date_range,
                page_size
            )
            account["total_pago"] = payables.get("pago", 0)
            account["total_a_pagar"] = payables.get("nao_pago", 0)
            
            # Calculate current balance
            account["saldo_atual"] = (
                account["total_recebido"] - account["total_pago"] +
                account["total_a_receber"] - account["total_a_pagar"]
            )
            
        return accounts

    def _calculate_for_account_type(self, access_token: str, account_id: str, 
                                 account_type: str, date_range: Dict, page_size: int) -> Dict:
        """Helper method to calculate totals for a specific account type"""
        endpoint = "/financeiro/eventos-financeiros/contas-a-receber" if account_type == "receivable" \
                  else "/financeiro/eventos-financeiros/contas-a-pagar"
        
        totals = {"pago": 0, "nao_pago": 0}
        page = 1
        
        while True:
            try:
                response = requests.post(
                    f"{self.base_url}{endpoint}/buscar",
                    headers=self._get_headers(access_token),
                    params={
                        "pagina": page,
                        "tamanho_pagina": page_size
                    },
                    json={
                        **date_range,
                        "ids_contas_financeiras": [account_id]
                    }
                )
                response.raise_for_status()
                
                data = response.json()
                if not data or not data.get("itens"):
                    break
                
                for item in data["itens"]:
                    if item.get("pago"):
                        totals["pago"] += float(item["pago"])
                    if item.get("nao_pago"):
                        totals["nao_pago"] += float(item["nao_pago"])
                
                if len(data["itens"]) < page_size:
                    break
                    
                page += 1
                time.sleep(0.2)  # Small delay to avoid rate limiting
                
            except requests.exceptions.RequestException as e:
                print(f"Error calculating {account_type} totals for account {account_id}: {e}")
                break
                
        return totals

    def save_financial_accounts(self, accounts: List[Dict]):
        """Save financial accounts data to a JSON file."""
        return self._save_items(accounts, "financial_accounts_data.json")


@etl_bp.route('/extract_sales/<customer_id>')
def extract_sales(customer_id):
    """Sales data extraction endpoint."""
    etl = SalesETL(customer_id)
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

@etl_bp.route('/categorias/<customer_id>', methods=['GET'])
def get_all_categories(customer_id):
    """Endpoint to fetch and save all categories."""
    etl = CategoriesETL(customer_id)
    access_token = etl._get_token()
    
    if not access_token:
        return jsonify({
            "error": "No access token found",
            "message": "Please authenticate first using /auth-new"
        }), 401

    # Fetch all categories with progress bar
    with tqdm(desc="Fetching categories") as pbar:
        all_categories = etl.fetch_all_categories(access_token)
        if all_categories is None:
            return jsonify({
                "error": "Failed to fetch categories",
                "message": "See server logs for details"
            }), 500
        
        pbar.update(len(all_categories))

    if not all_categories:
        return jsonify({
            "message": "No categories found",
            "total_items": 0,
            "data": []
        }), 200

    # Flatten and save all items
    categories = [etl.flatten_category(c) for c in all_categories]
    output_file = etl.save_categories(categories)
    
    return jsonify({
        "message": "Categories data extracted successfully",
        "total_items": len(categories),
        "output_file": str(output_file),
    })

@etl_bp.route('/contas-a-receber-com-categorias/<customer_id>', methods=['GET'])
def search_accounts_receivable_with_parent_categories_optimized(customer_id):
    """Endpoint otimizado para buscar contas a receber apenas de categorias pai."""
    etl = AccountsReceivableETL(customer_id)
    access_token = etl._get_token()
    
    if not access_token:
        return jsonify({
            "error": "No access token found",
            "message": "Please authenticate first using /auth-new"
        }), 401

    # 1. Busca categorias pai de forma mais eficiente
    parent_categories = []
    try:
        categories_data = etl._get_categories_data()
        parent_categories = [
            {'id': cat['id'], 'nome': cat['nome']} 
            for cat in categories_data 
            if cat['tipo'] == 'RECEITA'
        ]

    except Exception as e:
        print(f"Error loading categories: {e}")
        return jsonify({
            "error": "Failed to load categories",
            "message": str(e)
        }), 500

    if not parent_categories:
        return jsonify({
            "error": "No parent categories found",
            "message": "Please fetch categories first using /categorias endpoint"
        }), 400

    # 2. Configuração otimizada
    date_range = {
        "data_vencimento_de": "2023-01-01",
        "data_vencimento_ate": (datetime.now() + timedelta(days=5*365)).strftime("%Y-%m-%d")
    }
    page_size = 100  # Máximo permitido pela API
    retry_limit = 3
    delay_between_requests = 0.3  # 300ms entre requisições

    all_items = []
    category_map = {cat['id']: cat['nome'] for cat in parent_categories}

    # 3. Busca paralela por categoria (com controle de rate limit)
    with tqdm(parent_categories, desc="Processing parent categories") as pbar:
        for category in pbar:
            category_id = category['id']
            pbar.set_postfix({'category': category['nome'][:15] + '...'})
            
            page = 1
            attempts = 0
            has_more = True
            
            while has_more and attempts < retry_limit:
                try:
                    time.sleep(delay_between_requests)
                    
                    result = etl.search_accounts_receivable(
                        access_token,
                        {**date_range, "ids_categorias": [category_id]},
                        page,
                        page_size
                    )
                    
                    if not result or not result.get('itens'):
                        has_more = False
                        continue
                    
                    # Adiciona informações de categoria diretamente
                    for item in result['itens']:
                        item['categoria_principal_id'] = category_id
                        item['categoria_principal_nome'] = category['nome']
                    
                    all_items.extend(result['itens'])
                    
                    # Verifica se há mais páginas
                    if len(result['itens']) < page_size:
                        has_more = False
                    else:
                        page += 1
                    
                    attempts = 0  # Reset attempts after success
                        
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 429:  # Too Many Requests
                        delay_between_requests *= 2  # Exponential backoff
                        print(f"Rate limit hit, increasing delay to {delay_between_requests}s")
                    attempts += 1
                    print(f"Attempt {attempts} failed for category {category_id}: {e}")
                except Exception as e:
                    print(f"Error processing category {category_id}: {e}")
                    has_more = False

    # 4. Processamento otimizado dos resultados
    if not all_items:
        return jsonify({
            "message": "No accounts receivable found for parent categories",
            "total_items": 0,
            "data": []
        }), 200

    # Otimização: Usa dict comprehension para mapeamento mais rápido
    accounts_dict = {item['id']: item for item in all_items}

    # Processamento em lote
    accounts = []
    for item_id, item in accounts_dict.items():
        try:
            flat_account = etl.flatten_account_receivable(item)
            flat_account.update({
                'categoria_principal_id': item['categoria_principal_id'],
                'categoria_principal_nome': item['categoria_principal_nome']
            })
            accounts.append(flat_account)
        except Exception as e:
            print(f"Error processing account {item_id}: {e}")

    # 5. Salvamento otimizado
    try:
        output_file = etl.save_accounts_receivable(accounts)
        return jsonify({
            "message": "Accounts receivable with parent categories extracted successfully",
            "total_items": len(accounts),
            "output_file": str(output_file),
            "parent_categories_processed": len(parent_categories),
            "performance_notes": {
                "initial_delay": f"{delay_between_requests}s between requests",
                "retry_limit": retry_limit,
                "max_page_size": page_size
            }
        })
    except Exception as e:
        print(f"Error saving results: {e}")
        return jsonify({
            "error": "Failed to save results",
            "message": str(e)
        }), 500
    

@etl_bp.route('/contas-a-pagar-com-categorias/<customer_id>', methods=['GET'])
def search_accounts_payable_with_parent_categories_optimized(customer_id):
    """Endpoint otimizado para buscar contas a pagar apenas de categorias pai."""
    etl = AccountsPayableETL(customer_id)  # Usando a classe AccountsPayableETL
    access_token = etl._get_token()
    
    if not access_token:
        return jsonify({
            "error": "No access token found",
            "message": "Please authenticate first using /auth-new"
        }), 401

    # 1. Busca categorias pai de DESPESA de forma mais eficiente
    parent_categories = []
    try:
        categories_data = etl._get_categories_data()
        parent_categories = [
            {'id': cat['id'], 'nome': cat['nome']} 
            for cat in categories_data 
            if cat['tipo'] == 'DESPESA'
        ]
    except Exception as e:
        print(f"Error loading categories: {e}")
        return jsonify({
            "error": "Failed to load categories",
            "message": str(e)
        }), 500

    if not parent_categories:
        return jsonify({
            "error": "No parent categories found for DESPESA",
            "message": "Please fetch categories first using /categorias endpoint"
        }), 400

    # 2. Configuração otimizada (igual ao anterior)
    date_range = {
        "data_vencimento_de": "2023-01-01",
        "data_vencimento_ate": (datetime.now() + timedelta(days=5*365)).strftime("%Y-%m-%d")
    }
    page_size = 100
    retry_limit = 3
    delay_between_requests = 0.3

    all_items = []
    category_map = {cat['id']: cat['nome'] for cat in parent_categories}

    # 3. Busca por categoria (com controle de rate limit)
    with tqdm(parent_categories, desc="Processing DESPESA parent categories") as pbar:
        for category in pbar:
            category_id = category['id']
            pbar.set_postfix({'category': category['nome'][:15] + '...'})
            
            page = 1
            attempts = 0
            has_more = True
            
            while has_more and attempts < retry_limit:
                try:
                    time.sleep(delay_between_requests)
                    
                    # Usa search_accounts_payable em vez de search_accounts_receivable
                    result = etl.search_accounts_payable(
                        access_token,
                        {**date_range, "ids_categorias": [category_id]},
                        page,
                        page_size
                    )
                    
                    if not result or not result.get('itens'):
                        has_more = False
                        continue
                    
                    for item in result['itens']:
                        item['categoria_principal_id'] = category_id
                        item['categoria_principal_nome'] = category['nome']
                    
                    all_items.extend(result['itens'])
                    
                    if len(result['itens']) < page_size:
                        has_more = False
                    else:
                        page += 1
                    
                    attempts = 0
                        
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 429:
                        delay_between_requests *= 2
                        print(f"Rate limit hit, increasing delay to {delay_between_requests}s")
                    attempts += 1
                    print(f"Attempt {attempts} failed for category {category_id}: {e}")
                except Exception as e:
                    print(f"Error processing category {category_id}: {e}")
                    has_more = False

    # 4. Processamento dos resultados
    if not all_items:
        return jsonify({
            "message": "No accounts payable found for parent categories",
            "total_items": 0,
            "data": []
        }), 200

    accounts_dict = {item['id']: item for item in all_items}

    accounts = []
    for item_id, item in accounts_dict.items():
        try:
            # Usa flatten_account_payable em vez de flatten_account_receivable
            flat_account = etl.flatten_account_payable(item)
            flat_account.update({
                'categoria_principal_id': item['categoria_principal_id'],
                'categoria_principal_nome': item['categoria_principal_nome']
            })
            accounts.append(flat_account)
        except Exception as e:
            print(f"Error processing account {item_id}: {e}")

    # 5. Salvamento
    try:
        output_file = etl.save_accounts_payable(accounts)  # Método específico para contas a pagar
        return jsonify({
            "message": "Accounts payable with parent categories extracted successfully",
            "total_items": len(accounts),
            "output_file": str(output_file),
            "parent_categories_processed": len(parent_categories),
            "performance_notes": {
                "initial_delay": f"{delay_between_requests}s between requests",
                "retry_limit": retry_limit,
                "max_page_size": page_size
            }
        })
    except Exception as e:
        print(f"Error saving results: {e}")
        return jsonify({
            "error": "Failed to save results",
            "message": str(e)
        }), 500


@etl_bp.route('/contas-combinadas/<customer_id>', methods=['GET'])
def get_combined_accounts(customer_id):
    """Endpoint que combina contas a receber e a pagar com informações de categoria pai."""
    try:
        # 1. Carrega os dados existentes
        folder = BaseETL(customer_id, "")._get_customer_folder()
        if not folder:
            return jsonify({
                "error": "Customer folder not found",
                "message": "Verify customer ID"
            }), 404

        data_path = Path(DATA_PATH) / folder
        
        # Carrega contas a receber
        receivables_file = data_path / "accounts_receivable_data.json"
        if not receivables_file.exists():
            return jsonify({
                "error": "Accounts receivable data not found",
                "message": "Run /contas-a-receber-com-categorias first"
            }), 404
        
        with open(receivables_file, 'r', encoding='utf-8') as f:
            receivables = json.load(f)
            for item in receivables:
                item['tipo'] = 'R'  # RECEITA

        # Carrega contas a pagar
        payables_file = data_path / "accounts_payable_data.json"
        if not payables_file.exists():
            return jsonify({
                "error": "Accounts payable data not found",
                "message": "Run /contas-a-pagar-com-categorias first"
            }), 404
        
        with open(payables_file, 'r', encoding='utf-8') as f:
            payables = json.load(f)
            for item in payables:
                item['tipo'] = 'D'  # DESPESA

        # 2. Carrega as categorias para mapeamento
        categories_file = data_path / "categories_data.json"
        if not categories_file.exists():
            return jsonify({
                "error": "Categories data not found",
                "message": "Run /categorias first"
            }), 404
        
        with open(categories_file, 'r', encoding='utf-8') as f:
            categories = json.load(f)

        # Cria mapeamento de categorias (id -> {dados})
        category_map = {cat['id']: cat for cat in categories}

        # 3. Processa as contas para adicionar informações de categoria pai
        def process_account(account):
            # Verifica se já tem categoria_principal_id (do endpoint anterior)
            cat_id = account.get('categoria_principal_id')
            if not cat_id:
                return account  # Se não tiver categoria, mantém como está

            # Se a categoria já for pai, usa ela mesma
            category = category_map.get(cat_id, {})
            if category.get('categoria_pai') is None:
                account['categoria_pai_id'] = cat_id
                account['categoria_pai_nome'] = category.get('nome', '')
            else:
                # Se não for pai, busca a categoria pai
                parent_id = category.get('categoria_pai')
                parent_category = category_map.get(parent_id, {})
                account['categoria_pai_id'] = parent_id
                account['categoria_pai_nome'] = parent_category.get('nome', '')
            
            return account

        # Processa todas as contas
        processed_receivables = [process_account(acc) for acc in receivables]
        processed_payables = [process_account(acc) for acc in payables]

        # 4. Combina as listas
        combined_accounts = processed_receivables + processed_payables

        # 5. Salva o resultado combinado
        output_file = data_path / "combined_accounts_data.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(combined_accounts, f, ensure_ascii=False, indent=2)

        return jsonify({
            "message": "Accounts combined successfully",
            "total_receivables": len(processed_receivables),
            "total_payables": len(processed_payables),
            "total_combined": len(combined_accounts),
            "output_file": str(output_file)
        })

    except Exception as e:
        print(f"Error combining accounts: {str(e)}")
        return jsonify({
            "error": "Failed to combine accounts",
            "message": str(e)
        }), 500


@etl_bp.route('/contas-financeiras/<customer_id>', methods=['GET'])
def extract_financial_accounts(customer_id):
    """Endpoint to fetch financial accounts with calculated totals."""
    etl = FinancialAccountsETL(customer_id)
    access_token = etl._get_token()
    
    if not access_token:
        return jsonify({
            "error": "No access token found",
            "message": "Please authenticate first using /auth-new"
        }), 401

    # Fetch all financial accounts
    accounts = etl.fetch_all_financial_accounts(access_token)
    if not accounts:
        return jsonify({
            "error": "No financial accounts found",
            "message": "Failed to fetch financial accounts"
        }), 404

    # Calculate totals for each account
    accounts_with_totals = etl.calculate_account_totals(access_token, accounts)
    
    # Save the results
    output_file = etl.save_financial_accounts(accounts_with_totals)
    
    return jsonify({
        "message": "Financial accounts data extracted successfully",
        "total_accounts": len(accounts_with_totals),
        "output_file": str(output_file),
        "sample_account": accounts_with_totals[0] if accounts_with_totals else None
    })