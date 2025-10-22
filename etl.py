import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Dict, List, Optional
import time
import requests
from flask import Blueprint, jsonify, request
from tqdm import tqdm
from flask import current_app
from mysql_token_store import _connect
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from mysql_token_store import get_token

etl_bp = Blueprint('etl', __name__)

# BigQuery client setup
def get_bigquery_client():
    """Initialize BigQuery client with credentials from first row in MySQL credencial_google"""
    try:
        conn = _connect()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT credencial FROM credencial_google LIMIT 1")
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if row and row.get("credencial"):
            credentials_info = json.loads(row["credencial"])
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            return bigquery.Client(credentials=credentials)
        print("No credentials found in MySQL credencial_google table.")
        return None
    except Exception as e:
        print(f"Error initializing BigQuery client: {e}")
        return None

# BigQuery table names (agora cada cliente tem seu próprio dataset)
BQ_TABLES = {
    "categories": "categories",
    "accounts_receivable": "accounts_receivable", 
    "accounts_payable": "accounts_payable",
    "parcelas": "parcelas",
    "parcelas_baixas": "parcelas_baixas",
    "sales": "sales",
    "financial_accounts": "financial_accounts",
    "sync_logs": "sync_logs"
}

class BigQueryStorage:
    def __init__(self, customer_id):
        self.customer_id = customer_id
        self.client = get_bigquery_client()
        self.dataset = f"contaazul_{customer_id.replace('-', '_')}"
        self.location = "US"  # Força região US
        self.timezone = ZoneInfo("America/Sao_Paulo")
        
    def _ensure_dataset_exists(self):
        """Cria o dataset se não existir"""
        try:
            dataset_ref = self.client.dataset(self.dataset)
            
            # Verifica se o dataset já existe
            try:
                existing_dataset = self.client.get_dataset(dataset_ref)
                # Se existe mas em região diferente, mudamos
                if existing_dataset.location != self.location:
                    print(f"⚠️  Dataset existe em região diferente: {existing_dataset.location}")
                    # Muda a região do self.location para a do dataset existente
                    self.location = existing_dataset.location
                return True
            except Exception:
                # Dataset não existe, cria novo
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "US"  # Força região US
                self.client.create_dataset(dataset)
                print(f"✅ Dataset criado: {self.dataset} em {self.location}")
                return True
                
        except Exception as e:
            print(f"❌ Error creating dataset {self.dataset}: {e}")
            return False
            
    def save_data(self, table_name: str, data: List[Dict], merge_key: str = None):
        """Save data to BigQuery table with merge/update capability"""
        if not data or not self._ensure_dataset_exists():
            return False
            
        try:
            df = pd.DataFrame(data)
            # Adiciona metadados com timezone de São Paulo (TIMESTAMP)
            loaded_at = datetime.now(self.timezone).replace(microsecond=0)
            if '_loaded_at' in df.columns:
                arr = pd.to_datetime(df['_loaded_at'], errors='coerce')
                arr = arr.dt.tz_localize(None) if hasattr(arr.dt, 'tz_localize') else arr
                df['_loaded_at'] = arr.apply(lambda x: x.isoformat() if pd.notnull(x) else None)
            else:
                df['_loaded_at'] = pd.Series([loaded_at.replace(tzinfo=None).isoformat()] * len(df))

            # Convert nested structures to JSON strings for BigQuery where needed
            for col in df.columns:
                if col != '_loaded_at' and df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                    df[col] = df[col].apply(lambda x: json.dumps(x, default=str) if x is not None else None)
            
            table_ref = self._get_table_ref(table_name)
            
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
                autodetect=True
            )
            
            # Se merge_key é fornecido, faz DELETE + INSERT para updates
            if merge_key and not df.empty:
                # Primeiro verifica se a tabela existe antes de tentar deletar
                try:
                    self.client.get_table(table_ref)
                    # Tabela existe, pode fazer DELETE
                    unique_keys = df[merge_key].dropna().unique().tolist()
                    
                    if unique_keys:
                        delete_query = f"""
                        DELETE FROM `{table_ref}`
                        WHERE {merge_key} IN ({','.join([f"'{str(k)}'" for k in unique_keys if k])})
                        """
                        self.client.query(delete_query, location=self.location).result()
                except Exception:
                    # Tabela não existe, não faz DELETE (primeira execução)
                    print(f"📝 Tabela {table_name} não existe ainda, criando...")
            
            # Insert new data - isso cria a tabela se não existir
            job = self.client.load_table_from_dataframe(
                df, table_ref, job_config=job_config, location=self.location
            )
            job.result()
            
            print(f"✅ Dados salvos em {table_name}: {len(data)} registros")
            return True
            
        except Exception as e:
            print(f"❌ Error saving to BigQuery table {table_name}: {e}")
            return False
    
    def load_data(self, table_name: str, filters: Dict = None) -> pd.DataFrame:
        """Load data from BigQuery table"""
        try:
            if not self._ensure_dataset_exists():
                return pd.DataFrame()
                
            table_ref = self._get_table_ref(table_name)
            
            # Primeiro verifica se a tabela existe
            try:
                self.client.get_table(table_ref)
            except Exception:
                # Tabela não existe, retorna DataFrame vazio
                return pd.DataFrame()
            
            query = f"SELECT * FROM `{table_ref}`"
            
            if filters:
                where_conditions = []
                for key, value in filters.items():
                    where_conditions.append(f"{key} = '{value}'")
                if where_conditions:
                    query += " WHERE " + " AND ".join(where_conditions)
            
            return self.client.query(query, location=self.location).to_dataframe()
            
        except Exception as e:
            print(f"❌ Error loading from BigQuery table {table_name}: {e}")
            return pd.DataFrame()
        
    def _get_table_ref(self, table_name):
        return f"{self.client.project}.{self.dataset}.{table_name}"

    
    def get_last_sync(self, table_name: str) -> Optional[datetime]:
        """Get last sync timestamp for a table"""
        try:
            if not self._ensure_dataset_exists():
                return None
                
            sync_table = self._get_table_ref(BQ_TABLES["sync_logs"])
            query = f"""
            SELECT MAX(last_sync) as last_sync 
            FROM `{sync_table}` 
            WHERE table_name = '{table_name}'
            """
            result = self.client.query(query).result()
            for row in result:
                return row.last_sync
            return None
        except Exception as e:
            print(f"Error getting last sync: {e}")
            return None
    
    def save_sync_log(self, table_name: str, record_count: int):
        """Save sync log entry"""
        try:
            if not self._ensure_dataset_exists():
                return False
                
            dataset_ref = self.client.dataset(self.dataset)
            table_id = BQ_TABLES["sync_logs"]
            table_ref = dataset_ref.table(table_id)

            # Ensure sync_logs table exists with expected schema
            try:
                self.client.get_table(table_ref)
            except Exception:
                schema = [
                    bigquery.SchemaField("table_name", "STRING"),
                    bigquery.SchemaField("last_sync", "TIMESTAMP"),
                    bigquery.SchemaField("record_count", "INT64"),
                    bigquery.SchemaField("customer_id", "STRING"),
                ]
                table = bigquery.Table(table_ref, schema=schema)
                table.time_partitioning = bigquery.TimePartitioning(field="last_sync")
                table.clustering_fields = ["table_name", "customer_id"]
                self.client.create_table(table)

            sync_table = self._get_table_ref(BQ_TABLES["sync_logs"])
            now_ts = datetime.now(self.timezone).replace(microsecond=0).isoformat()
            rows_to_insert = [{
                "table_name": table_name,
                "last_sync": now_ts,
                "record_count": record_count,
                "customer_id": self.customer_id
            }]
            
            errors = self.client.insert_rows_json(sync_table, rows_to_insert)
            return len(errors) == 0
        except Exception as e:
            print(f"Error saving sync log: {e}")
            return False

class BaseETL:
    def __init__(self, customer_id, endpoint):
        self.customer_id = customer_id
        self.base_url = "https://api-v2.contaazul.com/v1"
        self.bq_storage = BigQueryStorage(customer_id)
        self.endpoint = endpoint
        self.timezone = self.bq_storage.timezone

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

    def _normalize_last_sync(self, last_sync: Optional[datetime]) -> Optional[datetime]:
        if not last_sync:
            return None
        if last_sync.tzinfo is None:
            last_sync = last_sync.replace(tzinfo=timezone.utc)
        return last_sync.astimezone(self.timezone)

    def _parse_datetime_value(self, value) -> Optional[datetime]:
        if not value:
            return None

        candidate = value
        if isinstance(candidate, datetime):
            dt_value = candidate
        else:
            text = str(candidate).strip()
            if not text:
                return None
            if text.endswith('Z'):
                text = text[:-1] + '+00:00'
            try:
                dt_value = datetime.fromisoformat(text)
            except ValueError:
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                    try:
                        dt_value = datetime.strptime(text, fmt)
                        break
                    except ValueError:
                        dt_value = None
                if dt_value is None:
                    return None

        if dt_value.tzinfo is None:
            dt_value = dt_value.replace(tzinfo=self.timezone)
        else:
            dt_value = dt_value.astimezone(self.timezone)
        return dt_value

    def _was_updated_after_sync(self, item: Dict, last_sync: Optional[datetime]) -> bool:
        if not last_sync:
            return True

        candidate_keys = [
            "data_atualizacao",
            "data_alteracao",
            "dataAtualizacao",
            "dataAlteracao",
            "updated_at",
            "ultima_atualizacao",
            "data_modificacao",
            "data_criacao",
            "dataCriacao",
            "created_at"
        ]

        for key in candidate_keys:
            value = item.get(key)
            dt_value = self._parse_datetime_value(value)
            if dt_value:
                return dt_value > last_sync

        # Se não achou nenhum timestamp válido, considera atualizado para evitar perder dados
        return True

    def _get_token(self) -> Optional[str]:
        try:
            # Try to get token from database
            row = get_token(self.customer_id, 'new')
            if row and row.get('access_token'):
                return row.get('access_token')
        except Exception as e:
            print(f"Error reading token from DB for {self.customer_id}: {e}")
        return None

    def _search_items(
        self,
        access_token: str,
        filters: Dict,
        page: int = 0,
        page_size: int = 100,
        ascending_field: Optional[str] = None,
        descending_field: Optional[str] = None,
        max_retries: int = 1
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
                    time.sleep(0.3)
                    continue
                elif e.response.status_code == 429:
                    print("Rate limit atingido, aguardando antes de tentar novamente")
                    time.sleep(5)
                    continue
                else:
                    break
            except requests.exceptions.RequestException as e:
                last_exception = e
                break
        
        print(f"Error searching items após {attempts} tentativas: {last_exception}")
        return None

    def _get_categories_data(self) -> List[Dict]:
        """Load categories from BigQuery"""
        df = self.bq_storage.load_data(BQ_TABLES["categories"])
        return df.to_dict('records') if not df.empty else []

    def _save_to_bigquery(self, table_name: str, items: List[Dict], merge_key: str = None):
        """Save items to BigQuery"""
        success = self.bq_storage.save_data(table_name, items, merge_key)
        if success:
            self.bq_storage.save_sync_log(table_name, len(items))
        return success

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
        due_date = account.get("data_vencimento", "")
        creation_date = account.get("data_criacao", "")
        update_date = account.get("data_alteracao", "")
        
        return {
            "id": account.get("id"),
            "descricao": account.get("descricao"),
            "data_vencimento": due_date[:10] if due_date else "",
            "status": account.get("status"),
            "total": account.get("total"),
            "nao_pago": account.get("nao_pago"),
            "pago": account.get("pago"),
            "data_criacao": creation_date[:10] if creation_date else "",
            "data_alteracao": update_date[:10] if update_date else "",
            "categoria_principal_id": account.get("categoria_principal_id"),
            "categoria_principal_nome": account.get("categoria_principal_nome")
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
            max_retries=1
        )

    def save_accounts_payable(self, accounts: List[Dict]):
        return self._save_to_bigquery(BQ_TABLES["accounts_payable"], accounts, "id")

class CategoriesETL(BaseETL):
    def __init__(self, customer_id):
        super().__init__(customer_id, "/categorias")

    def flatten_category(self, category: Dict) -> Dict:
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
            
            if isinstance(response_data, dict) and 'itens' in response_data:
                categories = response_data['itens']
            else:
                categories = response_data
                
            if not categories or not isinstance(categories, list):
                return None
                
            return [c for c in categories if isinstance(c, dict)]
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching categories: {e}")
            return None

    def save_categories(self, categories: List[Dict]):
        return self._save_to_bigquery(BQ_TABLES["categories"], categories, "id")

class AccountsReceivableETL(BaseETL):
    def __init__(self, customer_id):
        super().__init__(customer_id, "/financeiro/eventos-financeiros/contas-a-receber")

    def flatten_account_receivable(self, account: Dict) -> Dict:
        due_date = account.get("data_vencimento", "")
        creation_date = account.get("data_criacao", "")
        update_date = account.get("data_alteracao", "")
        
        return {
            "id": account.get("id"),
            "descricao": account.get("descricao"),
            "data_vencimento": due_date[:10] if due_date else "",
            "status": account.get("status"),
            "total": account.get("total"),
            "nao_pago": account.get("nao_pago"),
            "pago": account.get("pago"),
            "data_criacao": creation_date[:10] if creation_date else "",
            "data_alteracao": update_date[:10] if update_date else "",
            "categoria_principal_id": account.get("categoria_principal_id"),
            "categoria_principal_nome": account.get("categoria_principal_nome")
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
            max_retries=1
        )

    def save_accounts_receivable(self, accounts: List[Dict]):
        return self._save_to_bigquery(BQ_TABLES["accounts_receivable"], accounts, "id")

class SalesETL:
    def __init__(self, customer_id):
        self.customer_id = customer_id
        self.base_url = "https://api.contaazul.com/v1"
        self.bq_storage = BigQueryStorage(customer_id)

    def _get_headers(self, access_token: str) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }

    def _get_token(self) -> Optional[str]:
        try:
            row = get_token(self.customer_id, 'old')
            if row:
                return row.get('access_token')
        except Exception as e:
            print(f"Error reading token from DB for {self.customer_id}: {e}")
        return None

    def flatten_sale(self, sale: Dict) -> Dict:
        emission = sale.get("emission", "")
        first_installment_due_date = (
            sale.get("payment", {}).get("installments", [{}])[0].get("due_date", "")
            if sale.get("payment", {}).get("installments") else ""
        )
        
        financial_account = sale.get("payment", {}).get("financial_account") or {}
        
        return {
            "id": sale.get("id"),
            "number": sale.get("number"),
            "emission": emission[:10] if emission else "",
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
            "financial_account_id": financial_account.get("uuid"),
            "financial_account_name": financial_account.get("name"),
            "notes": sale.get("notes"),
            "shipping_cost": sale.get("shipping_cost"),
            "total": sale.get("total"),
            "seller_id": (sale.get("seller") or {}).get("id"),
            "seller_name": (sale.get("seller") or {}).get("name"),
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
        return self.bq_storage.save_data(BQ_TABLES["sales"], sales, "id")

# BigQuery endpoint para parcelas
@etl_bp.route('/parcelas/<customer_id>', methods=['GET'])
def get_event_installments(customer_id):
    """Endpoint para buscar parcelas dos eventos de contas a receber e a pagar."""
    bq_storage = BigQueryStorage(customer_id)
    etl = BaseETL(customer_id, "")

    # Busca último sync do BigQuery e converte para timezone de Brasília
    last_sync = etl._normalize_last_sync(
        bq_storage.get_last_sync(BQ_TABLES["parcelas"])
    )

    # Busca token
    access_token = etl._get_token()
    if not access_token:
        return jsonify({"error": "No access token found"}), 401

    # Carrega eventos existentes do BigQuery
    df_receivables = bq_storage.load_data(BQ_TABLES["accounts_receivable"])
    df_payables = bq_storage.load_data(BQ_TABLES["accounts_payable"])
    
    # Combina os DataFrames
    df_existente = pd.concat([df_receivables, df_payables], ignore_index=True)
    
    if df_existente.empty:
        return jsonify({"error": "No events found"}), 404

    if 'pago' not in df_existente.columns:
        df_existente['pago'] = 0

    # Filtra eventos com pago > 0
    df_eventos_com_pago = df_existente[
        df_existente['pago'].fillna(0).astype(float) > 0
    ].copy()
    
    if df_eventos_com_pago.empty:
        return jsonify({"error": "No events with pago > 0 found"}), 404

    # Aplica filtro incremental
    if last_sync:
        # Usa data_alteracao ou data_criacao para comparação
        df_eventos_com_pago['data_alteracao_dt'] = df_eventos_com_pago.apply(
            lambda row: etl._parse_datetime_value(
                row.get('data_alteracao') or row.get('data_criacao')
            ),
            axis=1
        )
        valid_mask = df_eventos_com_pago['data_alteracao_dt'].notna()
        if valid_mask.any():
            df_eventos_para_processar = df_eventos_com_pago[
                valid_mask & (df_eventos_com_pago['data_alteracao_dt'] > last_sync)
            ].copy()
        else:
            # Se não conseguimos interpretar nenhuma data, processa tudo para evitar perda
            df_eventos_para_processar = df_eventos_com_pago.copy()
        print(f"Processando {len(df_eventos_para_processar)} eventos modificados desde {last_sync}")
    else:
        df_eventos_para_processar = df_eventos_com_pago.copy()
        print(f"Primeira execução - processando {len(df_eventos_para_processar)} eventos")

    if df_eventos_para_processar.empty:
        total_parcelas = len(bq_storage.load_data(BQ_TABLES["parcelas"]))
        return jsonify({
            "message": "Nenhum evento modificado desde o último sync",
            "total_parcelas": total_parcelas,
            "last_sync": last_sync.isoformat() if last_sync else None
        })

    # Throttle: max 50 requests per minute com espaçamento constante de 1.2s
    request_interval = 60.0 / 50.0
    window_start = time.monotonic()
    requests_in_window = 0
    next_slot = window_start

    total_parcelas_salvas = 0
    eventos_processados = 0
    batch_records: List[Dict] = []
    batch_baixas_records: List[Dict] = []
    batch_event_ids: List[str] = []
    BATCH_EVENT_SIZE = 100

    def flush_batch():
        nonlocal batch_records, batch_baixas_records, batch_event_ids, total_parcelas_salvas
        # Save parcelas batch
        success = True
        if batch_records:
            success = bq_storage.save_data(BQ_TABLES["parcelas"], batch_records, "parent_evento_id")
            if success:
                total_parcelas_salvas += len(batch_records)
            else:
                print(f"❌ Falha ao salvar lote de {len(batch_records)} parcelas no BigQuery")

        # Save baixas batch (no sync log)
        if batch_baixas_records:
            ok_b = bq_storage.save_data(BQ_TABLES["parcelas_baixas"], batch_baixas_records, "parcela_id")
            if not ok_b:
                print(f"❌ Falha ao salvar lote de {len(batch_baixas_records)} baixas no BigQuery")

        # reset batches
        batch_records = []
        batch_baixas_records = []
        batch_event_ids = []
        return success
    if not bq_storage._ensure_dataset_exists():
        return jsonify({"error": "Failed to ensure BigQuery dataset"}), 500

    # Processa cada evento que precisa ser atualizado/buscado
    for _, evento in tqdm(df_eventos_para_processar.iterrows(), total=len(df_eventos_para_processar), desc="Buscando parcelas"):
        evento_id = evento['id']
        
        # Controle preciso de 50 requisições por minuto
        now = time.monotonic()
        if requests_in_window >= 50:
            elapsed_window = now - window_start
            if elapsed_window < 60:
                time.sleep(60 - elapsed_window)
            window_start = time.monotonic()
            requests_in_window = 0
            next_slot = window_start
            now = window_start

        if next_slot < now:
            next_slot = now

        slot_wait = next_slot - now
        if slot_wait > 0:
            time.sleep(slot_wait)
            now = time.monotonic()
            if next_slot < now:
                next_slot = now

        # Chama API de parcelas
        url = f"https://api-v2.contaazul.com/v1/financeiro/eventos-financeiros/parcelas/{evento_id}"
        max_attempts = 3
        attempt = 0
        local_token_attempts = 0
        parcela_data = None
        
        while attempt < max_attempts:
            try:
                resp = requests.get(url, headers=etl._get_headers(access_token), timeout=30)
                requests_in_window += 1
                next_slot += request_interval
                if resp.status_code == 429:
                    print(f"Rate limit 429 para evento {evento_id}, aguardando 30s antes de tentar novamente...")
                    time.sleep(30)
                    attempt += 1
                    continue
                if resp.status_code == 401:
                    if local_token_attempts < 2:
                        print(f"401 for evento {evento_id}, reloading token from DB and retrying...")
                        try:
                            row = get_token(customer_id, 'new')
                            if row and row.get('access_token'):
                                access_token = row.get('access_token')
                                local_token_attempts += 1
                                attempt += 1
                                time.sleep(1)
                                continue
                        except Exception as e:
                            print(f"Error reloading token from DB: {e}")
                resp.raise_for_status()
                parcela_data = resp.json()
                break
            except requests.exceptions.HTTPError as e:
                if resp.status_code == 429:
                    print(f"Rate limit 429 para evento {evento_id}, aguardando 30s antes de tentar novamente...")
                    time.sleep(30)
                    attempt += 1
                    continue
                print(f"Erro buscando parcelas do evento {evento_id}: {e}")
                break
            except Exception as e:
                print(f"Erro buscando parcelas do evento {evento_id}: {e}")
                break
        else:
            print(f"Falha ao buscar parcelas do evento {evento_id} após {max_attempts} tentativas.")
            continue

        if not parcela_data:
            continue

        # Prepara dados da parcela (pode retornar lista ou único objeto)
        if isinstance(parcela_data, dict) and parcela_data.get('parcelas'):
            parcelas_iter = parcela_data.get('parcelas') or []
        elif isinstance(parcela_data, list):
            parcelas_iter = parcela_data
        else:
            parcelas_iter = [parcela_data]

        parcelas_registros = []
        parcelas_baixas_registros = []
        for parcela in parcelas_iter:
            if not isinstance(parcela, dict):
                continue
            parcela_row = {
                "parcela_id": parcela.get('id'),
                "parcela_status": parcela.get('status'),
                "condicao_pagamento": parcela.get('condicao_pagamento'),
                "referencia": parcela.get('referencia'),
                "agendado": parcela.get('agendado'),
                "tipo_evento": parcela.get('tipo'),
                "rateio": parcela.get('rateio'),
                "conciliado": parcela.get('conciliado'),
                "valor_pago": parcela.get('valor_pago'),
                "perda": parcela.get('perda'),
                "nao_pago": parcela.get('nao_pago'),
                "data_vencimento": parcela.get('data_vencimento'),
                "data_pagamento_previsto": parcela.get('data_pagamento_previsto'),
                "descricao": parcela.get('descricao'),
                "id_conta_financeira": parcela.get('id_conta_financeira'),
                "metodo_pagamento": parcela.get('metodo_pagamento'),
                "parent_evento_id": evento_id,
            }
            parcelas_registros.append(parcela_row)

            # Extrai baixas individuais para tabela auxiliar parcelas_baixas
            baixas = parcela.get('baixas') if isinstance(parcela.get('baixas'), list) else []
            for baixa in baixas:
                if not isinstance(baixa, dict):
                    continue

                # parse valor_composicao (pode ser dict ou JSON string)
                vc = baixa.get('valor_composicao')
                vc_obj = None
                if isinstance(vc, dict):
                    vc_obj = vc
                elif isinstance(vc, str):
                    try:
                        vc_obj = json.loads(vc)
                    except Exception:
                        vc_obj = None

                baixa_multa = vc_obj.get('multa') if vc_obj and 'multa' in vc_obj else None
                baixa_juros = vc_obj.get('juros') if vc_obj and 'juros' in vc_obj else None
                baixa_valor_bruto = vc_obj.get('valor_bruto') if vc_obj and 'valor_bruto' in vc_obj else None
                baixa_desconto = vc_obj.get('desconto') if vc_obj and 'desconto' in vc_obj else None
                baixa_taxa = vc_obj.get('taxa') if vc_obj and 'taxa' in vc_obj else None
                baixa_valor_liquido = vc_obj.get('valor_liquido') if vc_obj and 'valor_liquido' in vc_obj else None

                loaded_at_str = datetime.now(etl.timezone).replace(microsecond=0).isoformat()

                baixa_row = {
                    "parcela_id": parcela.get('id'),
                    "baixa_id": baixa.get('id'),
                    "baixa_versao": baixa.get('versao'),
                    "baixa_data_pagamento": baixa.get('data_pagamento') or baixa.get('atualizado_em'),
                    "baixa_id_reconciliacao": baixa.get('id_reconciliacao'),
                    "baixa_id_parcela": baixa.get('id_parcela'),
                    "baixa_id_solicitacao_cobranca": baixa.get('id_solicitacao_cobranca'),
                    "baixa_observacao": baixa.get('observacao'),
                    "baixa_metodo_pagamento": baixa.get('metodo_pagamento'),
                    "baixa_origem": baixa.get('origem'),
                    "baixa_id_recibo_digital": baixa.get('id_recibo_digital'),
                    "baixa_tipo_evento_financeiro": baixa.get('tipo_evento_financeiro'),
                    "baixa_nsu": baixa.get('nsu'),
                    "baixa_id_referencia": baixa.get('id_referencia'),
                    "baixa_atualizado_em": baixa.get('atualizado_em'),
                    # explicit composition fields
                    "baixa_desconto": baixa_desconto,
                    "baixa_juros": baixa_juros,
                    "baixa_multa": baixa_multa,
                    "baixa_taxa": baixa_taxa,
                    "baixa_valor_bruto": baixa_valor_bruto,
                    "baixa_valor_liquido": baixa_valor_liquido,
                    # loaded timestamps
                    "_loaded_at": loaded_at_str,
                    "baixa_loaded_at": loaded_at_str,
                    "parcela_loaded_at": loaded_at_str,
                }
                parcelas_baixas_registros.append(baixa_row)

        if not parcelas_registros:
            continue

        eventos_processados += 1
        batch_records.extend(parcelas_registros)
        # collect baixas into the batch list (will be flushed together with parcelas)
        if parcelas_baixas_registros:
            batch_baixas_records.extend(parcelas_baixas_registros)
        batch_event_ids.append(evento_id)

        if len(batch_event_ids) >= BATCH_EVENT_SIZE:
            flush_batch()

    flush_batch()

    if total_parcelas_salvas > 0:
        bq_storage.save_sync_log(BQ_TABLES["parcelas"], total_parcelas_salvas)
        total_parcelas = len(bq_storage.load_data(BQ_TABLES["parcelas"]))
        return jsonify({
            "message": "Parcelas extraídas com sucesso",
            "parcelas_processadas": total_parcelas_salvas,
            "eventos_atualizados": eventos_processados,
            "total_parcelas": total_parcelas,
            "is_first_sync": last_sync is None,
            "last_sync": last_sync.isoformat() if last_sync else None
        })

    total_parcelas = len(bq_storage.load_data(BQ_TABLES["parcelas"]))
    return jsonify({
        "message": "Nenhuma parcela nova encontrada",
        "total_parcelas": total_parcelas,
        "last_sync": last_sync.isoformat() if last_sync else None
    })

# Outros endpoints convertidos para BigQuery
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

    success = etl.save_sales(all_sales)
    if success:
        return jsonify({
            "message": "Sales data extracted successfully",
            "total_sales": len(all_sales)
        })
    else:
        return jsonify({"error": "Error saving sales to BigQuery"}), 500

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
            "total_items": 0
        }), 200

    # Flatten and save all items
    categories = [etl.flatten_category(c) for c in all_categories]
    success = etl.save_categories(categories)
    
    if success:
        return jsonify({
            "message": "Categories data extracted successfully",
            "total_items": len(categories)
        })
    else:
        return jsonify({"error": "Error saving categories to BigQuery"}), 500

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

    last_sync = etl._normalize_last_sync(
        etl.bq_storage.get_last_sync(BQ_TABLES["accounts_receivable"])
    )

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
        "data_vencimento_de": "2010-01-01",
        "data_vencimento_ate": (datetime.now() + timedelta(days=5*365)).strftime("%Y-%m-%d")
    }
    page_size = 100  # Máximo permitido pela API
    retry_limit = 3
    delay_between_requests = 0.3  # 300ms entre requisições

    updated_items = []
    total_examined = 0

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
                        total_examined += 1
                        if etl._was_updated_after_sync(item, last_sync):
                            updated_items.append(item)
                    
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
    if not updated_items:
        etl.bq_storage.save_sync_log(BQ_TABLES["accounts_receivable"], 0)
        return jsonify({
            "message": "Nenhuma conta a receber atualizada desde o último sync",
            "total_items": 0,
            "last_sync": last_sync.isoformat() if last_sync else None,
            "examined": total_examined
        }), 200

    # Otimização: Usa dict comprehension para mapeamento mais rápido
    accounts_dict = {item['id']: item for item in updated_items}

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

    # 5. Salvamento de todos os registros de uma vez só
    if not accounts:
        return jsonify({"message": "No accounts to save", "total_saved": 0}), 200

    ok = etl.bq_storage.save_data(BQ_TABLES["accounts_receivable"], accounts, "id")
    total_saved = len(accounts) if ok else 0

    if total_saved > 0:
        etl.bq_storage.save_sync_log(BQ_TABLES["accounts_receivable"], total_saved)
        return jsonify({
            "message": "Accounts receivable with parent categories extracted successfully",
            "total_items": total_saved,
            "parent_categories_processed": len(parent_categories),
            "processed_since": last_sync.isoformat() if last_sync else None,
            "examined": total_examined
        })
    else:
        return jsonify({"error": "Error saving accounts receivable to BigQuery"}), 500

@etl_bp.route('/contas-a-pagar-com-categorias/<customer_id>', methods=['GET'])
def search_accounts_payable_with_parent_categories_optimized(customer_id):
    """Endpoint otimizado para buscar contas a pagar apenas de categorias pai."""
    etl = AccountsPayableETL(customer_id)
    access_token = etl._get_token()
    
    if not access_token:
        return jsonify({
            "error": "No access token found",
            "message": "Please authenticate first using /auth-new"
        }), 401

    last_sync = etl._normalize_last_sync(
        etl.bq_storage.get_last_sync(BQ_TABLES["accounts_payable"])
    )

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

    # 2. Configuração otimizada
    date_range = {
        "data_vencimento_de": "2010-01-01",
        "data_vencimento_ate": (datetime.now() + timedelta(days=5*365)).strftime("%Y-%m-%d")
    }
    page_size = 100
    retry_limit = 3
    delay_between_requests = 0.3

    updated_items = []
    total_examined = 0

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
                        total_examined += 1
                        if etl._was_updated_after_sync(item, last_sync):
                            updated_items.append(item)
                    
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
    if not updated_items:
        etl.bq_storage.save_sync_log(BQ_TABLES["accounts_payable"], 0)
        return jsonify({
            "message": "Nenhuma conta a pagar atualizada desde o último sync",
            "total_items": 0,
            "last_sync": last_sync.isoformat() if last_sync else None,
            "examined": total_examined
        }), 200

    accounts_dict = {item['id']: item for item in updated_items}

    accounts = []
    for item_id, item in accounts_dict.items():
        try:
            flat_account = etl.flatten_account_payable(item)
            flat_account.update({
                'categoria_principal_id': item['categoria_principal_id'],
                'categoria_principal_nome': item['categoria_principal_nome']
            })
            accounts.append(flat_account)
        except Exception as e:
            print(f"Error processing account {item_id}: {e}")

    # 5. Salvamento de todos os registros de uma vez só
    if not accounts:
        return jsonify({"message": "No accounts to save", "total_saved": 0}), 200

    ok = etl.bq_storage.save_data(BQ_TABLES["accounts_payable"], accounts, "id")
    total_saved = len(accounts) if ok else 0

    if total_saved > 0:
        etl.bq_storage.save_sync_log(BQ_TABLES["accounts_payable"], total_saved)
        return jsonify({
            "message": "Accounts payable with parent categories extracted successfully",
            "total_items": total_saved,
            "parent_categories_processed": len(parent_categories),
            "processed_since": last_sync.isoformat() if last_sync else None,
            "examined": total_examined
        })
    else:
        return jsonify({"error": "Error saving accounts payable to BigQuery"}), 500

class FinancialAccountsETL(BaseETL):
    def __init__(self, customer_id):
        super().__init__(customer_id, "/conta-financeira")
        self.max_retries = 5
        self.initial_delay = 1.0
        self.max_delay = 60.0
        self.backoff_factor = 2.0

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
            "total_recebido": 0,
            "total_a_receber": 0,
            "total_pago": 0,
            "total_a_pagar": 0,
            "saldo_atual": 0,
            "last_updated": datetime.now().isoformat()
        }

    def fetch_all_financial_accounts(self, access_token: str) -> Optional[List[Dict]]:
        """Fetch all financial accounts with retry logic"""
        attempts = 0
        last_exception = None
        
        while attempts < self.max_retries:
            try:
                params = {
                    "tamanho_pagina": 1000,
                    "apenas_ativo": True
                }
                
                response = requests.get(
                    f"{self.base_url}{self.endpoint}",
                    headers=self._get_headers(access_token),
                    params=params,
                    timeout=30
                )
                response.raise_for_status()
                
                accounts = response.json()
                accounts = accounts.get("itens", accounts)
                
                if not accounts or not isinstance(accounts, list):
                    return None
                    
                return [self.flatten_financial_account(acc) for acc in accounts]
                
            except requests.exceptions.HTTPError as e:
                last_exception = e
                if e.response.status_code == 429:
                    delay = min(self.initial_delay * (self.backoff_factor ** attempts), self.max_delay)
                    print(f"Rate limit hit, waiting {delay} seconds before retry (attempt {attempts + 1})")
                    time.sleep(delay)
                else:
                    print(f"HTTP error fetching accounts: {e}")
                    time.sleep(self.initial_delay)
                attempts += 1
            except requests.exceptions.RequestException as e:
                last_exception = e
                print(f"Request error fetching accounts: {e}")
                time.sleep(self.initial_delay)
                attempts += 1
        
        print(f"Failed to fetch financial accounts after {attempts} attempts: {last_exception}")
        return None

    def calculate_account_totals(self, access_token: str, accounts: List[Dict]) -> List[Dict]:
        """Calculate totals for each financial account with resilience"""
        date_range = {
            "data_vencimento_de": "2020-01-01",
            "data_vencimento_ate": (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d")
        }
        
        for i in tqdm(range(len(accounts)), desc="Calculating account totals"):
            account = accounts[i]
            account_id = account["id"]
            
            # Calculate for accounts receivable
            receivables = self._calculate_with_retry(
                access_token,
                account_id,
                "receivable",
                date_range
            )
            account["total_recebido"] = receivables.get("pago", 0)
            account["total_a_receber"] = receivables.get("nao_pago", 0)
            
            # Calculate for accounts payable
            payables = self._calculate_with_retry(
                access_token,
                account_id,
                "payable",
                date_range
            )
            account["total_pago"] = payables.get("pago", 0)
            account["total_a_pagar"] = payables.get("nao_pago", 0)
            
            # Calculate current balance
            account["saldo_atual"] = (
                account["total_recebido"] - account["total_pago"] +
                account["total_a_receber"] - account["total_a_pagar"]
            )
            
            # Small delay between accounts to avoid rate limiting
            time.sleep(0.5)
            
        return accounts

    def _calculate_with_retry(self, access_token: str, account_id: str, 
                            account_type: str, date_range: Dict) -> Dict:
        """Calculate totals with retry logic"""
        endpoint = "/financeiro/eventos-financeiros/contas-a-receber" if account_type == "receivable" \
                  else "/financeiro/eventos-financeiros/contas-a-pagar"
        
        totals = {"pago": 0, "nao_pago": 0}
        page = 1
        page_size = 400
        attempts = 0
        delay = self.initial_delay
        
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
                    },
                    timeout=30
                )
                
                if response.status_code == 429:
                    raise requests.exceptions.HTTPError("Rate limit exceeded", response=response)
                    
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
                attempts = 0
                delay = self.initial_delay
                time.sleep(0.3)
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    print(f"Rate limit hit for account {account_id}, waiting {delay} seconds")
                    time.sleep(delay)
                    delay = min(delay * self.backoff_factor, self.max_delay)
                    attempts += 1
                    
                    if attempts >= self.max_retries:
                        print(f"Max retries reached for account {account_id}")
                        break
                else:
                    print(f"HTTP error calculating {account_type} for account {account_id}: {e}")
                    break
            except requests.exceptions.RequestException as e:
                print(f"Request error calculating {account_type} for account {account_id}: {e}")
                break
                
        return totals

    def save_financial_accounts(self, accounts: List[Dict]):
        """Save financial accounts data to BigQuery."""
        return self._save_to_bigquery(BQ_TABLES["financial_accounts"], accounts, "id")

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
    success = etl.save_financial_accounts(accounts_with_totals)
    
    if success:
        return jsonify({
            "message": "Financial accounts data extracted successfully",
            "total_accounts": len(accounts_with_totals),
            "sample_account": accounts_with_totals[0] if accounts_with_totals else None
        })
    else:
        return jsonify({"error": "Error saving financial accounts to BigQuery"}), 500

@etl_bp.route('/contas-combinadas/<customer_id>', methods=['GET'])
def get_combined_accounts(customer_id):
    """Endpoint que combina contas a receber e a pagar com informações de categoria pai."""
    try:
        bq_storage = BigQueryStorage(customer_id)
        
        # Carrega contas a receber
        df_receivables = bq_storage.load_data(BQ_TABLES["accounts_receivable"])
        if df_receivables.empty:
            return jsonify({
                "error": "Accounts receivable data not found",
                "message": "Run /contas-a-receber-com-categorias first"
            }), 404
        
        # Carrega contas a pagar
        df_payables = bq_storage.load_data(BQ_TABLES["accounts_payable"])
        if df_payables.empty:
            return jsonify({
                "error": "Accounts payable data not found",
                "message": "Run /contas-a-pagar-com-categorias first"
            }), 404

        # Adiciona tipo
        df_receivables['tipo'] = 'R'
        df_payables['tipo'] = 'D'

        # Carrega categorias
        df_categories = bq_storage.load_data(BQ_TABLES["categories"])
        if df_categories.empty:
            return jsonify({
                "error": "Categories data not found",
                "message": "Run /categorias first"
            }), 404

        # Cria mapeamento de categorias
        category_map = {}
        for _, cat in df_categories.iterrows():
            category_map[cat['id']] = {
                'nome': cat.get('nome', ''),
                'categoria_pai': cat.get('categoria_pai')
            }

        # Processa as contas para adicionar informações de categoria pai
        def process_accounts(df):
            processed_accounts = []
            for _, account in df.iterrows():
                account_dict = account.to_dict()
                cat_id = account_dict.get('categoria_principal_id')
                
                if cat_id and cat_id in category_map:
                    category = category_map[cat_id]
                    if category['categoria_pai'] is None or pd.isna(category['categoria_pai']):
                        account_dict['categoria_pai_id'] = cat_id
                        account_dict['categoria_pai_nome'] = category['nome']
                    else:
                        parent_id = category['categoria_pai']
                        parent_category = category_map.get(parent_id, {})
                        account_dict['categoria_pai_id'] = parent_id
                        account_dict['categoria_pai_nome'] = parent_category.get('nome', '')
                
                processed_accounts.append(account_dict)
            return processed_accounts

        # Processa todas as contas
        processed_receivables = process_accounts(df_receivables)
        processed_payables = process_accounts(df_payables)

        # Combina as listas
        combined_accounts = processed_receivables + processed_payables

        # Salva no BigQuery
        success = bq_storage.save_data("combined_accounts", combined_accounts, "id")
        
        if success:
            return jsonify({
                "message": "Accounts combined successfully",
                "total_receivables": len(processed_receivables),
                "total_payables": len(processed_payables),
                "total_combined": len(combined_accounts)
            })
        else:
            return jsonify({"error": "Error saving combined accounts to BigQuery"}), 500

    except Exception as e:
        print(f"Error combining accounts: {str(e)}")
        return jsonify({
            "error": "Failed to combine accounts",
            "message": str(e)
        }), 500

@etl_bp.route('/sync-status/<customer_id>', methods=['GET'])
def get_sync_status(customer_id):
    """Endpoint para verificar status dos syncs"""
    try:
        bq_storage = BigQueryStorage(customer_id)
        
        # Carrega logs de sync
        df_sync_logs = bq_storage.load_data(BQ_TABLES["sync_logs"])
        
        if df_sync_logs.empty:
            return jsonify({
                "message": "No sync data found",
                "customer_id": customer_id
            })
        
        # Agrupa por tabela e pega o último sync
        latest_syncs = df_sync_logs.sort_values('last_sync').groupby('table_name').last().reset_index()
        
        status = {}
        for _, row in latest_syncs.iterrows():
            status[row['table_name']] = {
                'last_sync': row['last_sync'].isoformat() if hasattr(row['last_sync'], 'isoformat') else str(row['last_sync']),
                'record_count': int(row['record_count'])
            }
        
        return jsonify({
            "customer_id": customer_id,
            "sync_status": status
        })
        
    except Exception as e:
        print(f"Error getting sync status: {e}")
        return jsonify({
            "error": "Failed to get sync status",
            "message": str(e)
        }), 500

@etl_bp.route('/clean-customer-data/<customer_id>', methods=['DELETE'])
def clean_customer_data(customer_id):
    """Endpoint para limpar todos os dados de um cliente (para testes)"""
    try:
        bq_storage = BigQueryStorage(customer_id)
        client = bq_storage.client
        
        if not bq_storage._ensure_dataset_exists():
            return jsonify({
                "message": "No data found for customer",
                "customer_id": customer_id
            })
        
        dataset_ref = client.dataset(bq_storage.dataset)
        
        # Lista todas as tabelas no dataset
        tables = list(client.list_tables(dataset_ref))
        
        deleted_tables = []
        for table in tables:
            table_ref = dataset_ref.table(table.table_id)
            client.delete_table(table_ref)
            deleted_tables.append(table.table_id)
        
        return jsonify({
            "message": "Customer data cleaned successfully",
            "customer_id": customer_id,
            "deleted_tables": deleted_tables
        })
        
    except Exception as e:
        print(f"Error cleaning customer data: {e}")
        return jsonify({
            "error": "Failed to clean customer data",
            "message": str(e)
        }), 500
    
@etl_bp.route('/sincroniza-parcelas-faltantes/<customer_id>', methods=['GET'])
def sincroniza_parcelas_faltantes(customer_id):
    """Compara contas a pagar/receber com parcelas e sincroniza as faltantes."""
    bq_storage = BigQueryStorage(customer_id)
    etl = BaseETL(customer_id, "")

    # Carrega contas a pagar e a receber
    df_receivables = bq_storage.load_data(BQ_TABLES["accounts_receivable"])
    df_payables = bq_storage.load_data(BQ_TABLES["accounts_payable"])
    df_contas = pd.concat([df_receivables, df_payables], ignore_index=True)
    # Filtra apenas contas com pago > 0
    if 'pago' not in df_contas.columns:
        df_contas['pago'] = 0
    df_contas_pago = df_contas[df_contas['pago'].fillna(0).astype(float) > 0].copy()

    # Carrega parcelas
    df_parcelas = bq_storage.load_data(BQ_TABLES["parcelas"])

    # IDs de contas e de parcelas já associadas
    contas_ids = set(df_contas_pago["id"].dropna().astype(str))
    parcelas_event_ids = set(df_parcelas["parent_evento_id"].dropna().astype(str))

    # Contas sem parcela associada
    contas_sem_parcela = contas_ids - parcelas_event_ids

    if not contas_sem_parcela:
        return jsonify({"message": "Todas as contas possuem parcela associada."})

    access_token = etl._get_token()
    if not access_token:
        return jsonify({"error": "No access token found"}), 401

    total_criadas = 0
    total_baixas = 0
    erros = []
    for evento_id in contas_sem_parcela:
        url = f"https://api-v2.contaazul.com/v1/financeiro/eventos-financeiros/parcelas/{evento_id}"
        try:
            resp = requests.get(url, headers=etl._get_headers(access_token), timeout=30)
            resp.raise_for_status()
            parcela_data = resp.json()
        except Exception as e:
            erros.append(f"Erro ao buscar parcelas do evento {evento_id}: {e}")
            continue

        # Prepara dados da parcela
        if isinstance(parcela_data, dict) and parcela_data.get('parcelas'):
            parcelas_iter = parcela_data.get('parcelas') or []
        elif isinstance(parcela_data, list):
            parcelas_iter = parcela_data
        else:
            parcelas_iter = [parcela_data]

        parcelas_registros = []
        parcelas_baixas_registros = []
        for parcela in parcelas_iter:
            if not isinstance(parcela, dict):
                continue
            parcela_row = {
                "parcela_id": parcela.get('id'),
                "parcela_status": parcela.get('status'),
                "condicao_pagamento": parcela.get('condicao_pagamento'),
                "referencia": parcela.get('referencia'),
                "agendado": parcela.get('agendado'),
                "tipo_evento": parcela.get('tipo'),
                "rateio": parcela.get('rateio'),
                "conciliado": parcela.get('conciliado'),
                "valor_pago": parcela.get('valor_pago'),
                "perda": parcela.get('perda'),
                "nao_pago": parcela.get('nao_pago'),
                "data_vencimento": parcela.get('data_vencimento'),
                "data_pagamento_previsto": parcela.get('data_pagamento_previsto'),
                "descricao": parcela.get('descricao'),
                "id_conta_financeira": parcela.get('id_conta_financeira'),
                "metodo_pagamento": parcela.get('metodo_pagamento'),
                "parent_evento_id": evento_id,
            }
            parcelas_registros.append(parcela_row)

            baixas = parcela.get('baixas') if isinstance(parcela.get('baixas'), list) else []
            for baixa in baixas:
                if not isinstance(baixa, dict):
                    continue
                vc = baixa.get('valor_composicao')
                vc_obj = None
                if isinstance(vc, dict):
                    vc_obj = vc
                elif isinstance(vc, str):
                    try:
                        vc_obj = json.loads(vc)
                    except Exception:
                        vc_obj = None
                baixa_multa = vc_obj.get('multa') if vc_obj and 'multa' in vc_obj else None
                baixa_juros = vc_obj.get('juros') if vc_obj and 'juros' in vc_obj else None
                baixa_valor_bruto = vc_obj.get('valor_bruto') if vc_obj and 'valor_bruto' in vc_obj else None
                baixa_desconto = vc_obj.get('desconto') if vc_obj and 'desconto' in vc_obj else None
                baixa_taxa = vc_obj.get('taxa') if vc_obj and 'taxa' in vc_obj else None
                baixa_valor_liquido = vc_obj.get('valor_liquido') if vc_obj and 'valor_liquido' in vc_obj else None
                loaded_at_str = datetime.now(etl.timezone).replace(microsecond=0).isoformat()
                baixa_row = {
                    "parcela_id": parcela.get('id'),
                    "baixa_id": baixa.get('id'),
                    "baixa_versao": baixa.get('versao'),
                    "baixa_data_pagamento": baixa.get('data_pagamento') or baixa.get('atualizado_em'),
                    "baixa_id_reconciliacao": baixa.get('id_reconciliacao'),
                    "baixa_id_parcela": baixa.get('id_parcela'),
                    "baixa_id_solicitacao_cobranca": baixa.get('id_solicitacao_cobranca'),
                    "baixa_observacao": baixa.get('observacao'),
                    "baixa_metodo_pagamento": baixa.get('metodo_pagamento'),
                    "baixa_origem": baixa.get('origem'),
                    "baixa_id_recibo_digital": baixa.get('id_recibo_digital'),
                    "baixa_tipo_evento_financeiro": baixa.get('tipo_evento_financeiro'),
                    "baixa_nsu": baixa.get('nsu'),
                    "baixa_id_referencia": baixa.get('id_referencia'),
                    "baixa_atualizado_em": baixa.get('atualizado_em'),
                    "baixa_desconto": baixa_desconto,
                    "baixa_juros": baixa_juros,
                    "baixa_multa": baixa_multa,
                    "baixa_taxa": baixa_taxa,
                    "baixa_valor_bruto": baixa_valor_bruto,
                    "baixa_valor_liquido": baixa_valor_liquido,
                    "_loaded_at": loaded_at_str,
                    "baixa_loaded_at": loaded_at_str,
                    "parcela_loaded_at": loaded_at_str,
                }
                parcelas_baixas_registros.append(baixa_row)

        # Salva parcelas e baixas
        if parcelas_registros:
            ok_p = bq_storage.save_data(BQ_TABLES["parcelas"], parcelas_registros, "parent_evento_id")
            if ok_p:
                total_criadas += len(parcelas_registros)
        if parcelas_baixas_registros:
            ok_b = bq_storage.save_data(BQ_TABLES["parcelas_baixas"], parcelas_baixas_registros, "parcela_id")
            if ok_b:
                total_baixas += len(parcelas_baixas_registros)

    return jsonify({
        "contas_sem_parcela": len(contas_sem_parcela),
        "parcelas_criadas": total_criadas,
        "baixas_criadas": total_baixas,
        "erros": erros
    })