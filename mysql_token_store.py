import os
import mysql.connector
from mysql.connector import Error
from datetime import datetime

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_USER")

_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS tokens (
  id INT AUTO_INCREMENT PRIMARY KEY,
  customer_id VARCHAR(100) NOT NULL,
  customer_folder VARCHAR(255) NOT NULL,
  type_token ENUM('old','new') NOT NULL,
  access_token TEXT,
  refresh_token TEXT,
  expires_at DATETIME,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uniq_customer_type (customer_id, type_token)
) CHARACTER SET utf8mb4;
"""

def _connect():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER,
        password=DB_PASSWORD, database=DB_NAME
    )

def _ensure_table():
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(_TABLE_SQL)
        conn.commit()
    finally:
        conn.close()

def upsert_token(customer_id, customer_folder, type_token, access_token, refresh_token, expires_at_iso):
    _ensure_table()
    expires_dt = None
    if expires_at_iso:
        try:
            expires_dt = datetime.fromisoformat(expires_at_iso.replace("Z",""))
        except Exception:
            expires_dt = None
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO tokens (customer_id, customer_folder, type_token, access_token, refresh_token, expires_at)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                  customer_folder=VALUES(customer_folder),
                  access_token=VALUES(access_token),
                  refresh_token=VALUES(refresh_token),
                  expires_at=VALUES(expires_at)
            """, (customer_id, customer_folder, type_token, access_token, refresh_token, expires_dt))
        conn.commit()
    finally:
        conn.close()

def get_token(customer_id, type_token):
    _ensure_table()
    conn = _connect()
    try:
        with conn.cursor(dictionary=True) as cur:
            cur.execute("""
                SELECT customer_id, customer_folder, type_token,
                       access_token, refresh_token, expires_at, updated_at
                FROM tokens
                WHERE customer_id=%s AND type_token=%s
            """, (customer_id, type_token))
            row = cur.fetchone()
            if not row:
                return None
            if row["expires_at"]:
                row["expires_at"] = row["expires_at"].isoformat()
            return row
    finally:
        conn.close()

def get_all_tokens():
    _ensure_table()
    conn = _connect()
    rows = []
    try:
        with conn.cursor(dictionary=True) as cur:
            cur.execute("""
                SELECT customer_id, customer_folder, type_token,
                       access_token, refresh_token, expires_at, updated_at
                FROM tokens
            """)
            for r in cur.fetchall():
                if r["expires_at"]:
                    r["expires_at"] = r["expires_at"].isoformat()
                rows.append(r)
    finally:
        conn.close()
    return rows
