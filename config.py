import os
import secrets
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Flask Configuration
FLASK_SECRET_KEY = os.getenv('FLASK_SECRET_KEY', secrets.token_hex(16))

# ContaAzul Configuration
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")
AUTH_URL = os.getenv("AUTH_URL", "https://api.contaazul.com/auth")
TOKEN_URL = os.getenv("TOKEN_URL", "https://api.contaazul.com/auth/token")

CLIENT_NEW_ID = os.getenv("CLIENT_NEW_ID")
CLIENT_NEW_SECRET = os.getenv("CLIENT_NEW_SECRET")
REDIRECT_NEW_URI = os.getenv("REDIRECT_NEW_URI")
AUTH_NEW_URL = os.getenv("AUTH_NEW_URL")
TOKEN_NEW_URL = os.getenv("TOKEN_NEW_URL")

# Data Storage Configuration
DATA_PATH = Path(os.getenv('DATA_OUTPUT_PATH', './data/private'))
DATA_PATH.mkdir(parents=True, exist_ok=True)
TOKEN_FILE = DATA_PATH / "access_token.json"

# API Access Configuration
API_USERNAME = os.getenv("API_USERNAME")
API_PASSWORD = os.getenv("API_PASSWORD")

GOOGLE_APPLICATION_CREDENTIALS_JSON = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")