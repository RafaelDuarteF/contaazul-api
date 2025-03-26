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
REDIRECT_URI = os.getenv("REDIRECT_URI", "https://yourusername.pythonanywhere.com/callback")
AUTH_URL = "https://api.contaazul.com/auth/authorize"
TOKEN_URL = "https://api.contaazul.com/oauth2/token"

# Data Storage Configuration
DATA_PATH = Path(os.getenv('DATA_OUTPUT_PATH', './data/private'))
DATA_PATH.mkdir(parents=True, exist_ok=True)
TOKEN_FILE = DATA_PATH / "access_token.json"

# API Access Configuration
API_USERNAME = os.getenv("API_USERNAME")
API_PASSWORD = os.getenv("API_PASSWORD")
