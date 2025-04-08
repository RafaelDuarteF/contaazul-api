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

app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY

# Register blueprints
app.register_blueprint(auth_bp)
app.register_blueprint(etl_bp)
app.register_blueprint(token_bp)
app.register_blueprint(data_bp)
app.register_blueprint(token_actions_bp)
app.register_blueprint(token_new_bp)

if __name__ == "__main__":
    # Only for local development
    app.run(port=8000, debug=True)
