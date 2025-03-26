# ContaAzul Integration

Integration service for ContaAzul API with ETL capabilities.

## Features

- OAuth2 authentication with ContaAzul
- Sales data extraction and transformation
- Automatic token refresh
- Protected data access endpoints
- JSON data storage

## Deployment on Render

1. Fork/push this repository to GitHub
2. Create a new Web Service on Render
3. Connect your GitHub repository
4. Add the following environment variables in Render dashboard:
   - `CLIENT_ID`: Your ContaAzul client ID
   - `CLIENT_SECRET`: Your ContaAzul client secret
   - `REDIRECT_URI`: Your app's callback URL (https://your-app.onrender.com/callback)
   - `API_USERNAME`: Username for accessing data endpoints
   - `API_PASSWORD`: Password for accessing data endpoints

The service will automatically:
- Use Python 3.13.2
- Install dependencies from requirements.txt
- Start with Gunicorn using the configuration in gunicorn.conf.py
- Mount a persistent disk at /data for storing JSON files

## Local Development

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Copy .env.example to .env and fill in your credentials:
```bash
cp .env.example .env
```

4. Run the development server:
```bash
python index.py
```

## API Endpoints

- `/`: OAuth authorization
- `/callback`: OAuth callback
- `/extract_sales`: Extract and transform sales data
- `/refresh_token`: Refresh OAuth token
- `/read_data/<type>`: Read stored data (requires auth)
- `/list_data`: List available data files (requires auth)

## Authentication

Protected endpoints use Basic Authentication:
```bash
curl -u username:password https://your-app.onrender.com/read_data/sales
```
