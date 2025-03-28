# ContaAzul Integration  

Integration service for ContaAzul API with ETL capabilities.  

## Features  

- **OAuth2 authentication** with ContaAzul  
- **Sales data extraction and transformation**  
- **Automatic token refresh**  
- **Protected data access endpoints**  
- **JSON data storage**  

## Deployment  

1. **Fork/push** this repository to GitHub.  
2. **Create a new Web Service** on Render.  
3. **Connect your GitHub repository**.  
4. **Add the following environment variables** in the Render dashboard:  
   - `CLIENT_ID`: Your ContaAzul client ID  
   - `CLIENT_SECRET`: Your ContaAzul client secret  
   - `REDIRECT_URI`: Your app's callback URL (`https://your-app.onrender.com/callback`)  
   - `API_USERNAME`: Username for accessing data endpoints  
   - `API_PASSWORD`: Password for accessing data endpoints  
   - `DATA_OUTPUT_PATH`: Path to store data files  
   - `AUTH_URL`: ContaAzul authentication URL  
   - `TOKEN_URL`: ContaAzul token URL  

The service will automatically:  
- Use **Python 3.13.2**  
- Install dependencies from **`requirements.txt`**  
- Start with **Gunicorn** using the configuration in **`gunicorn.conf.py`**  
- Mount a **persistent disk** at `/data` for storing JSON files  

---

## Local Development  

1. **Create a virtual environment**:  
   ```bash  
   python -m venv venv  
   source venv/bin/activate  # On Windows: venv\Scripts\activate  
   ```  

2. **Install dependencies**:  
   ```bash  
   pip install -r requirements.txt  
   ```  

3. **Copy `.env.example` to `.env` and fill in your credentials**:  
   ```bash  
   cp .env.example .env  
   ```  

4. **Run the development server**:  
   ```bash  
   python index.py  
   ```  

---

## API Endpoints  

### **1. Authentication**  
- `GET /oauth`: Initiates the OAuth flow.  
- `GET /callback`: Receives the authorization code and returns the access token.  

### **2. Token Management**  
- `GET /get-tokens`: Returns all client access tokens.  
- `POST /insert-tokens`: Inserts or updates access tokens. 
Example Body: 
```json
   {
      "customers": [
         {
            "access_token": "your-token",
            "customer_id": "your-customer-id",
            "customer_folder": "your-customer-folder",
            "expires_at": "2025-03-26T19:05:00.896116",
            "refresh_token": "your-refresh-token"
         }
      ]
   }
```  

- `GET /refresh_token/<customer_id>`: Refreshes a client's access token.  

### **3. Data Handling**  
- `GET /read/<customer_id>/<data_type>`: Returns the latest data of the specified type (e.g., `sales`, `products`).  
- `GET /list/<customer_id>`: Lists all data files for a client.  

### **4. ETL Operations**  
- `GET /extract_sales/<customer_id>`: Extracts and transforms sales data.  
- `GET /read/<customer_id>/<data_type>`: Reads stored data (requires authentication).  
- `GET /list/<customer_id>`: Lists available data files (requires authentication).  

---

## Authentication  

Protected endpoints use **Basic Authentication**:  
```bash  
curl -u username:password https://your-app.com/read/<customer_id>/sales  
```  

---