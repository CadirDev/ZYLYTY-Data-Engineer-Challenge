import os
from cfonts import render
from dotenv import load_dotenv
import requests
import pandas as pd
import io
import time
from sqlalchemy import create_engine, Numeric, String, BigInteger, Date, DateTime, text
from sqlalchemy.orm import sessionmaker
import psycopg2

# Note: Do not rename this file, it must be the entry point of your application.

# load .env file to environment
load_dotenv()

# Note 2: You must read from the following environment variables:
# ADMIN_API_KEY -> "The secret API key used to call the API endpoints (the Bearer token)"
# DB_HOST -> "The hostname of the database"
# DB_PORT -> "The port of the database"
# DB_USERNAME -> "The username of the database"
# DB_PASSWORD -> "The password of the database"
# DB_NAME -> "The name of the database"
# API_BASE_URL -> "The base URL of the API your project will connect to"

# Example:
ADMIN_API_KEY = os.getenv('ADMIN_API_KEY')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
API_BASE_URL = os.getenv('API_BASE_URL')

# Token Bearer - API Key
headers = {
        "Authorization": f"Bearer {ADMIN_API_KEY}"
    }

# Database Engine
db_engine = create_engine(f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
SessionLocal = sessionmaker(bind=db_engine)

# Use a session to execute command
session = SessionLocal()

print(render('Hello ZYLYTY!', colors=['cyan', 'magenta'], align='center', font='3d'))
print(f"Admin API Key: {ADMIN_API_KEY}")
print(f"Database Host: {DB_HOST}")
print(f"Database Port: {DB_PORT}")
print(f"Database Username: {DB_USERNAME}")
print(f"Database Password: {DB_PASSWORD}")
print(f"Database Name: {DB_NAME}")
print(f"API Base URL: {API_BASE_URL}")

# Example main, modify at will
def main():
    totalAccounts = 0
    totalClients = 0
    totalTransactions = 0
    
    # Get accounts data from ETL
    accountsData = etl_import_accounts()
    # Get clients data from ETL
    clientsData = etl_import_clients()
    # Get transactions data from ETL
    transactionsData = etl_import_transactions()
    
    # Clean transactions data
    transactionsData = clean_Transactions_Data(transactionsData)
    
    # Total Accounts
    totalAccounts = len(accountsData)
    # Total Clients
    totalClients = len(clientsData)
    # Total Transactions
    totalTransactions = len(transactionsData)
    
    # Insert Account Data imported to SQL tables
    insert_Account_Data_to_Table(accountsData)
    # Insert Client Data imported to SQL tables
    insert_Client_Data_to_Table(clientsData)
    # Insert Transaction Data imported to SQL tables
    insert_Transaction_Data_to_Table(transactionsData)
    
    try:
        # Create views
        create_View_Client_Transaction_Counts()
        create_View_Monthly_Transaction_Summary()
        create_View_High_Transaction_Accounts()
        print("Views created successfully!")
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        session.close()  # Close session  
    
    # Print the summary message
    print(f"ZYLYTY Data Import Completed [{totalClients}, {totalAccounts}, {totalTransactions}]")

# Import accounts data from API
def etl_import_accounts():
    accountsURL = "download/accounts.csv"
    
    payloadAccount = {}
    accountsResponse = requests.request("GET", f"{API_BASE_URL}/{accountsURL}", headers=headers, data=payloadAccount).content
    accountsData = pd.read_csv(io.StringIO(accountsResponse.decode('utf-8')))
    
    return accountsData

def etl_import_clients():
    clientsURL = "download/clients.csv"
    
    payloadClients = {}
    clientsResponse = requests.request("GET", f"{API_BASE_URL}/{clientsURL}", headers=headers, data=payloadClients).content
    clientsData = pd.read_csv(io.StringIO(clientsResponse.decode('utf-8')))
    
    return clientsData

def etl_import_transactions():
    transactionsURL = "transactions"
    transactionsData = get_Transactions_Paginated(f"{API_BASE_URL}/{transactionsURL}", headers)
    
    return transactionsData

# Get transactions paginated
def get_Transactions_Paginated(url, headers):
    page = 0
    results_per_page = 1500  # result per page
    max_pages = 20  # max pages
    max_retries = 5  # max number of attempts
    all_data = pd.DataFrame()

    while page <= max_pages:
        attempts = 0
        success = False

        while attempts < max_retries and not success:
            try:
                params = {
                    "page": page,
                    "limit": results_per_page
                }
                response = requests.get(url, headers=headers, params=params, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, list) and len(data) > 0:
                        page_data = pd.DataFrame(data)
                    elif isinstance(data, dict) and "results" in data and len(data["results"]) > 0:
                        page_data = pd.DataFrame(data["results"])
                    else:
                        break  # Stop looping If no more pages

                    all_data = pd.concat([all_data, page_data], ignore_index=True)
                    success = True
                    page += 1
                
                else:
                    attempts += 1
                    time.sleep(1)

            except requests.exceptions.RequestException:
                attempts += 1
                time.sleep(1)
        
        if not success:
            break
    
    return all_data  

# Cleaning Data
def clean_Transactions_Data(all_data):
    if not all_data.empty:
        all_data = all_data.drop_duplicates(subset=['timestamp', 'account_id'])
        all_data['amount'] = pd.to_numeric(all_data['amount'], errors='coerce').fillna(0)
    
    return all_data

# Insert Account Data imported to SQL tables
def insert_Account_Data_to_Table(accountsData):
    dtype_mapping = {
        'account_id': BigInteger,
        'client_id': String(55)
    }
    accountsData.to_sql('accounts', db_engine, if_exists='replace', index=False, dtype=dtype_mapping)

# Insert Client Data imported to SQL tables
def insert_Client_Data_to_Table(clientsData): 
    dtype_mapping = {
        'client_id': String(40),
        'client_name': String(40),
        'client_email': String(40),
        'client_birth_date': Date
    }
    clientsData.to_sql('clients', db_engine, if_exists='replace', index=False, dtype=dtype_mapping)
    
# Insert Transaction Data imported to SQL tables
def insert_Transaction_Data_to_Table(transactionsData):
    dtype_mapping = {
        'transaction_id': BigInteger,
        'timestamp': DateTime,
        'account_id': BigInteger,
        'amount': Numeric(10, 2),
        'type': String(5),
        'medium': String(10)
    }
    transactionsData.to_sql('transactions', db_engine, if_exists='replace', index=False, dtype=dtype_mapping)

# Create view for client transaction counts
def create_View_Client_Transaction_Counts():
    create_view_query = '''
        CREATE OR REPLACE VIEW client_transaction_counts AS
        SELECT c.client_id, count(transaction_id) as transaction_count
        FROM clients as c
        INNER JOIN accounts as a ON c.client_id = a.client_id
        INNER JOIN transactions as tr ON a.account_id = tr.account_id
        GROUP BY c.client_id
        ORDER BY c.client_id;
    '''
  
    with SessionLocal() as session:
        session.execute(text(create_view_query))  
        session.commit()

# Create view for monthly transaction summary 
def create_View_Monthly_Transaction_Summary():
    create_view_query = '''
        CREATE OR REPLACE VIEW monthly_transaction_summary AS
        SELECT to_char(date_trunc('month', timestamp::timestamp), 'YYYY-MM-01') as month, client_email,
        count(transaction_id) as transaction_count, sum(amount) as total_amount
        FROM transactions as tr
        INNER JOIN accounts as a ON tr.account_id = a.account_id
        INNER JOIN clients as c ON c.client_id = a.client_id
        GROUP BY to_char(date_trunc('month', timestamp::timestamp), 'YYYY-MM-01'), client_email
        ORDER BY month, client_email;
    '''
    
    with SessionLocal() as session:
        session.execute(text(create_view_query))  
        session.commit()

# Create view for high transaction accounts  
def create_View_High_Transaction_Accounts():
    create_view_query = '''
        CREATE OR REPLACE VIEW high_transaction_accounts AS
        SELECT to_char(date_trunc('month', timestamp::timestamp), 'YYYY-MM-DD') as date, account_id,
        count(transaction_id) as transaction_count
        FROM transactions as tr
        GROUP BY to_char(date_trunc('month', timestamp::timestamp), 'YYYY-MM-DD'), account_id
        HAVING count(transaction_id) > 2
        ORDER BY date, account_id;
    '''
    
    with SessionLocal() as session:
        session.execute(text(create_view_query))  
        session.commit()

if __name__ == "__main__":
    main()
