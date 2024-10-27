import os
from cfonts import render
from dotenv import load_dotenv
import requests
import pandas as pd
import io
import time
from sqlalchemy import create_engine, Numeric, String, BigInteger, Date, DateTime, text
from sqlalchemy.orm import sessionmaker

# Load environment variables
load_dotenv()

# Load environment variables needed for API and Database
ADMIN_API_KEY = os.getenv('ADMIN_API_KEY')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
API_BASE_URL = os.getenv('API_BASE_URL')

# Set Authorization for API
headers = {"Authorization": f"Bearer {ADMIN_API_KEY}"}

# Database connection engine
db_engine = create_engine(f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
Session = sessionmaker(bind=db_engine)

print(render('Hello ZYLYTY!', colors=['cyan', 'magenta'], align='center', font='3d'))

print(f"Admin API Key: {ADMIN_API_KEY}")
print(f"Database Host: {DB_HOST}")
print(f"Database Port: {DB_PORT}")
print(f"Database Username: {DB_USERNAME}")
print(f"Database Password: {DB_PASSWORD}")
print(f"Database Name: {DB_NAME}")
print(f"API Base URL: {API_BASE_URL}")

def main():
    """Main function to run the ETL process and create necessary views."""
    totalAccounts, totalClients, totalTransactions = 0, 0, 0
    session = Session()  # Create session

    # Import data from API
    accountsData = etl_import_accounts()
    clientsData = etl_import_clients()
    transactionsData = etl_import_transactions()

    # Clean transactions data
    transactionsData = clean_transactions_data(transactionsData)

    # Calculate data metrics
    totalAccounts = len(accountsData)
    totalClients = len(clientsData)
    totalTransactions = len(transactionsData)

    # Data insertion into SQL tables
    insert_account_data(accountsData, session)
    insert_client_data(clientsData, session)
    insert_transaction_data(transactionsData, session)

    # Create SQL views
    try:
        create_view_client_transaction_counts(session)
        create_view_monthly_transaction_summary(session)
        create_view_high_transaction_accounts(session)
        #print("Views created successfully.")
    except Exception as e:
        print(f"Error while creating views: {e}")
    finally:
        session.close()  # Close session

    # Log import completion
    print(f"ZYLYTY Data Import Completed [{totalClients}, {totalAccounts}, {totalTransactions}]")

def etl_import_accounts():
    """Fetch account data from API and load into DataFrame."""
    accountsURL = "download/accounts.csv"
    accountsResponse = requests.get(f"{API_BASE_URL}/{accountsURL}", headers=headers).content
    accountsData = pd.read_csv(io.StringIO(accountsResponse.decode('utf-8')))
    return accountsData

def etl_import_clients():
    """Fetch client data from API and load into DataFrame."""
    clientsURL = "download/clients.csv"
    clientsResponse = requests.get(f"{API_BASE_URL}/{clientsURL}", headers=headers).content
    clientsData = pd.read_csv(io.StringIO(clientsResponse.decode('utf-8')))
    return clientsData

def etl_import_transactions():
    """Fetch transaction data from API with pagination and load into DataFrame."""
    transactionsURL = "transactions"
    transactionsData = get_transactions_paginated(f"{API_BASE_URL}/{transactionsURL}", headers)
    return transactionsData

def get_transactions_paginated(url, headers):
    """Fetch paginated transaction data from API."""
    page, results_per_page, max_pages, max_retries = 0, 1000, 300, 5
    all_data = pd.DataFrame()

    while page <= max_pages:
        attempts, success = 0, False
        while attempts < max_retries and not success:
            try:
                response = requests.get(url, headers=headers, params={"page": page, "limit": results_per_page}, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, list) and data:
                        page_data = pd.DataFrame(data)
                    elif isinstance(data, dict) and "results" in data and data["results"]:
                        page_data = pd.DataFrame(data["results"])
                    else:
                        break
                    all_data = pd.concat([all_data, page_data], ignore_index=True)
                    success = True
                    page += 1
                else:
                    attempts += 1
                    time.sleep(2)
            except requests.exceptions.RequestException:
                attempts += 1
                time.sleep(2)
        if not success:
            break
    return all_data

# Clean transaction data
def clean_transactions_data(all_data):
    """Clean transaction data by removing duplicates and converting invalid amounts."""
    if not all_data.empty:
        # Remove duplicate transaction_id
        all_data = all_data.drop_duplicates(subset=['timestamp', 'account_id'])
        
        # Convert non-numeric values in 'amount' to zero
        all_data.loc[:, 'amount'] = pd.to_numeric(all_data['amount'], errors='coerce').fillna(0)
        
    return all_data

def insert_account_data(accountsData, session):
    """Insert account data into the database."""
    dtype_mapping = {'account_id': BigInteger, 'client_id': String(50)}
    accountsData.to_sql('accounts', db_engine, if_exists='replace', index=False, dtype=dtype_mapping)

def insert_client_data(clientsData, session):
    """Insert client data into the database."""
    dtype_mapping = {'client_id': String(50), 'client_name': String(50), 'client_email': String(40), 'client_birth_date': Date}
    clientsData.to_sql('clients', db_engine, if_exists='replace', index=False, dtype=dtype_mapping)

def insert_transaction_data(transactionsData, session):
    """Insert transaction data into the database."""
    dtype_mapping = {'transaction_id': BigInteger, 'timestamp': DateTime, 'account_id': BigInteger, 'amount': Numeric(10, 2), 'type': String(5), 'medium': String(10)}
    transactionsData.to_sql('transactions', db_engine, if_exists='replace', index=False, dtype=dtype_mapping)

def create_view_client_transaction_counts(session):
    """Create SQL view for client transaction counts."""
    create_view_query = '''
        CREATE OR REPLACE VIEW client_transaction_counts AS
        SELECT c.client_id, COUNT(transaction_id) AS transaction_count
        FROM clients AS c
        INNER JOIN accounts AS a ON c.client_id = a.client_id
        INNER JOIN transactions AS tr ON a.account_id = tr.account_id
        GROUP BY c.client_id
        ORDER BY c.client_id;
    '''
    session.execute(text(create_view_query))
    session.commit()

def create_view_monthly_transaction_summary(session):
    """Create SQL view for monthly transaction summary."""
    create_view_query = '''
        CREATE OR REPLACE VIEW monthly_transaction_summary AS
        SELECT TO_CHAR(DATE_TRUNC('month', timestamp), 'YYYY-MM-01') AS month, client_email,
               COUNT(transaction_id) AS transaction_count, SUM(amount) AS total_amount
        FROM transactions AS tr
        INNER JOIN accounts AS a ON tr.account_id = a.account_id
        INNER JOIN clients AS c ON c.client_id = a.client_id
        GROUP BY TO_CHAR(DATE_TRUNC('month', timestamp), 'YYYY-MM-01'), client_email
        ORDER BY month, client_email;
    '''
    session.execute(text(create_view_query))
    session.commit()

def create_view_high_transaction_accounts(session):
    """Create SQL view for high transaction accounts."""
    create_view_query = '''
        CREATE OR REPLACE VIEW high_transaction_accounts AS
        SELECT TO_CHAR(DATE_TRUNC('month', timestamp), 'YYYY-MM-DD') AS date, account_id,
               COUNT(transaction_id) AS transaction_count
        FROM transactions AS tr
        GROUP BY TO_CHAR(DATE_TRUNC('month', timestamp), 'YYYY-MM-DD'), account_id
        HAVING COUNT(transaction_id) > 2
        ORDER BY date, account_id;
    '''
    session.execute(text(create_view_query))
    session.commit()

if __name__ == "__main__":
    main()
