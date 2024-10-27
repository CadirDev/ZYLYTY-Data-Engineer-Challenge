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
from concurrent.futures import ThreadPoolExecutor
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO)

# Carregar variáveis de ambiente
load_dotenv()

# Configuração das variáveis de ambiente
ADMIN_API_KEY = os.getenv('ADMIN_API_KEY')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
API_BASE_URL = os.getenv('API_BASE_URL')

# Token Bearer - API Key
headers = {"Authorization": f"Bearer {ADMIN_API_KEY}"}

# Database Engine
db_engine = create_engine(f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
Session = sessionmaker(bind=db_engine)

print(render('Hello ZYLYTY!', colors=['cyan', 'magenta'], align='center', font='3d'))


print(render('Hello ZYLYTY!', colors=['cyan', 'magenta'], align='center', font='3d'))
print(f"Admin API Key: {ADMIN_API_KEY}")
print(f"Database Host: {DB_HOST}")
print(f"Database Port: {DB_PORT}")
print(f"Database Username: {DB_USERNAME}")
print(f"Database Password: {DB_PASSWORD}")
print(f"Database Name: {DB_NAME}")
print(f"API Base URL: {API_BASE_URL}")

def main():
    with Session() as session:
        totalAccounts, totalClients, totalTransactions = 0, 0, 0
        
        # Usar ThreadPoolExecutor para execução paralela
        with ThreadPoolExecutor(max_workers=3) as executor:
            accountsFuture = executor.submit(etl_import_accounts)
            clientsFuture = executor.submit(etl_import_clients)
            transactionsFuture = executor.submit(etl_import_transactions)
            
            accountsData = accountsFuture.result()
            clientsData = clientsFuture.result()
            transactionsData = transactionsFuture.result()

        transactionsData = clean_transactions_data(transactionsData)
        
        totalAccounts = len(accountsData)
        totalClients = len(clientsData)
        totalTransactions = len(transactionsData)

        try:
            insert_Account_Data_to_Table(accountsData, session)
            insert_Client_Data_to_Table(clientsData, session)
            insert_Transaction_Data_to_Table(transactionsData, session)
            
            create_View_Client_Transaction_Counts(session)
            create_View_Monthly_Transaction_Summary(session)
            create_View_High_Transaction_Accounts(session)
            print("Views criadas com sucesso!")
        except Exception as e:
            logging.error(f"Error occurred: {e}")

        print(f"ZYLYTY Data Import Completed [{totalClients}, {totalAccounts}, {totalTransactions}]")

def etl_import_accounts():
    accountsURL = "download/accounts.csv"
    accountsResponse = requests.get(f"{API_BASE_URL}/{accountsURL}", headers=headers).content
    return pd.read_csv(io.StringIO(accountsResponse.decode('utf-8')))

def etl_import_clients():
    clientsURL = "download/clients.csv"
    clientsResponse = requests.get(f"{API_BASE_URL}/{clientsURL}", headers=headers).content
    return pd.read_csv(io.StringIO(clientsResponse.decode('utf-8')))

def etl_import_transactions():
    transactionsURL = "transactions"
    return get_transactions_paginated(f"{API_BASE_URL}/{transactionsURL}", headers)

def get_transactions_paginated(url, headers, config=None):
    # Default configuration for pagination and retries
    config = config or {
        "results_per_page": 1000,
        "max_pages": 30,
        "max_retries": 5,
        "timeout": 10,
        "sleep_duration": 1,
    }

    page = 0
    all_data_pages = []

    while page <= config["max_pages"]:
        attempts = 0
        while attempts < config["max_retries"]:
            try:
                # Request with pagination
                params = {"page": page, "limit": config["results_per_page"]}
                response = requests.get(url, headers=headers, params=params, timeout=config["timeout"])

                if response.status_code == 200:
                    data = response.json()

                    # Process data structure and standardize format
                    if isinstance(data, list) and data:
                        page_data = pd.DataFrame(data)
                    elif isinstance(data, dict) and "results" in data and data["results"]:
                        page_data = pd.DataFrame(data["results"])
                    else:
                        # Exit if no more data is found
                        return pd.concat(all_data_pages, ignore_index=True)

                    # Convert columns to efficient types if necessary
                    for col in page_data.select_dtypes(include=["int64"]).columns:
                        page_data[col] = pd.to_numeric(page_data[col], downcast="integer")
                    for col in page_data.select_dtypes(include=["float64"]).columns:
                        page_data[col] = pd.to_numeric(page_data[col], downcast="float")
                    for col in page_data.select_dtypes(include=["object"]).columns:
                        if page_data[col].nunique() / len(page_data) < 0.5:
                            page_data[col] = page_data[col].astype("category")

                    # Add current page data
                    all_data_pages.append(page_data)
                    page += 1
                    break  # Exit retry loop on success

                else:
                    attempts += 1
                    time.sleep(config["sleep_duration"])  # Wait before retrying

            except requests.exceptions.RequestException:
                attempts += 1
                time.sleep(config["sleep_duration"])  # Wait before retrying

        # Check for failure after max attempts
        if attempts == config["max_retries"]:
            logging.error(f"Failed to load page {page} after {config['max_retries']} attempts.")
            break

    # Concatenate all pages at the end
    return pd.concat(all_data_pages, ignore_index=True)

def clean_transactions_data(all_data):
    if not all_data.empty:
        all_data = all_data[all_data['transaction_id'].notna()].drop_duplicates(subset=['transaction_id', 'account_id'])
        all_data['amount'] = pd.to_numeric(all_data['amount'], errors='coerce').fillna(0).astype(float)
    return all_data

def insert_Account_Data_to_Table(accountsData, session):
    dtype_mapping = {'account_id': BigInteger, 'client_id': String(50)}
    accountsData.to_sql('accounts', con=session.bind, if_exists='append', index=False, dtype=dtype_mapping)

def insert_Client_Data_to_Table(clientsData, session): 
    dtype_mapping = {'client_id': String(50), 'client_name': String(50), 'client_email': String(40), 'client_birth_date': Date}
    clientsData.to_sql('clients', con=session.bind, if_exists='append', index=False, dtype=dtype_mapping)

def insert_Transaction_Data_to_Table(transactionsData, session):
    dtype_mapping = {'transaction_id': BigInteger, 'timestamp': DateTime, 'account_id': BigInteger, 'amount': Numeric(10,2), 'type': String(5), 'medium': String(10)}
    transactionsData.to_sql('transactions', con=session.bind, if_exists='append', index=False, dtype=dtype_mapping)

def create_View_Client_Transaction_Counts(session):
    create_view_query = '''CREATE OR REPLACE VIEW client_transaction_counts AS
                           SELECT c.client_id, COUNT(transaction_id) AS transaction_count
                           FROM clients AS c
                           INNER JOIN accounts AS a ON c.client_id = a.client_id
                           INNER JOIN transactions AS tr ON a.account_id = tr.account_id
                           GROUP BY c.client_id ORDER BY c.client_id;'''
    session.execute(text(create_view_query))
    session.commit()  

def create_View_Monthly_Transaction_Summary(session):
    create_view_query = '''CREATE OR REPLACE VIEW monthly_transaction_summary AS
                           SELECT TO_CHAR(date_trunc('month', timestamp), 'YYYY-MM-01') AS month, client_email,
                                  COUNT(transaction_id) AS transaction_count, SUM(amount) AS total_amount
                           FROM transactions AS tr
                           INNER JOIN accounts AS a ON tr.account_id = a.account_id
                           INNER JOIN clients AS c ON c.client_id = a.client_id
                           GROUP BY TO_CHAR(date_trunc('month', timestamp), 'YYYY-MM-01'), client_email
                           ORDER BY month, client_email;'''
    session.execute(text(create_view_query))
    session.commit()  

def create_View_High_Transaction_Accounts(session):
    create_view_query = '''CREATE OR REPLACE VIEW high_transaction_accounts AS
                           SELECT TO_CHAR(date_trunc('month', timestamp), 'YYYY-MM-DD') AS date, account_id,
                                  COUNT(transaction_id) AS transaction_count
                           FROM transactions AS tr
                           GROUP BY TO_CHAR(date_trunc('month', timestamp), 'YYYY-MM-DD'), account_id
                           HAVING COUNT(transaction_id) > 2
                           ORDER BY date, account_id;'''
    session.execute(text(create_view_query))
    session.commit()  

if __name__ == "__main__":
    main()