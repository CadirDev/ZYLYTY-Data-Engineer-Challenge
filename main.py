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

# Load .env file to environment
load_dotenv()

# Read environment variables
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
db_engine = create_engine(
    f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
Session = sessionmaker(bind=db_engine)

# Use a session to execute commands
session = Session()

print(render('Hello ZYLYTY!', colors=[
      'cyan', 'magenta'], align='center', font='3d'))
print(f"Admin API Key: {ADMIN_API_KEY}")
print(f"Database Host: {DB_HOST}")
print(f"Database Port: {DB_PORT}")
print(f"Database Username: {DB_USERNAME}")
print(f"Database Password: {DB_PASSWORD}")
print(f"Database Name: {DB_NAME}")
print(f"API Base URL: {API_BASE_URL}")

# Example main function


def main():
    totalAccounts = 0
    totalClients = 0
    totalTransactions = 0

    # Get data from API
    accountsData = etl_import_accounts()
    clientsData = etl_import_clients()
    transactionsData = etl_import_transactions()

    # Clean transactions data
    transactionsData = clean_Transactions_Data(transactionsData)

    # Total counts
    totalAccounts = len(accountsData)
    totalClients = len(clientsData)
    totalTransactions = len(transactionsData)

    # Insert data into SQL tables
    insert_Client_Data_to_Table(clientsData)
    insert_Account_Data_to_Table(accountsData)
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

    print(f"ZYLYTY Data Import Completed [{totalClients}, {
          totalAccounts}, {totalTransactions}]")

# Import accounts data from API


def etl_import_accounts():
    accountsURL = "download/accounts.csv"
    accountsResponse = requests.get(
        f"{API_BASE_URL}/{accountsURL}", headers=headers).content
    accountsData = pd.read_csv(io.StringIO(accountsResponse.decode('utf-8')))
    return accountsData

# Import clients data from API


def etl_import_clients():
    clientsURL = "download/clients.csv"
    clientsResponse = requests.get(
        f"{API_BASE_URL}/{clientsURL}", headers=headers).content
    clientsData = pd.read_csv(io.StringIO(clientsResponse.decode('utf-8')))
    return clientsData

# Import transactions data from API


def etl_import_transactions():
    transactionsURL = "transactions"
    transactionsData = get_Transactions_Paginated(
        f"{API_BASE_URL}/{transactionsURL}", headers)
    return transactionsData

# Get transactions paginated


def get_Transactions_Paginated(url, headers):
    page = 0
    results_per_page = 1000  # results per page
    max_pages = 300  # max pages
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
                response = requests.get(
                    url, headers=headers, params=params, timeout=10)

                if response.status_code == 200:
                    data = response.json()

                    if isinstance(data, list) and len(data) > 0:
                        page_data = pd.DataFrame(data)
                    elif isinstance(data, dict) and "results" in data and len(data["results"]) > 0:
                        page_data = pd.DataFrame(data["results"])
                    else:
                        break  # Stop if no more pages

                    all_data = pd.concat(
                        [all_data, page_data], ignore_index=True)
                    success = True
                    page += 1  # Go to next page

                else:
                    attempts += 1
                    time.sleep(2)

            except requests.exceptions.RequestException as e:
                attempts += 1
                time.sleep(2)

        if not success:
            break

    return all_data

# Cleaning Data


def clean_Transactions_Data(all_data):
    if not all_data.empty:
        all_data = all_data.drop_duplicates(subset=['timestamp', 'account_id'])
        all_data['amount'] = pd.to_numeric(
            all_data['amount'], errors='coerce').fillna(0)
    return all_data

# Insert Account Data into SQL tables


def insert_Account_Data_to_Table(accountsData):
    dtype_mapping = {
        'account_id': BigInteger,
        'client_id': String(50)
    }
    accountsData.to_sql('accounts', db_engine, if_exists='append',
                        index=False, dtype=dtype_mapping, chunksize=5000)

# Insert Client Data into SQL tables


def insert_Client_Data_to_Table(clientsData):
    dtype_mapping = {
        'client_id': String(50),
        'client_name': String(50),
        'client_email': String(40),
        'client_birth_date': Date
    }
    clientsData.to_sql('clients', db_engine, if_exists='append',
                       index=False, dtype=dtype_mapping, chunksize=5000)

# Insert Transaction Data into SQL tables


def insert_Transaction_Data_to_Table(transactionsData):
    dtype_mapping = {
        'transaction_id': BigInteger,
        'timestamp': DateTime,
        'account_id': BigInteger,
        'amount': Numeric(10, 2),
        'type': String(5),
        'medium': String(10)
    }
    transactionsData.to_sql('transactions', db_engine, if_exists='append',
                            index=False, dtype=dtype_mapping, chunksize=5000)

# Create view called client_transaction_counts


def create_View_Client_Transaction_Counts():
    create_view_query = '''
        CREATE OR REPLACE VIEW client_transaction_counts AS
        SELECT c.client_id, COUNT(tr.transaction_id) AS transaction_count
        FROM clients AS c
        INNER JOIN accounts AS a ON c.client_id = a.client_id
        INNER JOIN transactions AS tr ON a.account_id = tr.account_id
        GROUP BY c.client_id
        ORDER BY c.client_id;
    '''
    session.execute(text(create_view_query))
    session.commit()  # Commit transaction

# Create view called monthly_transaction_summary


def create_View_Monthly_Transaction_Summary():
    create_view_query = '''
        CREATE OR REPLACE VIEW monthly_transaction_summary AS
        SELECT TO_CHAR(DATE_TRUNC('month', timestamp), 'YYYY-MM-01') AS month, 
               client_email,
               COUNT(tr.transaction_id) AS transaction_count, 
               SUM(tr.amount) AS total_amount
        FROM transactions AS tr
        INNER JOIN accounts AS a ON tr.account_id = a.account_id
        INNER JOIN clients AS c ON c.client_id = a.client_id
        GROUP BY TO_CHAR(DATE_TRUNC('month', timestamp), 'YYYY-MM-01'), client_email
        ORDER BY month, client_email;
    '''
    session.execute(text(create_view_query))
    session.commit()  # Commit transaction

# Create view called high_transaction_accounts


def create_View_High_Transaction_Accounts():
    create_view_query = '''
        CREATE OR REPLACE VIEW high_transaction_accounts AS
        SELECT TO_CHAR(DATE_TRUNC('month', timestamp), 'YYYY-MM-DD') AS date, 
               account_id,
               COUNT(transaction_id) AS transaction_count
        FROM transactions AS tr
        GROUP BY TO_CHAR(DATE_TRUNC('month', timestamp), 'YYYY-MM-DD'), account_id
        HAVING COUNT(transaction_id) > 2
        ORDER BY date, account_id;
    '''
    session.execute(text(create_view_query))
    session.commit()  # Commit transaction


if __name__ == "__main__":
    main()
