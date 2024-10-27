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

# Environment variables
ADMIN_API_KEY = os.getenv('ADMIN_API_KEY')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
API_BASE_URL = os.getenv('API_BASE_URL')

# Database Engine
db_engine = create_engine(
    f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
Session = sessionmaker(bind=db_engine)

# Print greeting message
print(render('Hello ZYLYTY!', colors=[
      'cyan', 'magenta'], align='center', font='3d'))

print(f"Admin API Key: {ADMIN_API_KEY}")
print(f"Database Host: {DB_HOST}")
print(f"Database Port: {DB_PORT}")
print(f"Database Username: {DB_USERNAME}")
print(f"Database Password: {DB_PASSWORD}")
print(f"Database Name: {DB_NAME}")
print(f"API Base URL: {API_BASE_URL}")


def main():
    totalAccounts, totalClients, totalTransactions = 0, 0, 0

    # Import data
    accountsData = etl_import_accounts()
    clientsData = etl_import_clients()
    transactionsData = etl_import_transactions()

    # Clean transactions data
    transactionsData = clean_transactions_data(transactionsData)

    # Insert data into SQL tables
    insert_data_to_tables(accountsData, clientsData, transactionsData)

    # Create views
    create_views()

    print(f"ZYLYTY Data Import Completed [{len(clientsData)}, {
          len(accountsData)}, {len(transactionsData)}]")


def etl_import_accounts():
    accountsURL = "download/accounts.csv"
    try:
        accountsResponse = requests.get(f"{API_BASE_URL}/{accountsURL}", headers={
                                        "Authorization": f"Bearer {ADMIN_API_KEY}"}, timeout=10)
        accountsResponse.raise_for_status()  # Raise an error for bad responses
        accountsData = pd.read_csv(io.StringIO(
            accountsResponse.content.decode('utf-8')))
        return accountsData
    except Exception as e:
        # print(f"Error importing accounts data: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error


def etl_import_clients():
    clientsURL = "download/clients.csv"
    try:
        clientsResponse = requests.get(f"{API_BASE_URL}/{clientsURL}", headers={
                                       "Authorization": f"Bearer {ADMIN_API_KEY}"}, timeout=10)
        clientsResponse.raise_for_status()
        clientsData = pd.read_csv(io.StringIO(
            clientsResponse.content.decode('utf-8')))
        return clientsData
    except Exception as e:
        # print(f"Error importing clients data: {e}")
        return pd.DataFrame()


def etl_import_transactions():
    transactionsURL = "transactions"
    return get_transactions_paginated(f"{API_BASE_URL}/{transactionsURL}")


def get_transactions_paginated(url):
    page = 0
    results_per_page = 1000
    max_pages = 300
    all_data = pd.DataFrame()

    while page <= max_pages:
        try:
            response = requests.get(url, headers={"Authorization": f"Bearer {
                                    ADMIN_API_KEY}"}, params={"page": page, "limit": results_per_page}, timeout=10)
            response.raise_for_status()
            data = response.json()
            page_data = pd.DataFrame(
                data["results"]) if "results" in data else pd.DataFrame(data)
            all_data = pd.concat([all_data, page_data], ignore_index=True)
            if len(page_data) < results_per_page:  # Stop if no more data
                break
            page += 1
        except requests.exceptions.RequestException as e:
            # print(f"Error fetching page {page}: {e}")
            time.sleep(2)  # Wait before retrying
            continue

    return all_data


def clean_transactions_data(all_data):
    if not all_data.empty:
        all_data = all_data.drop_duplicates(subset=['timestamp', 'account_id'])
        all_data['amount'] = pd.to_numeric(
            all_data['amount'], errors='coerce').fillna(0)
    return all_data


def insert_data_to_tables(accountsData, clientsData, transactionsData):
    with Session() as session:
        if not accountsData.empty:
            insert_account_data_to_table(session, accountsData)
        if not clientsData.empty:
            insert_client_data_to_table(session, clientsData)
        if not transactionsData.empty:
            insert_transaction_data_to_table(session, transactionsData)


def insert_account_data_to_table(session, accountsData):
    # Table data type mapping for accounts
    dtype_mapping = {
        'account_id': BigInteger,
        'client_id': String(50)
    }
    accountsData.to_sql('accounts', con=session.bind,
                        if_exists='append', index=False, dtype=dtype_mapping)


def insert_client_data_to_table(session, clientsData):
    # Table data type mapping for clients
    dtype_mapping = {
        'client_id': String(50),
        'client_name': String(50),
        'client_email': String(40),
        'client_birth_date': Date
    }
    clientsData.to_sql('clients', con=session.bind,
                       if_exists='append', index=False, dtype=dtype_mapping)


def insert_transaction_data_to_table(session, transactionsData):
    # Table data type mapping for transactions
    dtype_mapping = {
        'transaction_id': BigInteger,
        'timestamp': DateTime,
        'account_id': BigInteger,
        'amount': Numeric(10, 2),
        'type': String(5),
        'medium': String(10)
    }
    transactionsData.to_sql('transactions', con=session.bind,
                            if_exists='append', index=False, dtype=dtype_mapping)


def create_views():
    with Session() as session:
        create_view_client_transaction_counts(session)
        create_view_monthly_transaction_summary(session)
        create_view_high_transaction_accounts(session)


def create_view_client_transaction_counts(session):
    create_view_query = '''
        CREATE OR REPLACE VIEW client_transaction_counts AS
        SELECT c.client_id, COUNT(tr.transaction_id) AS transaction_count
        FROM clients AS c
        JOIN accounts AS a ON c.client_id = a.client_id
        JOIN transactions AS tr ON a.account_id = tr.account_id
        GROUP BY c.client_id
        ORDER BY c.client_id;
    '''
    session.execute(text(create_view_query))
    session.commit()


def create_view_monthly_transaction_summary(session):
    create_view_query = '''
        CREATE OR REPLACE VIEW monthly_transaction_summary AS
        SELECT TO_CHAR(DATE_TRUNC('month', timestamp), 'YYYY-MM-01') AS month,
               client_email, COUNT(tr.transaction_id) AS transaction_count,
               SUM(tr.amount) AS total_amount
        FROM transactions AS tr
        JOIN accounts AS a ON tr.account_id = a.account_id
        JOIN clients AS c ON c.client_id = a.client_id
        GROUP BY month, client_email
        ORDER BY month, client_email;
    '''
    session.execute(text(create_view_query))
    session.commit()


def create_view_high_transaction_accounts(session):
    create_view_query = '''
        CREATE OR REPLACE VIEW high_transaction_accounts AS
        SELECT TO_CHAR(DATE_TRUNC('month', timestamp), 'YYYY-MM-DD') AS date,
               account_id, COUNT(transaction_id) AS transaction_count
        FROM transactions
        GROUP BY date, account_id
        HAVING COUNT(transaction_id) > 2
        ORDER BY date, account_id;
    '''
    session.execute(text(create_view_query))
    session.commit()


if __name__ == "__main__":
    main()
