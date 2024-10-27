import os
from cfonts import render
from dotenv import load_dotenv
import requests
import pandas as pd
import io
from sqlalchemy import create_engine, Column, Numeric, String, BigInteger, Date, DateTime, ForeignKey, text
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from concurrent.futures import ThreadPoolExecutor
import psycopg2
import time

# Load .env file to environment
load_dotenv()

# Environment variables
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
db_engine = create_engine(f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}', pool_size=10, max_overflow=20)
Session = sessionmaker(bind=db_engine)

# Define base for models
Base = declarative_base()

# Class Definitions
class Account(Base):
    __tablename__ = 'accounts'
    account_id = Column(BigInteger, primary_key=True)
    client_id = Column(String(50), ForeignKey('clients.client_id'), nullable=False)
    
    client = relationship("Client", back_populates="accounts")

class Client(Base):
    __tablename__ = 'clients'
    client_id = Column(String(50), primary_key=True)
    client_name = Column(String(50), nullable=False)
    client_email = Column(String(40), nullable=False)
    client_birth_date = Column(Date)
    
    accounts = relationship("Account", back_populates="client")

class Transaction(Base):
    __tablename__ = 'transactions'
    transaction_id = Column(BigInteger, primary_key=True)
    timestamp = Column(DateTime, nullable=False)
    account_id = Column(BigInteger, ForeignKey('accounts.account_id'), nullable=False)
    amount = Column(Numeric(10, 2), nullable=False)
    type = Column(String(5), nullable=False)
    medium = Column(String(10), nullable=False)

# Main function
def main() -> None:
    print(render('Hello ZYLYTY!', colors=['cyan', 'magenta'], align='center', font='3d'))
    
    print(render('Hello ZYLYTY!', colors=['cyan', 'magenta'], align='center', font='3d'))
    print(f"Admin API Key: {ADMIN_API_KEY}")
    print(f"Database Host: {DB_HOST}")
    print(f"Database Port: {DB_PORT}")
    print(f"Database Username: {DB_USERNAME}")
    print(f"Database Password: {DB_PASSWORD}")
    print(f"Database Name: {DB_NAME}")
    print(f"API Base URL: {API_BASE_URL}")

    
    # Create tables if they don't exist
    Base.metadata.create_all(db_engine)  

    # Import data using parallel processing
    with ThreadPoolExecutor() as executor:
        accountsFuture = executor.submit(etl_import_accounts)
        clientsFuture = executor.submit(etl_import_clients)
        transactionsFuture = executor.submit(etl_import_transactions)

        accountsData = accountsFuture.result()
        clientsData = clientsFuture.result()
        transactionsData = transactionsFuture.result()

    # Clean transactions data
    transactionsData = clean_transactions_data(transactionsData)

    totalAccounts = len(accountsData)
    totalClients = len(clientsData)
    totalTransactions = len(transactionsData)

    # Insert data into SQL tables
    with Session() as session:
        try:
            # Insert clients first
            print("Inserting clients...")
            insert_data_to_table(session, Client, clientsData)
            
            # Validate accounts before inserting
            valid_client_ids = set(clientsData['client_id'].unique())
            invalid_accounts = accountsData[~accountsData['client_id'].isin(valid_client_ids)]

            if not invalid_accounts.empty:
                print("Invalid accounts found:")
                print(invalid_accounts)
            else:
                #print("Inserting accounts...")
                insert_data_to_table(session, Account, accountsData)
            
            #print("Inserting transactions...")
            insert_data_to_table(session, Transaction, transactionsData)

            create_view_client_transaction_counts(session)
            create_view_monthly_transaction_summary(session)
            create_view_high_transaction_accounts(session)
            #print("Views created successfully!")
        except Exception as e:
            print(f"Error occurred: {e}")

    # Print summary after data import
    print(f"ZYLYTY Data Import Completed [{totalClients}, {totalAccounts}, {totalTransactions}]")

# ETL Functions
def etl_import_accounts() -> pd.DataFrame:
    accountsURL = "download/accounts.csv"
    accountsResponse = requests.get(f"{API_BASE_URL}/{accountsURL}", headers=headers).content
    accountsData = pd.read_csv(io.StringIO(accountsResponse.decode('utf-8')))
    return accountsData

def etl_import_clients() -> pd.DataFrame:
    clientsURL = "download/clients.csv"
    clientsResponse = requests.get(f"{API_BASE_URL}/{clientsURL}", headers=headers).content
    clientsData = pd.read_csv(io.StringIO(clientsResponse.decode('utf-8')))
    return clientsData

def etl_import_transactions() -> pd.DataFrame:
    transactionsURL = "transactions"
    transactionsData = get_transactions_paginated(f"{API_BASE_URL}/{transactionsURL}", headers)
    return transactionsData

# get transactions paginated
def get_transactions_paginated (url, headers):

    # Parameters for pagination
    page = 0
    results_per_page = 1000 # result per page
    max_pages = 300  # max pages
    max_retries = 5  # max number of attempts

    # Blank DataFrame for storing transactions
    all_data = pd.DataFrame()

    while page <= max_pages:
        # Request attempts
        attempts = 0
        success = False

        while attempts < max_retries and not success:
            try:
                # Get Request with Pagination
                params = {
                    "page": page,
                    "limit": results_per_page
                }
                response = requests.get(url, headers=headers, params=params, timeout=10)
                
                # Check if request was successful
                if response.status_code == 200:
                    data = response.json()
                    
                    # Check data type and process accordingly structure
                    if isinstance(data, list) and len(data) > 0:
                        page_data = pd.DataFrame(data)
                    elif isinstance(data, dict) and "results" in data and len(data["results"]) > 0:
                        page_data = pd.DataFrame(data["results"])
                    else:
                        #print("Nenhum dado adicional encontrado.")
                        break  # Stop looping If no more pages

                    # Adding data in main dataframe
                    all_data = pd.concat([all_data, page_data], ignore_index=True)

                    #print(f"Página {page} carregada com sucesso.")
                    success = True
                    page += 1  # Go to next page
                
                else:
                    #print(f"Erro na requisição: {response.status_code}")
                    attempts += 1
                    time.sleep(2)  # wait befere trying again

            except requests.exceptions.RequestException as e:
                #print(f"Erro de conexão na página {page}: {e}")
                attempts += 1
                time.sleep(2)  # wait befere trying again
        
        # If page fail after max attempts
        if not success:
            #print(f"Falha ao carregar a página {page} após {max_retries} tentativas.")
            break
    
    return all_data  
def clean_transactions_data(all_data: pd.DataFrame) -> pd.DataFrame:
    if not all_data.empty:
        all_data = all_data.drop_duplicates(subset=['timestamp', 'account_id'])
        all_data['amount'] = pd.to_numeric(all_data['amount'], errors='coerce').fillna(0)
    return all_data

# Insert data to SQL tables
def insert_data_to_table(session, model_class, data: pd.DataFrame) -> None:
    instances = [model_class(**record) for record in data.to_dict(orient='records')]
    session.bulk_save_objects(instances)
    session.commit()

# Create views
def create_view_client_transaction_counts(session) -> None:
    create_view_query = '''
        CREATE OR REPLACE VIEW client_transaction_counts AS
        SELECT c.client_id, COUNT(tr.transaction_id) AS transaction_count
        FROM clients c
        JOIN accounts a ON c.client_id = a.client_id
        JOIN transactions tr ON a.account_id = tr.account_id
        GROUP BY c.client_id
        ORDER BY c.client_id;
    '''
    session.execute(text(create_view_query))
    session.commit()

def create_view_monthly_transaction_summary(session) -> None:
    create_view_query = '''
        CREATE OR REPLACE VIEW monthly_transaction_summary AS
        SELECT to_char(date_trunc('month', timestamp), 'YYYY-MM-01') AS month, client_email,
        COUNT(transaction_id) AS transaction_count, SUM(amount) AS total_amount
        FROM transactions tr
        JOIN accounts a ON tr.account_id = a.account_id
        JOIN clients c ON c.client_id = a.client_id
        GROUP BY to_char(date_trunc('month', timestamp), 'YYYY-MM-01'), client_email
        ORDER BY month, client_email;
    '''
    session.execute(text(create_view_query))
    session.commit()

def create_view_high_transaction_accounts(session) -> None:
    create_view_query = '''
        CREATE OR REPLACE VIEW high_transaction_accounts AS
        SELECT to_char(date_trunc('month', timestamp), 'YYYY-MM-DD') AS date, account_id,
        COUNT(transaction_id) AS transaction_count
        FROM transactions tr
        GROUP BY to_char(date_trunc('month', timestamp), 'YYYY-MM-DD'), account_id
        HAVING COUNT(transaction_id) > 2
        ORDER BY date, account_id;
    '''
    session.execute(text(create_view_query))
    session.commit()

if __name__ == "__main__":
    main()