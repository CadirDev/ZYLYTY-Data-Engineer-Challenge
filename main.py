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

# Note 2: You must read from the follpuowing environment variables:
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
Session = sessionmaker(bind=db_engine)

# Use a session to execute command
session = Session()

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
    # You can import the data here from API_BASE_URL, using the ADMIN_API_KEY!
    # (...)
    
    totalAccounts = 0
    totalClients = 0
    totalTransactions = 0
    
    #Get accounts data from ETL
    accountsData = etl_import_accounts()
    #Get clients data from ETL
    clientsData = etl_import_clients()
    #Get transacitons data from ETL
    transactionsData = etl_import_transactions()
    
    #clean transactions data
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
        #Create view client_transaction_counts
        create_View_Client_Transaction_Counts()
        #Create view monthly_transaction_summary 
        create_View_Monthly_Transaction_Summary()
        #Create view high_transaction_accounts  
        create_View_High_Transaction_Accounts()
        # print("View criada com sucesso!")
    except Exception as e:
        print(f"Error occured: {e}")
    finally:
        session.close()  # Close session  
    
    # Don't forget to print the following string after you import all the necessary data:
    print(f"ZYLYTY Data Import Completed [{totalClients}, {totalAccounts}, {totalTransactions}]")

#import accounts data from API
def etl_import_accounts():
    # Accounts Endpoint
    accountsURL = "download/accounts.csv"
    
    payloadAccount = {}

    #Accounts.csv
    accountsResponse = requests.request("GET", f"{API_BASE_URL}/{accountsURL}", headers=headers, data=payloadAccount).content

    #Convert Accounts Data to Pandas DataFrame
    accountsData = pd.read_csv(io.StringIO(accountsResponse.decode('utf-8')))
    
    return accountsData

def etl_import_clients():
    # Clients Endpoint
    clientsURL = "download/clients.csv"
    
    payloadClients = {}
    
    #Clients.csv
    clientsResponse = requests.request("GET", f"{API_BASE_URL}/{clientsURL}", headers=headers, data=payloadClients).content
    
    #Convert Clients Data to Pandas DataFrame
    clientsData = pd.read_csv(io.StringIO(clientsResponse.decode('utf-8')))
    
    return clientsData

def etl_import_transactions():
    # Transactions Endpoint
    transactionsURL = "transactions"
    
    #Get Transactions Data in Pandas DataFrame
    transactionsData = get_Transactions_Paginated(f"{API_BASE_URL}/{transactionsURL}",headers)
    
    return transactionsData

   
# get transactions paginated
def get_Transactions_Paginated (url, headers):

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


def clean_Transactions_Data(all_data):
    # Cleaning Data
    if not all_data.empty:

        # Remove invalid transaction_id "N/A"
        all_data = all_data.dropna(subset=['transaction_id'])
        # Remove duplicate transaction_id
        all_data = all_data.drop_duplicates(subset=['transaction_id','account_id'])
        
        # Change non numeric amount to 0
        #all_data['amount'] = pd.to_numeric(all_data['amount'], errors='coerce')  # Convert to numeric, where is invalid set NaN
        #all_data = all_data.dropna(subset=['amount'])  # Remove Remove NaN Rows
        # Converte valores não numéricos para NaN e substitui NaN por 0
        all_data['amount'] = pd.to_numeric(all_data['amount'], errors='coerce').fillna(0)
    
    return all_data

# Insert Account Data imported to SQL tables
def insert_Account_Data_to_Table(accountsData):
    # Table data type mapping
    dtype_mapping = {
    'account_id': BigInteger,
    'client_id': String(255)
    }
    
    # Insert accounts data to accounts SQL Table
    accountsData.to_sql('accounts', db_engine, if_exists='replace', index=False, dtype=dtype_mapping)

# Insert Client Data imported to SQL tables
def insert_Client_Data_to_Table(clientsData): 
    # Table data type mapping
    dtype_mapping = {
    'client_id': String(255),
    'client_name': String(50),
    'client_email': String(255),
    'client_birth_date': Date
    }
    
    # Insert accounts data to clients SQL Table
    clientsData.to_sql('clients', db_engine, if_exists='replace', index=False, dtype=dtype_mapping)
    
# Insert Transaction Data imported to SQL tables
def insert_Transaction_Data_to_Table(transactionsData):
    # Table data type mapping
    dtype_mapping = {
    'transaction_id': BigInteger,
    'timestamp': DateTime,
    'account_id': BigInteger,
    'amount': Numeric(17,2),
    'type': String(5),
    'medium': String(30)
    }
    # Insert accounts data to transactions SQL Table
    transactionsData.to_sql('transactions', db_engine, if_exists='replace', index=False, dtype=dtype_mapping)

#Create view called client_transaction_counts
def create_View_Client_Transaction_Counts():
    
    # SQL Command to Create View
    create_view_query = '''
            CREATE OR REPLACE VIEW client_transaction_counts AS
            SELECT c.client_id, count(transaction_id) as transaction_count
            FROM clients as c
            inner join accounts as a on c.client_id=a.client_id
            inner join transactions as tr on a.account_id=tr.account_id
            group by c.client_id
            order by c.client_id
            ;
        '''
  
    session.execute(text(create_view_query))  
    session.commit()  # Commit transaction    


#Create view called monthly_transaction_summary 
def create_View_Monthly_Transaction_Summary():
    
    # SQL Command to Create View
    create_view_query = '''
            CREATE OR REPLACE VIEW monthly_transaction_summary AS
            SELECT to_char(date_trunc('month', timestamp), 'YYYY-MM-01') as month, client_email,
            count(transaction_id) as transaction_count, sum(amount) as total_amount
            from transactions as tr
            inner join accounts as a on tr.account_id=a.account_id
            inner join clients as c on c.client_id=a.client_id
            group by to_char(date_trunc('month', timestamp), 'YYYY-MM-01'), client_email
            order by month, client_email
            ;
        '''
    
    session.execute(text(create_view_query))  
    session.commit()  # Commit transaction
  

#Create view called high_transaction_accounts  
def create_View_High_Transaction_Accounts():
    
    # SQL Command to Create View
    create_view_query = '''
            CREATE OR REPLACE VIEW high_transaction_accounts AS
           SELECT to_char(date_trunc('month', timestamp), 'YYYY-MM-DD') as date, account_id,
            count(transaction_id) as transaction_count
            from transactions as tr
            group by to_char(date_trunc('month', timestamp), 'YYYY-MM-DD'), account_id
            having count(transaction_id) > 2
            order by date, account_id
            ;
        '''

    session.execute(text(create_view_query))  
    session.commit()  # Commit transaction
       

if __name__ == "__main__":
    main()
