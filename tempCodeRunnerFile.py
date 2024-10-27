# Insert Account Data imported to SQL tables
def insert_Account_Data_to_Table(accountsData):
    # Table data type mapping
    dtype_mapping = {
    'account_id': BigInteger,
    'client_id': String(55)
    }
    
    # Insert accounts data to accounts SQL Table
    accountsData.to_sql('accounts', db_engine, if_exists='replace', index=False, dtype=dtype_mapping)

# Insert Client Data imported to SQL tables
def insert_Client_Data_to_Table(clientsData): 
    # Table data type mapping
    dtype_mapping = {
    'client_id': String(40),
    'client_name': String(40),
    'client_email': String(30),
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
    'amount': Numeric(10,2),
    'type': String(5),
    'medium': String(10)
    }
    # Insert accounts data to transactions SQL Table
    transactionsData.to_sql('transactions', db_engine, if_exists='replace', index=False, dtype=dtype_mapping)