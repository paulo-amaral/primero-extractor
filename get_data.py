### Author: Paulo Amaral
### Date: 22/09/2024

import requests
import psycopg2
import json
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# PostgreSQL connection parameters
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_NAME = os.getenv('DB_NAME', 'primero')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')
DB_PORT = os.getenv('DB_PORT', '5432')

# API Authentication details
API_TOKEN_URL = os.getenv('API_TOKEN_URL', 'https://demo-tl.primero.org/api/v2/tokens')
API_CASES_URL = os.getenv('API_CASES_URL', 'https://demo-tl.primero.org/api/v2/cases')
API_USER = os.getenv('API_USER', '')
API_PASSWORD = os.getenv('API_PASSWORD', '')

def authenticate():
    """
    Function to authenticate and get the token from the /tokens endpoint
    """
    auth_payload = {
        "user": {
            "user_name": API_USER,
            "password": API_PASSWORD
        }
    }
    
    # Send POST request for authentication
    response = requests.post(API_TOKEN_URL, json=auth_payload)
    if response.status_code != 200:
        raise AssertionError(f"Authentication failed: {response.status_code}")
    
    auth_data = response.json()
    token = auth_data.get('token')
    print("Authentication successful. Token received.")
    return token
    
    # Send POST request for authentication
    # response = requests.post(API_TOKEN_URL, json=auth_payload)
    # if response.status_code !== 200:
    #     raise AssertionError("Authentication failed: %s", response.status_code)
    #     auth_data = response.json()
    #     token = auth_data.get('token')
    #     print("Authentication successful. Token received.")
    # return token

# Function to fetch cases from API using the token with pagination
def fetch_data_from_api(token):
    headers = {
        'Authorization': f'Bearer {token}'
    }
    
    all_cases = []
    page = 1
    
    while True:
        response = requests.get(f"{API_CASES_URL}?per=1000&page={page}", headers=headers)
        
        if response.status_code != 200:
            print(f"Error fetching data: {response.status_code}")
            break
        
        data = response.json()
        cases = data.get('data', [])
        all_cases.extend(cases)
        
        # Check if we have more pages to fetch
        total_records = data['metadata']['total']
        per_page = data['metadata']['per']
        total_pages = (total_records + per_page - 1) // per_page
        
        if page >= total_pages:
            break
        
        page += 1  # Go to the next page
    
    return all_cases

# Function to connect to PostgreSQL
def connect_to_postgres():
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        print("Connected to PostgreSQL")
        return connection
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

# Function to create the necessary table (if it does not exist)
def create_table(connection):
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS cases (
        id VARCHAR(255) PRIMARY KEY,
        enabled BOOLEAN,
        age INT,
        sex VARCHAR(50),
        name VARCHAR(255),
        status VARCHAR(100),
        case_id VARCHAR(255),
        owned_by VARCHAR(255),
        short_id VARCHAR(50),
        workflow VARCHAR(100),
        created_at TIMESTAMP,
        created_by VARCHAR(255),
        last_updated_at TIMESTAMP,
        last_updated_by VARCHAR(255),
        nationality VARCHAR(255),
        registration_date DATE
    );
    '''
    cursor = connection.cursor()
    cursor.execute(create_table_query)
    connection.commit()
    cursor.close()

# Function to insert data into PostgreSQL 
def insert_data_into_postgres(connection, cases):
    insert_query = '''
    INSERT INTO cases (id, enabled, age, sex, name, status, case_id, owned_by, short_id, workflow, 
                       created_at, created_by, last_updated_at, last_updated_by, nationality, registration_date)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING;
    '''
    
    cursor = connection.cursor()
    
    for case in cases:
        case_data = (
            case.get('id'),
            case.get('enabled'),
            case.get('age'),
            case.get('sex'),
            case.get('name'),
            case.get('status'),
            case.get('case_id'),
            case.get('owned_by'),
            case.get('short_id'),
            case.get('workflow'),
            case.get('created_at'),
            case.get('created_by'),
            case.get('last_updated_at'),
            case.get('last_updated_by'),
            ','.join(case.get('nationality', [])),  # Join nationality list into a string
            case.get('registration_date')
        )
        
        cursor.execute(insert_query, case_data)
    
    connection.commit()
    cursor.close()

# Main function to orchestrate the process
def main():
    # Step 1: Authenticate to get the token
    token = authenticate()
    
    if token is None:
        print("Authentication failed. Exiting...")
        return
    
    # Step 2: Fetch data from API using the token
    cases_data = fetch_data_from_api(token)
    
    if not cases_data:
        print("No data fetched. Exiting...")
        return
    
    # Step 3: Connect to PostgreSQL
    connection = connect_to_postgres()
    
    if connection is None:
        print("Failed to connect to PostgreSQL. Exiting...")
        return
    
    try:
        # Step 4: Create table (if not exists)
        create_table(connection)
        
        # Step 5: Insert fetched data into PostgreSQL
        insert_data_into_postgres(connection, cases_data)
        print("Data inserted successfully!")
    
    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Step 6: Close the connection
        if connection:
            connection.close()

if __name__ == '__main__':
    main()
