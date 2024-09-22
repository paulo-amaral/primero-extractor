### Author: Paulo Amaral
### Date: 22/09/2024


import requests
import psycopg2
import json

# PostgreSQL connection parameters
DB_HOST = 'localhost'
DB_NAME = 'primero'
DB_USER = 'postgres'
DB_PASSWORD = ''
DB_PORT = ''  # Default is 5432

# API Authentication details
API_TOKEN_URL = 'https://demo-tl.primero.org/api/v2/tokens'
API_CASES_URL = 'https://demo-tl.primero.org/api/v2/cases?per=100000'
API_USER = ''
API_PASSWORD = ''

# Function to authenticate and get the token from the /tokens endpoint
def authenticate():
    auth_payload = {
        "user": {
            "user_name": API_USER,
            "password": API_PASSWORD
        }
    }
    
    # Send POST request for authentication
    response = requests.post(API_TOKEN_URL, json=auth_payload)
    
    if response.status_code == 200:
        # Assuming the response contains the authentication token
        auth_data = response.json()
        token = auth_data.get('token')  # Adjust this if the token is under a different field
        print("Authentication successful. Token received.")
        return token
    else:
        print(f"Authentication failed: {response.status_code}")
        return None

# Function to fetch cases from API using the token
def fetch_data_from_api(token):
    headers = {
        'Authorization': f'Bearer {token}'
    }
    
    response = requests.get(API_CASES_URL, headers=headers)
    
    if response.status_code == 200:
        return response.json()['data']  # Extract the 'data' field containing cases
    else:
        print(f"Error fetching data: {response.status_code}")
        return None

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

# Function to create the necessary table (if it does not exist) - Please adjust it to get more data
# Please adjust to reflect PRIMERO Data tables to import
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
        # Extract the necessary fields
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
            ','.join(case.get('nationality', [])),  # Join the nationality list into a comma-separated string (Cgeck table first)
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
    
    if cases_data is None:
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
