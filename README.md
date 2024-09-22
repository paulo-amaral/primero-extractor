# UNICEF PRIMERO API to PostgreSQL Data Ingestion Script

This Python script is designed to authenticate with the Primero API, fetch case data, and insert it into a PostgreSQL database. It handles token-based authentication and transforms the JSON response from the API into structured data for PostgreSQL.

<img width="987" alt="Screenshot 2024-09-21 at 00 23 20" src="https://github.com/user-attachments/assets/20f974ed-a33b-4664-a6d4-95f35a5e2032">

## Features

- **Token-Based Authentication**: The script uses the `/api/v2/tokens` endpoint to authenticate with the Primero API.
- **Data Fetching**: After successful authentication, the script fetches case data from the `/api/v2/cases` endpoint.
- **Database Insertion**: The data is then inserted into a PostgreSQL table. If the table doesn't already exist, it will be created automatically.

## Prerequisites

Before running this script, ensure you have:

- Python 3.x installed on your system.
- A running PostgreSQL instance, with a database where the data will be stored.
- Access to the Primero API with a valid username and password.

## Installation

Follow these steps to get started:

1. **Clone the repository**:
   ```bash
   git clone https://github.com/yourusername/repo-name.git
   cd repo-name

2. Install required dependencies: The script uses requests for handling API calls and psycopg2 for PostgreSQL interactions. Install them using pip:
   ```
   pip install requests psycopg2
   ```

## Configuration

### PostgreSQL Configuration

Before running the script, you need to configure your PostgreSQL database connection. Update the following connection parameters in the script:

```
DB_HOST = 'your_postgres_host'
DB_NAME = 'your_postgres_db_name'
DB_USER = 'your_postgres_user'
DB_PASSWORD = 'your_postgres_password'
DB_PORT = 'your_postgres_port'  # Default is 5432
```

Make sure you have an existing PostgreSQL database ready to store the data.

## API Authentication
You will also need to configure the Primero API credentials in the script:

```
API_USER = 'your_api_username'
API_PASSWORD = 'your_api_password'
```

These credentials will be used to authenticate and obtain an access token from the /api/v2/tokens endpoint.




## How the Script Works
1. ***Authentication***: The script sends a POST request to the `/api/v2/tokens`endpoint with the username and password in order to obtain an access token.

2. ***Fetching Case Data***: The access token is used in the Authorization header to make a GET request to the `/api/v2/cases` endpoint. This returns the JSON data containing case details.

3. ***Table Creation***: The script will automatically create a table in PostgreSQL if it doesn't already exist. The table structure is designed to match the fields retrieved from the API.

4. ***Inserting Data***: Once the table is ready, the script inserts the case data into the PostgreSQL database. It ensures no duplicate entries by using ON CONFLICT logic on the id field (which is the primary key).

## Usage

1. Update the Configuration: Ensure that you have updated the script with the correct PostgreSQL connection details and API credentials as described above.
2. Run the Script: Once everything is configured, run the script using Python:
```
python script_name.py
```
The script will:

- Authenticate with the Primero API.
- Fetch the case data.
- Create the required PostgreSQL table (if it doesn't exist).
- Insert the data into the table.
  
3. View Data in PostgreSQL: Once the script has successfully run, you can query the PostgreSQL database to view the inserted data:
   ```
   SELECT * FROM cases;
   ```
## Error Handling

Authentication Errors: If the script fails to authenticate, check that the API credentials (username and password) are correct.
Database Connection Issues: Ensure the PostgreSQL server is running and the connection parameters (host, port, etc.) are correct.
API Response Issues: If the script cannot fetch data, verify that the API endpoint is correct and accessible.
Contributing
Feel free to fork this repository, make changes, and submit pull requests. Contributions are welcome!



### Additional Notes:
- **Dependencies**: In the installation section, I mention using `pip` to install the dependencies (`requests`, `psycopg2`).
- **Configuration**: The README makes it clear where the user should update their PostgreSQL and API credentials.
- **Table Schema**: The table schema section helps users understand the structure of the data being inserted into the database.
- **Usage**: The usage section provides clear instructions on running the script.
- **Security**: Instead of storing sensitive data in the script or directly setting environment variables in the shell, you can store them in a `.env` file, which is never committed to your version control system (like Git). Use the `python-dotenv` package to load them into your Python script.





