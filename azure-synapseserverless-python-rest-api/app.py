from flask import Flask, request, jsonify
import pyodbc
import os
from dotenv import load_dotenv


load_dotenv()

SERVER_NAME = os.getenv("SERVER_NAME")
DATABASE_NAME = os.getenv("DB_NAME")
USER_NAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

app = Flask(__name__)

driver = '{ODBC Driver 17 for SQL Server}'

# Function to execute SQL queries against Synapse Serverless SQL pool
def execute_sql(query):
    #add try catch with below 
    conn_str = f'Driver={driver};Server={SERVER_NAME};Database={DATABASE_NAME};Uid={USER_NAME};Pwd={PASSWORD};'
    try:
        conn = pyodbc.connect(conn_str)
        if conn is None:
            print("(***********************************)")
            print("Error: No connection to the database")
            print("(***********************************)")
            return None
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        conn.close()
        return rows
    except Exception as e:
        # Print the exception for debugging purposes
        print("An error occurred:", str(e))
        # Raise the exception again to propagate it to the caller
        raise e
    
# Example endpoint to retrieve data from an external table
@app.route('/api/data', methods=['POST'])
def get_data():
    # Retrieve parameters from the request body
    request_data = request.json

    # Extract conditions from the request data
    conditions = request_data.get('conditions', [])

    # Start building the base query
    query = 'SELECT TOP 10 * FROM flattened_transactions WHERE '

    condition_strings = []
    for condition in conditions:
        column_name = condition.get('column')
        if column_name == 'year':
            column_name = "YEAR(OriginalDateOfContract)"
        operator = condition.get('operator')
        value = condition.get('value')
        if column_name and operator and value:
            condition_strings.append(f"{column_name} {operator} '{value}'")

    query += ' AND '.join(condition_strings)
    
    print(query)
    # Execute the SQL query
    data = execute_sql(query)

    # Convert the data to a list of lists
    records_list = [list(record) for record in data]

    # Return the result as JSON
    return jsonify(records_list)

@app.route('/api/v1/', methods=['GET'])
def display_message():
    return jsonify({"message": "Server is up and running!"})


if __name__ == '__main__':
    app.run(debug=True)
    
    
