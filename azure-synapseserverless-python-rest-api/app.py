from flask import Flask, jsonify
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
@app.route('/api/data', methods=['GET'])
def get_data():
    query = 'SELECT TOP 10 * FROM deed_sales '
    data = execute_sql(query)
    records_list = [list(record) for record in data]
    # json_records = json.dumps(records_list)
    return jsonify(data=records_list)

@app.route('/api/v1/', methods=['GET'])
def display_message():
    return jsonify({"message": "Server is up and running!"})


if __name__ == '__main__':
    app.run(debug=True)
    
    
