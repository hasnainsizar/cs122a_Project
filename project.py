import mysql.connector
import csv
import os
import sys

def data_base_connection():
    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="test",
            password="password",
            database="cs122a"
        )
        return mydb
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

def execute_sql_file(cursor, file_path: str):
    with open(file_path, 'r') as file:
        sql = file.read()
        sql_commands = sql.split(';')
        for command in sql_commands:
            command = command.strip()
            if command:
                cursor.execute(command)

def import_d(folder_name: str) -> bool:
    mydb = data_base_connection()
    if mydb is None:
        return False
    cursor = mydb.cursor()
    
    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
        drop = [
            "ModelConfigurations",
            "ModelServices",
            "DataStorage",
            "LLMService",
            "Configuration",
            "CusomizedModel",
            "BaseModel",
            "InternetService",
            "AgentClient",
            "AgentCreator",
            "User"
        ]
        for table in drop:
            cursor.execute(f"DROP TABLE IF EXISTS {table};")
        execute_sql_file(cursor, 'schema.sql')
        cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
        mydb.commit()

        csv_files = [
            ("User", "User.csv"),
            ("AgentCreator", "AgentCreator.csv"),
            ("AgentClient", "AgentClient.csv"),
            ("BaseModel", "BaseModel.csv"),
            ("CusomizedModel", "CustomizedModel.csv"),
            ("Configuration", "Configuration.csv"),
            ("InternetService", "InternetService.csv"),
            ("LLMService", "LLMService.csv"),
            ("DataStorage", "DataStorage.csv"),
            ("ModelServices", "ModelServices.csv"),
            ("ModelConfigurations", "ModelConfigurations.csv")
        ]

        for table, file_name in csv_files:
            file_path = os.path.join(folder_name, file_name)
            if not os.path.isfile(file_path):
                print(f"File {file_path} does not exist.")
                continue
            with open(file_path, 'r') as csvfile:
                reader = csv.reader(csvfile)
                headers = next(reader, None)
                placeholders = ', '.join(['%s'] * len(headers))
                insert_query = f"INSERT INTO {table} ({', '.join(headers)}) VALUES ({placeholders})"
                for row in reader:
                    row = [val if val != '' else None for val in row]
                    cursor.execute(insert_query, row)
        mydb.commit()
        return True
    except Exception as err:
        print(f"Import Error: {err}")
        mydb.rollback()
        return False
    finally:
        cursor.close()
        mydb.close()

# Question 2:
def insert_agent_client(uid: int, username: str, email:str, card_number: int, card_holder: str, expire: str, cvv: int, zip_code: int, interests: str) -> bool:
    mydb = data_base_connection()
    if mydb is None:
        return False
    cursor = mydb.cursor()
    try:
        