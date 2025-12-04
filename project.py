import mysql.connector
import csv
import os
import sys

create_list = ["""CREATE TABLE User (
                        uid INT,
                        email TEXT NOT NULL,
                        username TEXT NOT NULL,
                        PRIMARY KEY (uid)
                        );""", 
                        """CREATE TABLE AgentCreator (
                        uid INT,
                        bio TEXT,
                        payout TEXT,
                        PRIMARY KEY (uid),
                        FOREIGN KEY (uid) REFERENCES User(uid) ON DELETE CASCADE
                        );""",
                        """CREATE TABLE AgentClient (
                        uid INT,
                        interests TEXT NOT NULL,
                        cardholder TEXT NOT NULL,
                        expire DATE NOT NULL,
                        cardno BIGINT NOT NULL,
                        cvv INT NOT NULL,
                        zip INT NOT NULL,
                        PRIMARY KEY (uid),
                        FOREIGN KEY (uid) REFERENCES User(uid) ON DELETE CASCADE
                        );""",
                        """CREATE TABLE BaseModel (
                            bmid INT,
                            creator_uid INT NOT NULL,
                            description TEXT NOT NULL,
                            PRIMARY KEY (bmid),
                            FOREIGN KEY (creator_uid) REFERENCES AgentCreator(uid) ON DELETE CASCADE
                        );""",
                        """CREATE TABLE CustomizedModel (
                            bmid INT,
                            mid INT,
                            PRIMARY KEY (bmid, mid),
                            FOREIGN KEY (bmid) REFERENCES BaseModel(bmid) ON DELETE CASCADE
                        );""",
                        """CREATE TABLE Configuration (
                        cid INT,
                        client_uid INT NOT NULL,
                        content TEXT NOT NULL,
                        labels TEXT NOT NULL,
                        PRIMARY KEY (cid),
                        FOREIGN KEY (client_uid) REFERENCES AgentClient(uid) ON DELETE CASCADE
                        );""",
                        """CREATE TABLE InternetService (
                        sid INT,
                        provider TEXT NOT NULL,
                        endpoints TEXT NOT NULL,
                        PRIMARY KEY (sid)
                        );""",
                        """CREATE TABLE LLMService (
                            sid INT,
                            domain TEXT,
                            PRIMARY KEY (sid),
                            FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
                        );""",
                        """CREATE TABLE DataStorage (
                            sid INT,
                            type TEXT,
                            PRIMARY KEY (sid),
                            FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
                        );""",
                        """CREATE TABLE ModelServices (
                            bmid INT NOT NULL,
                            sid INT NOT NULL,
                            version INT NOT NULL,
                            PRIMARY KEY (bmid, sid),
                            FOREIGN KEY (bmid) REFERENCES BaseModel(bmid) ON DELETE CASCADE,
                            FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
                        );""",
                        """CREATE TABLE ModelConfigurations (
                            bmid INT NOT NULL,
                            mid INT NOT NULL,
                            cid INT NOT NULL,
                            duration INT NOT NULL,
                            PRIMARY KEY (bmid, mid, cid),
                            FOREIGN KEY (bmid, mid) REFERENCES CustomizedModel(bmid, mid) ON DELETE CASCADE,
                            FOREIGN KEY (cid) REFERENCES Configuration(cid) ON DELETE CASCADE
                        );"""
                       ]

def data_base_connection():
    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="GreenOranges#3",
            database="cs122a"
        )
        return mydb
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

<<<<<<< HEAD
def import_data(folder_path):
    mydb = data_base_connection()

    if mydb is None:
        print("Fail")
        return
    
    cursor = mydb.cursor()

    try:
        # drop all existing tables
        drop_list = ["ModelConfigurations", "ModelServices", "DataStorage",
                     "LLMService", "Configuration", "CustomizedModel", 
                     "BaseModel", "AgentClient", "AgentCreator", 
                     "InternetService", "User"]
        
        for table in drop_list:
            cursor.execute(f"DROP TABLE IF EXISTS {table};")

        # recreate all tables
        for table in create_list:
            cursor.execute(table)

        mydb.commit()

        # make sure import order is correct
        import_order = ["User.csv", "AgentCreator.csv", "AgentClient.csv", "BaseModel.csv",
                        "CustomizedModel.csv", "Configuration.csv", "InternetService.csv", 
                        "LLMService.csv", "DataStorage.csv", "ModelServices.csv", "ModelConfigurations.csv"]
        # import data
        for f in import_order: # iterate through each file
            name, _ = os.path.splitext(f) # take away file extension
            table = name
            csv_path = os.path.join(folder_path, f) # csv path

            with open(csv_path, 'r', encoding = 'utf-8') as csvfile:
                csv_read = csv.reader(csvfile)
                next(csv_read) # skip header

                for r in csv_read: # iterate thru rows
                    arguments = ""
                    for _ in range(len(r)):
                        arguments += "%s,"
                    arguments = arguments[:-1] # remove last comma

                    insert = f"INSERT INTO {table} VALUES ({arguments})"
                    cursor.execute(insert, r)

        mydb.commit()
        print("Success")
    except mysql.connector.Error as err:
         print("Fail")
=======
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
        
>>>>>>> 61fd130ef449b149563fa225c2f561f3408ff1f0
