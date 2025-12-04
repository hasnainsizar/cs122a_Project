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
            user="test",
            password="password",
            database="cs122a"
        )
        return mydb
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

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

def main():
    if len(sys.argv) != 3:
        print("Usage: python project.py <folder_path>")
        return

    command = sys.argv[1]
    folder_path = sys.argv[2]
    if command == "import":
        import_data(folder_path)
    else:
        print("Unknown command. Use 'import' to import data.")

if __name__ == "__main__":
    main()