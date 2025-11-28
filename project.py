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

