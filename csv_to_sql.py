import pandas as pd
import mysql.connector as msql
from mysql.connector import Error

irisData = pd.read_csv(
    'https://github.com/Muhd-Shahid/Write-Raw-File-into-Database-Server/raw/main/iris.csv', index_col=False)
irisData.head()

try:
    conn = msql.connect(host='localhost', user='root', password='admin123')
    if conn.is_connected():
        cursor = conn.cursor()
        # cursor.execute("CREATE DATABASE irisDB")
        cursor.execute("use irisDB;")
        record = cursor.fetchone()
        print("Connected to Database: ", record)
        cursor.execute('DROP TABLE IF EXISTS iris; ')
        print('Creating table...')
        cursor.execute(
            "CREATE TABLE iris (sepal_length FLOAT(2,1) NOT NULL, sepal_width FLOAT(2, 1) NOT NULL, petal_length "
            "FLOAT(2, 1) NOT NULL, petal_width FLOAT(2, 1), species CHAR(11) NOT NULL)")
        print("iris table is created....")
        for i, row in irisData.iterrows():
            sql = "INSERT INTO irisdb.iris VALUES (%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            print("Record inserted")
            # the connection is not auto committed by default, so we
            # must commit to save our changes
        conn.commit()

        #Executing Query
        sql = "SELECT * FROM iris; "
        cursor.execute(sql)

        #Fetch all the records
        result = cursor.fetchall()
        for i in result:
            print(i)

        print("irisDB database is created")
except Error as e:
    print("Error while connecting to MySQL", e)
