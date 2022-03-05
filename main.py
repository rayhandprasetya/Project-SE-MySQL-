import func
import os
from func import *


def Home():
    dbfunc = func.StudiKasus2

    print("What do you want to do?\n")
    print("1. Try Connect Database")
    print("2. Create Database")
    print("3. Load CSV file")
    print("4. Create table ")
    print("5. Load data from database")
    print("6. Manual User")

    choice = input("Your choice: ")

    def tryConnDB():
        dbfunc._init_(dbfunc, 'host', '3306', 'root', os.environ['MYSQL_PASSWORD'])
        dbfunc.connect_db(dbfunc)

    if choice == "1":
        tryConnDB()
        Home()

    elif choice == "2":
        tryConnDB()
        dbname = input("Input your new database name: ")
        dbfunc.create_db(dbfunc, dbname)

    elif choice == "3":
        tryConnDB()
        path = input("Input the path of CSV file: ")
        dbfunc.import_csv(dbfunc, path)
        print('CSV file imported!\n')
        Home()

    elif choice == "4":
        tryConnDB()
        dbname = input("Input the name of database: ")
        tbname = input("Input the name of table: ")
        dafe = dbfunc.imp_df()
        print(dafe)
        dbfunc.create_table(dbfunc, dbname, tbname, dafe)

    elif choice == "5":
        tryConnDB()
        dbname = input("Input the name of database: ")
        tbname = input("Input the name of table: ")
        dbfunc.load_data(dbfunc, dbname, tbname)

    elif choice == "6":
        help(StudiKasus2)
        
Home()