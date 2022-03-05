import mysql.connector as mysql
from mysql.connector import Error
import sqlalchemy
import os
import pandas as pd

class StudiKasus2:
    """
    --------------------------------------------
    Initiaze this class with some variable like host, port and user
    to connect to the database.
    mysql.connect is the built-in function to try connect to the DB
    with the host, port, user, and password parameter from def _init_
    """
    def _init_(self, host, port, user, password):
        self.host = 'localhost' #Fill with your host name
        self.port = '3306' #Fill with your port number
        self.user = 'root' #Fill with your user name
        self.password = os.environ['MYSQL_PASSWORD'] #Fill with your password or local variable name

        self.conn = mysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.password
        )

    def connect_db(self):
        """
        -----------------------
        Call for test has this program been connected to the database.
        """
        try:
            if self.conn.is_connected():
                cursor = self.conn.cursor()
                print("Database connected!")
                # If the connection is successfull will print "Database Connected"
        except Error as e:
            # If can't connect to the database, it will print this error message
            print("Error while connecting to MySQL", e)
    
    def create_db(self, db_name):
        """
        --------------------------
        Function to ceate database,
        db_name is the parameter you can input in it and will be
        the name of your database.
        """
        try:
            if self.conn.is_connected():
                cursor = self.conn.cursor()
                cursor.execute("CREATE DATABASE {}".format(db_name))
                print('Database ', db_name, ' created!')
        except Error as e:
            print("Error while connecting to MySQL", e)

    def import_csv(self, path):
        """
        -------------------------------
        NOTE: CALL THIS FUNCTION FIRSTLY BEFORE YOU CREATE THE TABLE ON DATABASE
        Fill the path attribute with your path file of the csv file.
        """
        global df
        df = pd.read_csv(path, index_col=False, delimiter=',', encoding='latin1')
    
    def imp_df():
        """
        ----------
        Function to get the data file (df) from import_csv() function.
        """
        return df

    def create_table(self, db_name, table_name, df):
        """
        ------------------------------
        Call it to create a new table.
        Fill db_name and table_name with the database you will use and table name you want.
        The df attribute will automatically filled since imp_df() function called before this function.
        NOTE:
        "engine" variable here is the part of sqlalchemy.
        Sqlalchemy is the engine to connect between Python and the database so the user
        can perform some SQL statement.
        Look at "engine_stmt", there is statement 'mysql+mysqlconnector', this is the DBAPI
        or “Python Database API Specification” with purpose to connect Python application can talk with database.
        If you use Postgres, Microsoft SQL Server, or another database engine, you should change the statement. 
        'mysql+mysqlconnector' is DBAPI for MariaDB, usually for who use the XAMPP.
        
        Look at https://docs.sqlalchemy.org/en/14/core/engines.html for other information.
        """
        try:
            if self.conn.is_connected():
                cursor = self.conn.cursor()
                cursor.execute("USE {}".format(db_name))
                cursor.execute("CREATE TABLE {}".format(table_name))
        except Error as e:
            print("Error while connecting to MySQL", e)

        engine_stmt = 'mysql+mysqlconnector://%s:%s@%s:%s/%s' % (self.user, self.password,
                                                            self.host, self.port, db_name)
        engine = sqlalchemy.create_engine(engine_stmt)
                
        df.to_sql(name=table_name, con=engine,
                  if_exists='append', index=False, chunksize=1000)

    
    def load_data(self, db_name, table_name):
        """
        -------------------------------------
        Call it to show all data of the table.
        Fill db_name and table_name with the database you will use and table name you want.
        It is like you use SELECT * FROM TABLE in MySQL programming.
        """
        try:
            if self.conn.is_connected():
                cursor = self.conn.cursor()
                cursor.execute("SELECT * FROM {}.{}".format(db_name, table_name))
                result = cursor.fetchall()
                return result
        except Error as e:
            print("Error while connecting to MySQL", e)