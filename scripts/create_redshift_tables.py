import psycopg2
import configparser
from sql_queries import create_table_queries, drop_table_queries


def create_connection():
    """
    - Read configuration to get database parameters
    - Connects to the Redshift database
    - Returns the connection and cursor
    """
    
    # read configuration
    config = configparser.ConfigParser()
    
    #Normally this file should be in ~/.aws/credentials
    config.read_file(open('../aws/credentials.cfg'))
    
    DWH_DB = config.get("DWH","DWH_DB")
    DWH_PORT = config.get("DWH","DWH_PORT")
    DWH_DB_USER = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH","DWH_DB_PASSWORD")
    
    DWH_ENDPOINT = config.get("RedShift","DWH_ENDPOINT")
    
    # connect to default database
    try:
        conn=psycopg2.connect(dbname= DWH_DB, host=DWH_ENDPOINT, port= DWH_PORT, user= DWH_DB_USER, password= DWH_DB_PASSWORD)
    except psycopg2.Error as e: 
        print("Error: Could not make connection to the Redshift database")
        print(e)
    
    conn.set_session(autocommit=True)
    
    try:
        cur = conn.cursor()
    except psycopg2.Error as e: 
        print("Error: Could not get curser to the Database")
        print(e)

    return cur, conn


def drop_tables(cur):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)


def create_tables(cur):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)


def main():
    """
    - Establishes connection with the redshift database and gets a cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    print("- Establishing connection")
    cur, conn = create_connection()
    
    print("- Dropping tables")
    drop_tables(cur)
    
    print("- Creating tables")
    create_tables(cur)

    print("- Closing connection")
    cur.close()
    conn.close()
    
    print("- All done")


if __name__ == "__main__":
    main()