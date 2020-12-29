import psycopg2
import configparser
from sql_queries import *

DWH_ROLE_ARN = ""

def create_connection():
    """
    - Read configuration to get database parameters
    - Connects to the Redshift database
    - Returns the connection and cursor
    """
    
    global DWH_ROLE_ARN
    
    # read configuration
    config = configparser.ConfigParser()
    
    #Normally this file should be in ~/.aws/credentials
    config.read_file(open('../aws/credentials.cfg'))
    
    DWH_DB = config.get("DWH","DWH_DB")
    DWH_PORT = config.get("DWH","DWH_PORT")
    DWH_DB_USER = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH","DWH_DB_PASSWORD")
    
    DWH_ENDPOINT = config.get("RedShift","DWH_ENDPOINT")
    DWH_ROLE_ARN = config.get("RedShift","DWH_ROLE_ARN")
    
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

def load_data(cur):
    """Load data into Redshift tables."""
    global DWH_ROLE_ARN
    # Copy airport_codes
    print("- Current Table: airport_codes")
    cur.execute(airport_codes_copy.format(DWH_ROLE_ARN))
    
    # Copy code_country
    cur.execute(code_country_copy.format(DWH_ROLE_ARN))
    print("- Current Table: code_country")
    
    # Copy ports
    cur.execute(ports_copy.format(DWH_ROLE_ARN))
    print("- Current Table: ports")
    
    # Copy port_mode
    cur.execute(port_mode_copy.format(DWH_ROLE_ARN))
    print("- Current Table: port_mode")
    
    # Copy states
    cur.execute(states_copy.format(DWH_ROLE_ARN))
    print("- Current Table: states")
    
    # Copy visa_category
    cur.execute(visa_category_copy.format(DWH_ROLE_ARN))
    print("- Current Table: visa_category")
    
    # Copy world_temp
    cur.execute(world_temp_copy.format(DWH_ROLE_ARN))
    print("- Current Table: world_temp")
    
    # Copy us_cities_demographics
    cur.execute(us_cities_demographics_copy.format(DWH_ROLE_ARN))
    print("- Current Table: us_cities_demographics")
    
    # Copy i94_immigration
    cur.execute(i94_immigration_copy.format(DWH_ROLE_ARN))
    print("- Current Table: i94_immigration")
    
    # Insert i94_dates
    cur.execute(i94_dates_insert)
    print("- Current Table: i94_dates")
    

def main():
    """
    Load data to redshift.
    
    - Establishing connection
    - Load data into tables
    - Closing connection
    """
    print("- Establishing connection")
    cur, conn = create_connection()
    
    print("- Load data into tables")
    load_data(cur)

    print("- Closing connection")
    cur.close()
    conn.close()
    
    print("- All done")


if __name__ == "__main__":
    main()