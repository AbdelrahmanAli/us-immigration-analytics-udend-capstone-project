import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import configparser

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
    
    engine = create_engine('postgresql://'+DWH_DB_USER+':'+DWH_DB_PASSWORD+'@'+DWH_ENDPOINT+':'+DWH_PORT+'/'+DWH_DB)

    return engine

def get_count(table_name,engine):
    """Get the count of records in the table."""
    counter_pdf = pd.read_sql('SELECT count(*) as counter FROM '+table_name, engine)
    return counter_pdf.iloc[0]['counter']

def get_duplicate_pks_count(table_name, pks,engine):
    """Get the count of duplicated records."""
    pks_pdf = pd.read_sql("""
                        SELECT count(*) as counter 
                        FROM (
                            SELECT """+pks+""", count(*)
                            FROM """+table_name+"""
                            GROUP BY """+pks+"""
                            HAVING count(*) > 1
                        )
                        """, engine)
    return pks_pdf.iloc[0]['counter']

def check_count(engine):
    """Check if each table has the required count."""
    assert (get_count('airport_codes',engine) == 55075), "Error: airport_codes is not 55075"
    assert (get_count('code_country',engine) == 289), "Error: code_country is not 289"
    assert (get_count('ports',engine) == 661), "Error: ports is not 661"
    assert (get_count('port_mode',engine) == 4), "Error: port_mode is not 4"
    assert (get_count('states',engine) == 55), "Error: states is not 55"
    assert (get_count('visa_category',engine) == 3), "Error: visa_category is not 3"
    assert (get_count('world_temp',engine) == 3490), "Error: world_temp is not 3490"
    assert (get_count('us_cities_demographics',engine) == 596), "Error: us_cities_demographics is not 596"
    assert (get_count('i94_immigration',engine) == 40790529), "Error: i94_immigration is not 40790529"
    assert (get_count('i94_dates',engine) == 732), "Error: i94_dates is not 732"
    
def check_duplicates(engine):
    """Check if any duplicate exist."""
    assert (get_duplicate_pks_count('airport_codes','ident',engine) == 0), "Error: duplicate PKs in airport_codes"
    assert (get_duplicate_pks_count('code_country','code',engine) == 0), "Error: duplicate PKs in code_country"
    assert (get_duplicate_pks_count('ports','code',engine) == 0), "Error: duplicate PKs in ports"
    assert (get_duplicate_pks_count('port_mode','code',engine) == 0), "Error: duplicate PKs in port_mode"
    assert (get_duplicate_pks_count('states','state_code',engine) == 0), "Error: duplicate PKs in states"
    assert (get_duplicate_pks_count('visa_category','code',engine) == 0), "Error: duplicate PKs in visa_category"
    assert (get_duplicate_pks_count('world_temp','city,country',engine) == 0), "Error: duplicate PKs in world_temp"
    assert (get_duplicate_pks_count('us_cities_demographics','city,state',engine) == 0), "Error: duplicate PKs in us_cities_demographics"
    assert (get_duplicate_pks_count('i94_immigration','cicid,i94yr,i94mon',engine) == 0), "Error: duplicate PKs in i94_immigration"
    assert (get_duplicate_pks_count('i94_dates','full_date',engine) == 0), "Error: duplicate PKs in i94_dates"

def main():
    """
    - Establishing connection
    - Perform table count validation
    - Perform duplicate check validation
    """

    print("- Establishing connection")
    engine = create_connection()
    
    print("- Checking count ... ")
    check_count(engine)
    
    print("- Checking duplicates ... ")
    check_duplicates(engine)

    
    print("- All done")


if __name__ == "__main__":
    main()