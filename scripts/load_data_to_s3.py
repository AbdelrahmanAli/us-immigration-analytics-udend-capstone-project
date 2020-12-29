import boto3
import configparser
import os

KEY = ""
SECRET = ""
BUCKET_NAME = ""

def read_config():
    """Reading Configuration."""
    print("- Reading Configuration")
    global config, KEY, SECRET, BUCKET_NAME
    config = configparser.ConfigParser()
    
    #Normally this file should be in ~/.aws/credentials
    config.read_file(open('../aws/credentials.cfg'))
    
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    
    BUCKET_NAME            = config.get("S3", "BUCKET_NAME")
        
    return config

def upload_directory(s3,path):
    """Upload a directory with all it's files to AWS S3."""
    global BUCKET_NAME
    print("Source file path = "+path)
    for root,dirs,files in os.walk(path):
        if root[0:3] != '../' and (root[0] == '.' or "/." in root):
            continue
        print("======================")
        print("* Current root: "+root)
        print("---------------------")
        for file in files:
            if file[0] == '_' or file[0] == '.':
                continue
            print("Current file: "+file)
            s3.meta.client.upload_file(os.path.join(root,file),BUCKET_NAME,root.replace("../","")+"/"+file)
        print("======================")

def start_uploading(s3):
    """Start the process of uploading the required directories to AWS S3."""
    print("- Uploading airport_codes")
    upload_directory(s3,"../airport_codes")
    
    print("- Uploading lookups")
    upload_directory(s3,"../lookups")
    
    print("- Uploading world_temp")
    upload_directory(s3,"../world_temp")
    
    print("- Uploading us_cities_demographics")
    upload_directory(s3,"../us_cities_demographics")
    
    print("- Uploading i94_immigration")
    upload_directory(s3,"../i94_immigration")
        
def main():
    """
    - Read configuration
    - Connect to AWS S3 using boto
    - Initiate uploading
    """

    config = read_config()
    
    s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )
    
    print("- Start uploading")
    start_uploading(s3)
    
    print("- All done")


if __name__ == "__main__":
    main()