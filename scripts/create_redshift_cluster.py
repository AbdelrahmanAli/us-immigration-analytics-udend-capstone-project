import boto3
import configparser
import json
import os
import time

KEY = ""
SECRET = ""

DWH_CLUSTER_TYPE = ""
DWH_NUM_NODES = ""
DWH_NODE_TYPE = ""

DWH_CLUSTER_IDENTIFIER = ""
DWH_DB = ""
DWH_DB_USER = ""
DWH_DB_PASSWORD = ""
DWH_PORT = ""

DWH_IAM_ROLE_NAME = ""

DWH_ENDPOINT = ""
DWH_ROLE_ARN = ""

def read_config():
    """Reading Configuration."""
    print("- Reading Configuration")
    global config, KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME
    config = configparser.ConfigParser()
    
    #Normally this file should be in ~/.aws/credentials
    config.read_file(open('../aws/credentials.cfg'))
    
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
    
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")
    
    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
    
    return config
    
def create_iam_role(iam):
    """Creating a new IAM Role."""
    try:
        print('- Creating a new IAM Role')
        global DWH_IAM_ROLE_NAME
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
        return dwhRole
    except Exception as e:
        print(e)
        
def create_redshift_cluster(redshift,roleArn):
    """Creating RedShift Cluster."""
    try:
        print("- Creating RedShift Cluster")
        global DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD
        response = redshift.create_cluster(        
           # Add parameters for hardware
           ClusterType=DWH_CLUSTER_TYPE,
           NodeType=DWH_NODE_TYPE,
           NumberOfNodes=int(DWH_NUM_NODES),
           
           # Add parameters for identifiers & credentials
           DBName=DWH_DB,
           ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
           MasterUsername=DWH_DB_USER,
           MasterUserPassword=DWH_DB_PASSWORD,
           
           # Add parameter for role (to allow s3 access)
           IamRoles=[roleArn] 
       )
    except Exception as e:
        print(e)
        
def get_cluster_props(redshift):
    """Wait for cluster to be available and return cluster properties."""
    print("- Waiting for the cluster to be available ...")
    global DWH_CLUSTER_IDENTIFIER
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    # Busy wait until the cluster is created
    while myClusterProps["ClusterStatus"] == "creating":
        time.sleep(30) # Sleep 30 sec
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    print("- Cluster is now available")
    return myClusterProps

def set_endpoint_rolearn(config):
    """Setting ENDPOINT and ROLE_ARN in configuration file."""
    print("- Setting ENDPOINT and ROLE_ARN in configuration file")
    global DWH_ENDPOINT, DWH_ROLE_ARN
    # set new value
    config.set('RedShift', 'DWH_ENDPOINT', DWH_ENDPOINT)
    config.set('RedShift', 'DWH_ROLE_ARN', DWH_ROLE_ARN)

    # save the file
    with open('../aws/credentials.cfg', 'w') as configfile:
        config.write(configfile)

def enable_cluster_access(ec2,myClusterProps):
    """Opening TCP port to access the cluster."""
    print("- Opening TCP port to access the cluster")
    # Open an incoming TCP port to access the cluster endpoint
    try:
        global DWH_PORT
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)

def main():
    """
    Create RedShift Cluster.
    
    - Read the configuration
    - Create a Redshift Cluster
    - Set DWH_ENDPOINT and DWH_ROLE_ARN in the configuration file
    - Enable incoming TCP ports to access the cluster
    """
    
    config = read_config()
    
    global DWH_IAM_ROLE_NAME, DWH_ENDPOINT, DWH_ROLE_ARN
    
    #Define AWS resources
    ec2 = boto3.resource('ec2',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                        )
    iam = boto3.client('iam',aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET,
                         region_name='us-west-2'
                      )
    redshift = boto3.client('redshift',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                           )
    # Creating IAM role
    dwhRole = create_iam_role(iam)
    
    # Attach Policy
    print('- Attaching Policy')
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']
    
    # Get the IAM role ARN
    print('- Get the IAM role ARN')
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    
    # Create a Redshift Cluster
    create_redshift_cluster(redshift,roleArn)
    
    # Check if cluster available
    myClusterProps = get_cluster_props(redshift)
    
    # Set endpoint and role arn globally
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    
    # Set endpoint and role arn in configuration
    set_endpoint_rolearn(config)

    # Enable cluster accessing
    enable_cluster_access(ec2,myClusterProps)
    
    print("- All done")

if __name__ == "__main__":
    main()