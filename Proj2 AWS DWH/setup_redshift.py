'''
Setup AWS using python SDK:
    - Redshift Cluster - ETL/Staging Area
    - IAM user for extrnal data access (Udacity S3 Bucket)
      credintials (KEY + SECRET) stored in aws.cfg
'''

import configparser
import boto3

# Get IAM credentials
config = configparser.ConfigParser()
config.read_file(open('aws.cfg'))

KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")


# Check out the sample data sources on S3
s3 = boto3.resource(
    's3',
    region_name="us-west-2",
    aws_access_key_id = KEY,
    aws_secret_access_key = SECRET
)

sampleDbBucket =  s3.Bucket("awssampledbuswest2")
for obj in sampleDbBucket.objects.filter(Prefix="ssbgz"):
    print(obj)

# Create Redshift cluster


# Get Cluster details
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")
DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_DB = config.get("DWH", "DWH_DB")
DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")


# Create client for Redshift
redshift = boto3.client('redshift',
                       region_name="eu-west-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

try:
    response = redshift.create_cluster(        
        #HW
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        #Identifiers & Credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        
        #Roles (for s3 access)
        IamRoles=[DWH_IAM_ROLE_NAME]  
    )
except Exception as e:
    print(e)

# An error occurred (AccessDenied) when calling the CreateCluster operation: 
# User: 
#     arn:aws:iam::562911153477:user/airflow_redshift_user 
# is not authorized to perform: 
#     iam:PassRole 
# on resource: 
#     arn:aws:iam::562911153477:role/Redshift_RO