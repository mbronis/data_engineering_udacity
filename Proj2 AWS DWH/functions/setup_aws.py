'''Setting up AWS Redshift cluster'''

import configparser
import boto3

def create_redshift_admin(admin_config):
    '''
    Creates a redshift client instance with AWS IAM user credentials.
    The user needs policy to create clusters and pass 'S3 Read' role to it.
    
    Parameters
    ----------
    admin_config : str
        path to config (.cfg) file with user KEY and SECRET

    Returns
    ----------
    redshift client
        
    '''
    
    #Read user KEY and SECRET
    cfg_aws = configparser.ConfigParser()
    cfg_aws.read_file(open(admin_config))

    KEY = cfg_aws.get("AWS", "KEY")
    SECRET = cfg_aws.get("AWS", "SECRET")

    #Create client
    redshift_admin = boto3.client(
        'redshift',
        region_name="eu-west-1",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )
    
    return redshift_admin


def create_cluster(cluster_config, redshift_admin):
    '''
    Creates a Redshift cluster instance.
    
    Parameters
    ----------
    cluster_config : str
         config file with cluster characteristics
    
    redshift_admin : redshift client
        Redshift client with policies allowing for a cluster creation
        and passing 'S3 read' policy to the cluster       

    Returns
    ----------
    dict
        response dictionaty with cluster details
    '''
    
    # Get Cluster details from config file
    cfg_dwh = configparser.ConfigParser()
    cfg_dwh.read_file(open(cluster_config))

    DWH_NODE_TYPE = cfg_dwh.get("DWH", "DWH_NODE_TYPE")
    DWH_CLUSTER_TYPE = cfg_dwh.get("DWH", "DWH_CLUSTER_TYPE")
    DWH_NUM_NODES = cfg_dwh.get("DWH", "DWH_NUM_NODES")
    DWH_DB = cfg_dwh.get("DWH", "DWH_DB")
    DWH_DB_USER = cfg_dwh.get("DWH", "DWH_DB_USER")
    DWH_DB_PASSWORD = cfg_dwh.get("DWH", "DWH_DB_PASSWORD")

    DWH_CLUSTER_IDENTIFIER = cfg_dwh.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    DWH_IAM_ROLE_NAME = cfg_dwh.get("DWH", "DWH_IAM_ROLE_NAME")
    
    
    try:
        response = redshift_admin.create_cluster(        
            #DHW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD

            #Role (for s3 access)
            ,IamRoles=[DWH_IAM_ROLE_NAME]  
        )
        return response
    
    except Exception as e:
        print(e)


def delete_cluster(cluster_config, redshift_admin):
    '''
    Deletes a redshift cluster instance.
    
    Parameters
    ----------
    cluster_config : str
         config file with cluster characteristics
    
    redshift_admin : redshift client
        Redshift client with policies allowing for a cluster deletion
    '''
    
    # Get Cluster id config file
    cfg_dwh = configparser.ConfigParser()
    cfg_dwh.read_file(open(cluster_config))
    
    DWH_CLUSTER_IDENTIFIER = cfg_dwh.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    
    # Delete cluster
    try:
        redshift_admin.delete_cluster(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            SkipFinalClusterSnapshot=True
        )
    except Exception as e:
        print(e)
