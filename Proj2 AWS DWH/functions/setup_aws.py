'''Setting up AWS Redshift cluster'''

import configparser
import boto3
import psycopg2

def create_redshift_admin(iam_credentials):
    '''
    Creates a redshift client instance with AWS IAM user credentials.
    The user needs policy to create clusters and pass 'S3 Read' role to it.
    
    Parameters
    ----------
    iam_credentials : str
        path to config (.cfg) file with IAM user KEY and SECRET

    Returns
    ----------
    redshift client
        
    '''
    
    #Read user KEY and SECRET
    config = configparser.ConfigParser()
    config.read_file(open(iam_credentials))

    key = config.get("IAM", "key")
    secret = config.get("IAM", "secret")
    region = config.get("CLUSTER", "cl_region")

    #Create redshift client
    redshift_admin = boto3.client(
        'redshift',
        region_name = region,
        aws_access_key_id = key,
        aws_secret_access_key = secret
    )
    
    return redshift_admin

def create_s3(iam_credentials):
    '''
    Creates a S3 resource instance with AWS IAM user credentials.
    
    Parameters
    ----------
    iam_credentials : str
        path to config (.cfg) file with IAM user KEY and SECRET

    Returns
    ----------
    S3 resource instance
        
    '''
    
    #Read user KEY and SECRET
    config = configparser.ConfigParser()
    config.read_file(open(iam_credentials))

    key = config.get("IAM", "key")
    secret = config.get("IAM", "secret")
    region = config.get("CLUSTER", "cl_region")

    #S3 resource
    s3 = boto3.resource(
        's3',
        region_name = region,
        aws_access_key_id = key,
        aws_secret_access_key = secret
                   )
    
    return s3

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
    config = configparser.ConfigParser()
    config.read_file(open(cluster_config))
       
    cl_identifier = config.get("CLUSTER", "cl_identifier")
    
    cl_node_type = config.get("CLUSTER", "cl_node_type")
    cl_type = config.get("CLUSTER", "cl_type")
    cl_num_nodes = config.get("CLUSTER", "cl_num_nodes")
    cl_iam_arn = config.get("CLUSTER", "cl_iam_arn")
    
    db_name = config.get("DB", "db_name")
    db_user = config.get("DB", "db_user")
    db_pwd = config.get("DB", "db_pwd")
                
    try:
        response = redshift_admin.create_cluster(        
            #Cluster
            NodeType = cl_node_type,
            ClusterType = cl_type,
            NumberOfNodes = int(cl_num_nodes),

            #Identifiers & Credentials
            ClusterIdentifier = cl_identifier,
            DBName = db_name,
            MasterUsername = db_user,
            MasterUserPassword = db_pwd

            #Role (for s3 access)
            ,IamRoles = [cl_iam_arn]  
        )
        return response
    
    except Exception as e:
        print(e)


def delete_cluster(cluster_id, redshift_admin):
    '''
    Deletes a redshift cluster instance.
    
    Parameters
    ----------
    cluster_id : str
         cluster identifier
    
    redshift_admin : redshift client
        Redshift client with policies allowing for a cluster deletion
    '''
    
    # Delete cluster
    try:
        redshift_admin.delete_cluster(
            ClusterIdentifier = cluster_id,
            SkipFinalClusterSnapshot=True
        )
    except Exception as e:
        print(e)

def create_ec2_instance(iam_credentials):
    '''
    Creates a EC2 instance with AWS IAM user credentials.
    
    Parameters
    ----------
    iam_credentials : str
        path to config (.cfg) file with IAM user KEY and SECRET

    Returns
    ----------
    EC2 resource
        
    '''
    
    #Read user KEY and SECRET
    config = configparser.ConfigParser()
    config.read_file(open(iam_credentials))

    key = config.get("IAM", "key")
    secret = config.get("IAM", "secret")
    region = config.get("CLUSTER", "cl_region")

    #Create client
    ec2 = boto3.resource('ec2',
        region_name = region,
        aws_access_key_id = key,
        aws_secret_access_key = secret
    )
    
    return ec2

def open_tcp_endpoint(iam_credentials, cluster_config):
    '''
    Opens an incoming TCP port to access the cluster endpoint.
    
    Parameters
    ----------
    iam_credentials : str
        path to config (.cfg) file with IAM user KEY and SECRET
    
    cluster_config : str
         config file with cluster characteristics
    '''
    
    # create ec2 instance
    ec2 = create_ec2_instance(iam_credentials)
    
    # get cluster details from config file
    config = configparser.ConfigParser()
    config.read_file(open(cluster_config))
       
    cl_vpc_id = config.get("CLUSTER", "cl_vpc_id")
    cl_vpc_sg_id = config.get("CLUSTER", "cl_vpc_sg_id")
    db_port = config.get("DB", "db_port")
    
    
    try:
        vpc = ec2.Vpc(id = cl_vpc_id)
        sgId = cl_vpc_sg_id
        defaultSg = list(vpc.security_groups.filter(GroupIds=[sgId]))[0]
        print(defaultSg)

        defaultSg.authorize_ingress(
            GroupName = defaultSg.group_name,
            CidrIp = '0.0.0.0/0',
            IpProtocol = 'TCP',
            FromPort = int(db_port),
            ToPort = int(db_port)
        )
    except Exception as e:
        print(e)

   
def make_connection_string(cluster_config):
    '''
    Creates connection string for redshift cluster
    
    Parameters
    ----------
    cluster_config : str
         config file with cluster characteristics
        
    Returns
    ----------
    str
        a connection string
    '''

    # Get Cluster details from config file
    config = configparser.ConfigParser()
    config.read_file(open(cluster_config))
    
    conn_str = "host={} dbname={} user={} password={} port={}".format(*config['DB'].values())
    
    return conn_str


def make_connection(cluster_config):
    '''
    Creates connection object for redshift cluster
    
    Parameters
    ----------
    cluster_config : str
         config file with cluster characteristics
        
    Returns
    ----------
    connection object
        a new connection object
    '''

    # make connection string
    conn_str = make_connection_string(cluster_config)
    
    # connect
    try:
        conn = psycopg2.connect(conn_str)
        conn.autocommit = True
        
        return conn
    
    except Exception as e:
        print("Unable to connect to the database")
        print(e)


