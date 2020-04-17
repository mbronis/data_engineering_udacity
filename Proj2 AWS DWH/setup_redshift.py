import configparser

config = configparser.ConfigParser()
config.read_file(open('aws.cfg'))



import boto3

s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=config.get("AWS", "KEY"),
                       aws_secret_access_key=config.get("AWS", "SECRET")
                   )