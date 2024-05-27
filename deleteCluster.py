import pandas as pd
import boto3
import json
import configparser
from botocore.exceptions import ClientError


def load_config(config_file='dwh.cfg'):
    config = configparser.ConfigParser()
    config.read_file(open(config_file))
    return config


def create_aws_clients(config):
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    iam = boto3.client('iam',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,
                       region_name='us-west-2')

    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)

    return iam, redshift


def delete_redshift_cluster(redshift, config):
    try:
        redshift.delete_cluster(
            ClusterIdentifier=config.get("DWH", "DWH_CLUSTER_IDENTIFIER"),
            SkipFinalClusterSnapshot=True
        )
    except ClientError as e:
        print(e)

def delete_iam_role(iam, role_name):
    iam.detach_role_policy(RoleName=role_name, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=role_name)

if __name__ == '__main__':
    config = load_config()

    iam, redshift = create_aws_clients(config)
    role_name = config.get("DWH", "DWH_IAM_ROLE_NAME")

    delete_redshift_cluster(redshift, config)
    delete_iam_role(iam, role_name)
    print('Cluster and IAM role deleted successfully')


