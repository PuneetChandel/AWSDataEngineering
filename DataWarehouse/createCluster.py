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

    ec2 = boto3.resource('ec2',
                         region_name="us-west-2",
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET)

    s3 = boto3.resource('s3',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)

    iam = boto3.client('iam',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,
                       region_name='us-west-2')

    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)

    return ec2, s3, iam, redshift


def create_iam_role(iam, role_name):
    try:
        print("Creating a new IAM Role")
        iam.create_role(
            Path='/',
            RoleName=role_name,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )

        print("Attaching Policy")
        iam.attach_role_policy(RoleName=role_name,
                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")

        print("Getting the IAM role ARN")
        role_arn = iam.get_role(RoleName=role_name)['Role']['Arn']
        return role_arn
    except ClientError as e:
        print(e)
        return None


def create_redshift_cluster(redshift, config, role_arn):
    try:
        response = redshift.create_cluster(
            ClusterType=config.get("DWH", "DWH_CLUSTER_TYPE"),
            NodeType=config.get("DWH", "DWH_NODE_TYPE"),
            NumberOfNodes=int(config.get("DWH", "DWH_NUM_NODES")),
            DBName=config.get("DWH", "DWH_DB"),
            ClusterIdentifier=config.get("DWH", "DWH_CLUSTER_IDENTIFIER"),
            MasterUsername=config.get("DWH", "DWH_DB_USER"),
            MasterUserPassword=config.get("DWH", "DWH_DB_PASSWORD"),
            IamRoles=[role_arn]
        )
    except ClientError as e:
        print(e)


def pretty_redshift_props(props):
    keys_to_show = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint",
                    "NumberOfNodes", 'VpcId']
    x = [(k, v) for k, v in props.items() if k in keys_to_show]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def open_ports(ec2, vpc_id, port):
    try:
        vpc = ec2.Vpc(id=vpc_id)
        default_sg = list(vpc.security_groups.all())[0]
        print(default_sg)
        default_sg.authorize_ingress(
            GroupName=default_sg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(port),
            ToPort=int(port)
        )
    except ClientError as e:
        print(e)


if __name__ == '__main__':
    config = load_config()

    ec2, s3, iam, redshift = create_aws_clients(config)

    role_name = config.get("DWH", "DWH_IAM_ROLE_NAME")
    role_arn = create_iam_role(iam, role_name)

    if role_arn:
        create_redshift_cluster(redshift, config, role_arn)

        my_cluster_props = \
        redshift.describe_clusters(ClusterIdentifier=config.get("DWH", "DWH_CLUSTER_IDENTIFIER"))['Clusters'][0]
        print(pretty_redshift_props(my_cluster_props))

        open_ports(ec2, my_cluster_props['VpcId'], config.get("DWH", "DWH_PORT"))

