import decimal
import json
from typing import Any
from tempfile import NamedTemporaryFile

from botocore.exceptions import ClientError
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.dynamodb import AwsDynamoDBHook

from airflow_utilities.aws.utils import parse_bucket_and_path_from_uri


class S3toDynamoDBTransferOperator(BaseOperator):
    """
    A custom operator for inserting s3 data to DynamoDB.

    Parameters
    ----------
    aws_conn_id : str
        The aws connection ID defined in Airflow. Specify ONLY when the AWS account
            for S3 and DynamoDB is the same. If specified, it will overwrite all the other conn_id parameters
            (``dynamo_conn_id``, ``s3_conn_id``).
    dynamo_conn_id : str
        The aws connection ID defined in Airflow for the AWS account where the DynamoDB
            is accessible. Specify ONLY when the AWS account for S3 is different from the AWS account for DynamoDB.
    s3_conn_id : str
        The aws connection ID defined in Airflow for the AWS account where the S3 bucket
            is accessible. Specify ONLY when the AWS account for S3 is different from the AWS account for DynamoDB.
    dynamo_table : str
        The name of the dynamoDB table.
    s3_uri : str
        The source S3 path/file.
    s3_uri_is_prefix : bool
        Flag which specifies whether the provided ``s3_uri`` parameter
            is a path/prefix or file/key. Set to ``True`` if the specified S3 source is a path/prefix.
    """

    def __init__(
            self,
            dynamo_table: str,
            s3_uri: str,
            s3_uri_is_prefix: bool,
            aws_conn_id: str = None,
            dynamo_conn_id: str = None,
            s3_conn_id: str = None,
            overwrite_dynamo_table: bool = False,
            **kwargs):
        super().__init__(**kwargs)

        if aws_conn_id is None:
            assert dynamo_conn_id, "Must specify aws_conn_id for single account access or " \
                                   "both dynamo_conn_id and s3_conn_id for cross-account access"
            assert s3_conn_id, "Must specify aws_conn_id for single account access or " \
                               "both dynamo_conn_id and s3_conn_id for cross-account access"
        else:
            if dynamo_conn_id or s3_conn_id:
                self.log.warning("aws_conn_id will overwrite both dynamo_conn_id and s3_conn_id")
            dynamo_conn_id = aws_conn_id
            s3_conn_id = aws_conn_id

        self.dynamo_conn_id = dynamo_conn_id
        self.s3_uri = s3_uri
        self.s3_uri_is_prefix = s3_uri_is_prefix
        self.dynamo_hook: AwsDynamoDBHook = AwsDynamoDBHook(aws_conn_id=dynamo_conn_id)
        self.dynamo_table = dynamo_table
        self.s3_hook: S3Hook = S3Hook(aws_conn_id=s3_conn_id)
        self.overwrite_dynamo_table = overwrite_dynamo_table

    def execute(self, context: Any):

        s3_bucket, s3_path = parse_bucket_and_path_from_uri(self.s3_uri)

        if self.overwrite_dynamo_table:
            self.delete_table()
            self.create_table(
                keys=[{
                    "AttributeName": "lstyle_acct_id",
                    "KeyType": "HASH"
                }, {
                    "AttributeName": "model_id",
                    "KeyType": "RANGE"
                }],
                attributes=[
                    {'AttributeName': 'lstyle_acct_id', 'AttributeType': 'S'},
                    {'AttributeName': 'model_id', 'AttributeType': 'S'}
                ]
            )

        if self.s3_uri_is_prefix:
            # we treat this as a prefix
            self.log.info(f"Going through all objects with prefix s3://{s3_bucket}/{s3_path}")
            keys = self.s3_hook.list_keys(bucket_name=s3_bucket, prefix=s3_path)
            for key in keys:
                self.log.info(f"Processing single file s3://{s3_bucket}/{key}")
                self.process_single_file(s3_bucket, key)
        else:
            # we treat this as a single file
            self.log.info(f"Processing single file s3://{s3_bucket}/{s3_path}")
            self.process_single_file(s3_bucket, s3_path)

    def process_single_file(self, s3_bucket, s3_path):

        with NamedTemporaryFile("w") as temp:
            self.log.info(
                f"Dumping S3 file {s3_path} contents o local file {temp.name}"
            )
            obj = self.s3_hook.get_key(s3_path, bucket_name=s3_bucket)
            obj.download_file(temp.name)
            temp.flush()

            data = [json.loads(line, parse_float=decimal.Decimal) for line in open(temp.name, 'r')]

            self.write_to_dynamo(data)

    def write_to_dynamo(self, data):

        dynamodb = self.dynamo_hook.get_resource_type("dynamodb")
        table = dynamodb.Table(self.dynamo_table)

        with table.batch_writer() as writer:
            for item in data:
                try:
                    writer.put_item(Item=item)
                except Exception as e:
                    print(item)
                    raise Exception(repr(e))

    def create_table(self, keys: list, attributes: list):
        """
        Creates an Amazon DynamoDB table with the specified schema.

        Parameters
        -----------
        keys : list
            The key schema of the table. The schema defines the format
                       of the keys that identify items in the table. See here for the format:
                       https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.create_table
        attributes : list
            The attribute schema of the table. See here for the format:
                       https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.create_table

        Returns
        -------
        dict
            Created table information
        """

        try:
            dynamodb = self.dynamo_hook.get_resource_type("dynamodb")
            table = dynamodb.create_table(
                TableName=self.dynamo_table,
                KeySchema=keys,
                AttributeDefinitions=attributes,
                BillingMode="PROVISIONED",
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 2000
                }
            )
            table.wait_until_exists()
            self.log.info("Created table %s.", table.name)
        except ClientError:
            self.log.exception("Couldn't create table.")
            raise
        else:
            return table

    def delete_table(self):
        try:
            dynamodb = self.dynamo_hook.get_resource_type("dynamodb")
            table = dynamodb.Table(self.dynamo_table)
            _ = table.delete()
            table.wait_until_not_exists()
            print("Table deleted")
            return _
        except ClientError:
            self.log.info("Table does not exist")
