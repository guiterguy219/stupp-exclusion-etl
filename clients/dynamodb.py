import boto3
import time

class DyanmoDBClient:
    def __init__(self):
        self.resource = boto3.resource('dynamodb')
        self.client = boto3.client('dynamodb')

    def with_table(self, table_name, attributes, key):
        if self.get_table_status(table_name) == 'NOT CREATED':
            self.resource.create_table(
                AttributeDefinitions=[
                    {
                        'AttributeName': name,
                        'AttributeType': type
                    } for type, name in attributes
                ],
                TableName=table_name,
                KeySchema=[
                    {
                        'AttributeName': name,
                        'KeyType': 'HASH' if name == key else 'RANGE'
                    } for _, name in attributes
                ],
                BillingMode='PAY_PER_REQUEST',
            )
        while not self.get_table_status(table_name) == 'ACTIVE':
            time.sleep(1)

    def get_table_status(self, table_name):
        try:
            res = self.client.describe_table(TableName=table_name)
            return res['Table']['TableStatus']
        except self.client.exceptions.ResourceNotFoundException:
            return 'NOT CREATED'

    def typify_value(self, value):
        t = type(value)
        if t == dict:
            return { 'M' : { k: self.typify_value(v) for k, v in value.items() }}
        elif t == list:
            return { 'L' : [ self.typify_value(v) for v in value ]}
        elif t == str:
            return { 'S': value }
        elif t == int:
            return { 'S': str(value) }
        return { 'NULL' : True }
        