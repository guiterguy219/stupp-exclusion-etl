import boto3
import time

class DyanmoDBClient:
    def __init__(self):
        self.resource = boto3.resource('dynamodb')
        self.client = boto3.client('dynamodb')

    def with_table(self, table_name, key, **kwargs):
        if self.get_table_status(table_name) == 'NOT CREATED':
            table_create_args = {
                'AttributeDefinitions': [
                    {
                        'AttributeName': name,
                        'AttributeType': type
                    } for type, name in key + kwargs.get('indexes', [])
                ],
                'TableName': table_name,
                'KeySchema': [
                    {
                        'AttributeName': element[1],
                        'KeyType': 'HASH' if idx == 0 else 'RANGE'
                    } for idx, element in enumerate(key[:2])
                ],
                'BillingMode': 'PAY_PER_REQUEST'
            }
            if kwargs.get('indexes', False):
                table_create_args['GlobalSecondaryIndexes'] = [
                    {
                        'IndexName': name.strip().replace(' ', '_') + '-Index',
                        'KeySchema': [{
                            'AttributeName': name,
                            'KeyType': 'HASH'
                        }],
                        'Projection': {
                            'ProjectionType': 'KEYS_ONLY'
                        }
                    } for _, name in kwargs.get('indexes', [])
                ]
            self.resource.create_table(**table_create_args)
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
        # Check for values that may be converted into infinity or NaN (ie: 1E1771 becomes infinity)
        if 'inf'not in str(value) and 'nan' not in str(value) and 'e' not in str(value).lower():
            try:
                value = int(str(value))
                t = type(value)
            except ValueError:
                try:
                    value = float(str(value))
                    t = type(value)
                except ValueError:
                    pass
        if t == dict:
            return { 'M' : { k: self.typify_value(v) for k, v in value.items() }}
        elif t == list:
            return { 'L' : [ self.typify_value(v) for v in value ]}
        elif t == str:
            return { 'S': value }
        elif t == int or t == float:
            return { 'N': str(value) }
        return { 'NULL' : True }
        