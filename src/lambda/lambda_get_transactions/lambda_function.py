# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
from boto3.dynamodb.conditions import Key
import os
import logging
import json
import decimal
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


logger = logging.getLogger()
logger.setLevel(os.getenv('LOG_LEVEL'))
TABLE_NAME = os.getenv('DDB_TABLE_NAME')

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(TABLE_NAME)
return_object = {}


def return_error(message, http_status_code=500):
    global return_object
    return_message = {'status': 'error', 'message': message}
    return_object = {
        "statusCode": http_status_code,
        "body": json.dumps(return_message),
        "isBase64Encoded": False
    }
    logger.error(return_message)

    return return_object


def query_transactions(account_id):
    return_message = {}
    global return_object

    logger.info(f"Querying DynamoDB for account with id {account_id}")
    response = table.query(TableName=TABLE_NAME,
                           Select='ALL_ATTRIBUTES',
                           KeyConditionExpression=Key('accountId').eq(account_id))

    return_message['Transactions'] = response['Items']

    http_status_code = 200
    return_message['status'] = 'Ok'
    return_object = {
        "statusCode": http_status_code,
        "body": json.dumps(return_message, cls=DecimalEncoder),
        "isBase64Encoded": False
    }


def lambda_handler(event, context):
    logger.debug(f"Event received: {json.dumps(event)}")

    global return_object
    return_object = {}
    body = {}

    try:
        body = json.loads(event['body'])
    except Exception as e:
        return_error(str(e), http_status_code=400)

    if body['accountId']:
        try:
            query_transactions(body['accountId'])
        except Exception as e:
            return_error(str(e), http_status_code=500)
    else:
        return_error('accountId not specified', http_status_code=400)

    return return_object
