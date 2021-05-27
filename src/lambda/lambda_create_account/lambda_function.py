# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from pyqldb.driver.qldb_driver import QldbDriver
from pyqldb.config.retry_config import RetryConfig
import os
import logging
import json
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all


logger = logging.getLogger()
logger.setLevel(os.getenv('LOG_LEVEL'))

LEDGER_NAME = os.getenv('LEDGER_NAME')
QLDB_TABLE_NAME = os.getenv('QLDB_TABLE_NAME')
retry_config = RetryConfig(retry_limit=3)

# Initialize the driver
qldb_driver = QldbDriver(ledger_name=LEDGER_NAME, retry_config=retry_config)

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


def create_account(account_id, executor):
    global return_object
    return_message = {}

    logger.info(f"Verifying account with id {account_id} does not exist")
    cursor = executor.execute_statement(f"SELECT * FROM \"{QLDB_TABLE_NAME}\" WHERE accountId = ? ", account_id)
    first_record = next(cursor, None)

    if first_record:
        return_error(f"Account with user id {account_id} already exists", http_status_code=400)
        return return_object
    else:
        doc = {
            'accountId': account_id,
            'balance': 0
        }
        logger.info(f"Creating account with id {account_id} and balance = {doc['balance']}")
        executor.execute_statement(f"INSERT INTO \"{QLDB_TABLE_NAME}\" ?", doc)

    http_status_code = 200
    return_message['status'] = 'Ok'
    return_message['accountId'] = account_id

    return_object = {
        "statusCode": http_status_code,
        "body": json.dumps(return_message),
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
        return return_object

    if body['accountId']:
        try:
            qldb_driver.execute_lambda(lambda executor: create_account(body['accountId'], executor))
        except Exception as e:
            return_error(str(e), http_status_code=500)
            return return_object
    else:
        return_error('accountId not specified', http_status_code=400)
        return return_object

    return return_object
