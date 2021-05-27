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
    return_message = {'status': 'Error', 'message': message}
    return_object = {
        "statusCode": http_status_code,
        "body": json.dumps(return_message),
        "isBase64Encoded": False
    }
    logger.error(return_message)

    return return_object


def withdraw_funds(account_id, amount, executor):
    return_message = {}
    global return_object

    cursor = executor.execute_statement(
        f"SELECT count(accountId) as number_of_accounts FROM \"{QLDB_TABLE_NAME}\" WHERE accountId = ? ", account_id)

    first_doc = next(cursor, None)
    if first_doc:
        if first_doc['number_of_accounts'] > 1:
            return_error(f"More than one account with user id {account_id}", http_status_code=500)
            return return_object

        if first_doc['number_of_accounts'] == 0:
            return_error(f"Account {account_id} not found", http_status_code=400)
            return return_object

    cursor = executor.execute_statement(f"SELECT accountId, balance FROM \"{QLDB_TABLE_NAME}\" WHERE accountId = ?", account_id)

    first_doc = next(cursor, None)
    if first_doc['balance'] - amount < 0:
        return_error(f"Funds too low. Cannot deduct {amount} from account {account_id}", http_status_code=400)
        return return_object
    else:
        return_message['accountId'] = first_doc['accountId']
        return_message['old_balance'] = first_doc['balance']
        return_message['new_balance'] = first_doc['balance'] - amount
        executor.execute_statement(f"UPDATE \"{QLDB_TABLE_NAME}\" SET balance = ? WHERE accountId = ?",
                                   return_message['new_balance'], account_id)

    http_status_code = 200
    return_message['status'] = 'Ok'
    return_object = {
        "statusCode": http_status_code,
        "body": json.dumps(return_message),
        "isBase64Encoded": False
    }

    return return_object


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

    if body['accountId'] and body['amount'] and body['amount'] > 0:
        try:
            qldb_driver.execute_lambda(lambda executor: withdraw_funds(body['accountId'], body['amount'], executor))
        except Exception as e:
            return_error(str(e), http_status_code=500)
            return return_object
    else:
        return_error('accountId and amount not specified, or amount not greater than zero', http_status_code=400)

    return return_object
