# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import time
import amazon.ion.simpleion as ion
from amazon.ion.json_encoder import IonToJSONEncoder
import json
import base64
import logging
import os
from aws_kinesis_agg.deaggregator import deaggregate_records
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(os.getenv('LOG_LEVEL'))

session = boto3.Session()
QLDB_TABLE_NAME = os.getenv(key='QLDB_TABLE_NAME')
dynamodb = boto3.resource('dynamodb')
DDB_TABLE_NAME = os.getenv(key='DDB_TABLE_NAME')
table = dynamodb.Table(DDB_TABLE_NAME)
EXPIRE_AFTER_DAYS = os.getenv(key='EXPIRE_AFTER_DAYS', default=None)
TTL_ATTRIBUTE = os.getenv(key='TTL_ATTRIBUTE', default=None)

REVISION_DETAILS_RECORD_TYPE = "REVISION_DETAILS"


def filtered_records_generator(kinesis_deaggregate_records, table_names=None):
    for record in kinesis_deaggregate_records:
        # Kinesis data in Python Lambdas is base64 encoded
        payload = base64.b64decode(record['kinesis']['data'])
        # payload is the actual ion binary record published by QLDB to the stream
        ion_record = ion.loads(payload)
        logger.info(f"Ion record: {ion.dumps(ion_record, binary=False)}")

        if ("recordType" in ion_record) and (ion_record["recordType"] == REVISION_DETAILS_RECORD_TYPE):
            table_info = get_table_info_from_revision_record(ion_record)

            if not table_names or (table_info and (table_info["tableName"] in table_names)):
                revision_data, revision_metadata = get_data_metdata_from_revision_record(ion_record)

                yield {"table_info": table_info,
                       "revision_data": revision_data,
                       "revision_metadata": revision_metadata}


def get_data_metdata_from_revision_record(revision_record):
    """
    Retrieves the data block from revision Revision Record
    Parameters:
       revision_record (string): The ion representation of Revision record from QLDB Streams
    """

    revision_data = None
    revision_metadata = None

    if ("payload" in revision_record) and ("revision" in revision_record["payload"]):
        if "data" in revision_record["payload"]["revision"]:
            revision_data = revision_record["payload"]["revision"]["data"]
        else:
            revision_data = None
        if "metadata" in revision_record["payload"]["revision"]:
            revision_metadata = revision_record["payload"]["revision"]["metadata"]

    return [revision_data, revision_metadata]


def get_table_info_from_revision_record(revision_record):
    """
    Retrieves the table information block from revision Revision Record
    Table information contains the table name and table id
    Parameters:
       revision_record (string): The ion representation of Revision record from QLDB Streams
    """

    if ("payload" in revision_record) and "tableInfo" in revision_record["payload"]:
        return revision_record["payload"]["tableInfo"]


def days_to_seconds (days):
    return int(days) * 24 * 60 * 60


def lambda_handler(event, context):
    raw_kinesis_records = event['Records']

    # Deaggregate all records in one call
    records = deaggregate_records(raw_kinesis_records)

    # Iterate through deaggregated records
    for record in filtered_records_generator(records,
                                             table_names=[QLDB_TABLE_NAME]):
        table_name = record["table_info"]["tableName"]
        revision_data = record["revision_data"]
        revision_metadata = record["revision_metadata"]

        if revision_data:
            if table_name == QLDB_TABLE_NAME:
                ddb_item = json.loads(json.dumps(revision_data, cls=IonToJSONEncoder), parse_float=Decimal)
                string_datetime = ion.dumps(revision_metadata['txTime'], binary=False).split()[1]
                parsed_datetime = time.strptime(string_datetime, "%Y-%m-%dT%H:%M:%S.%fZ")
                unix_time = int(time.strftime('%s', parsed_datetime))
                ddb_item['txTime'] = string_datetime
                ddb_item['txId'] = revision_metadata['txId']
                ddb_item['timestamp'] = unix_time
                if TTL_ATTRIBUTE and EXPIRE_AFTER_DAYS:
                    ddb_item[TTL_ATTRIBUTE] = unix_time + days_to_seconds(EXPIRE_AFTER_DAYS)

                try:
                    table.put_item(Item=ddb_item)
                except Exception as e:
                    logger.error(f"Error processing record {ddb_item}")
                    raise e


    return {
        'statusCode': 200
    }
