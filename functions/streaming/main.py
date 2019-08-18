# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


'''
This Cloud function is responsible for:
- Parsing and validating new files added to Cloud Storage.
- Checking for duplications.
- Inserting files' content into BigQuery.
- Logging the ingestion status into Cloud Firestore and Stackdriver.
- Publishing a message to either an error or success topic in Cloud Pub/Sub.
'''

import csv
import json
import logging
import os
import traceback
from datetime import datetime
from io import StringIO

from google.api_core import retry
from google.cloud import bigquery
from google.cloud import firestore
from google.cloud import pubsub_v1
from google.cloud import storage
import pytz



PROJECT_ID = os.getenv('GCP_PROJECT')
# TODO: get dataset and table names from environmental variables
BQ_DATASET = 'Telematics'
BQ_TABLE = 'TractorsData'
ERROR_TOPIC = 'projects/%s/topics/%s' % (PROJECT_ID, 'streaming_error_topic')
SUCCESS_TOPIC = 'projects/%s/topics/%s' % (PROJECT_ID, 'streaming_success_topic')
DB = firestore.Client()
CS = storage.Client()
PS = pubsub_v1.PublisherClient()
BQ = bigquery.Client()


def streaming(data, context):
    '''This function is executed whenever a file is added to Cloud Storage'''
    bucket_name = data['bucket']
    file_name = data['name']
    db_ref = DB.document(u'streaming_files/%s' % file_name)
    if _was_already_ingested(db_ref):
        _handle_duplication(db_ref)
    else:
        try:
            _insert_into_bigquery(bucket_name, file_name)
            _handle_success(db_ref)
        except Exception:
            _handle_error(db_ref)


def _was_already_ingested(db_ref):
    status = db_ref.get()
    return status.exists and status.to_dict()['success']


def _handle_duplication(db_ref):
    dups = [_now()]
    data = db_ref.get().to_dict()
    if 'duplication_attempts' in data:
        dups.extend(data['duplication_attempts'])
    db_ref.update({
        'duplication_attempts': dups
    })
    logging.warn('Duplication attempt streaming file \'%s\'' % db_ref.id)


def _insert_into_bigquery(bucket_name, file_name):
    blob = CS.get_bucket(bucket_name).blob(file_name)
    
    # TODO: check if batching below flow of data is a better approach. Be mindful about the "ACID" principle also ...
    # batching can be implemented from the blob all the way to the "insert into BQ"
    
    # parse CSV data and transform it
    fileContents = blob.download_as_string().decode('utf-8')
    csvData = csv.reader(StringIO(fileContents), delimiter=';')
    parsedRows = []
    first = True
    for dateTime, serial, gpsLon, gpsLat, workingHs, engineRpm, \
        engineLoad, fuelConsumption, speedGearbox, speedRadar, \
        motorTemperature, frontPtoRpm, rearPtoRpm, gearShift, \
        ambientTemperature, parkingBreakStatus, differentialLockStatus, \
        allWheelStatus, creeperStatus in csvData:

        # skip header
        if (first):
            first = False
            continue

        # parse datetime to be able to use it later in the correct format 
        dateTimeParsed = datetime.strptime(dateTime, '%b %d, %Y %I:%M:%S %p')

        # append transformed data to rows list
        # TODO: Try to avoid the CSV "schema dependency" in the script...
        row = (
            str(dateTimeParsed), 
            serial, 
            gpsLat + ', ' + gpsLon, 
            workingHs, engineRpm, 
            engineLoad, 
            fuelConsumption, 
            speedGearbox, 
            speedRadar, 
            motorTemperature, 
            frontPtoRpm, 
            rearPtoRpm, 
            gearShift, 
            ambientTemperature, 
            parkingBreakStatus, 
            differentialLockStatus, 
            allWheelStatus, 
            creeperStatus)
        parsedRows.append(row)

        logging.debug("row inserted: " + str(row))

    # insert transformed data to BQ table
    tableRef = BQ.dataset(BQ_DATASET).table(BQ_TABLE)
    table = BQ.get_table(tableRef)
    errors = BQ.insert_rows(table, fileContents)

    # check and raise any errors
    if errors != []:
        raise BigQueryError(errors)


def _handle_success(db_ref):
    message = 'File \'%s\' streamed into BigQuery' % db_ref.id
    doc = {
        u'success': True,
        u'when': _now()
    }
    db_ref.set(doc)
    PS.publish(SUCCESS_TOPIC, message.encode('utf-8'), file_name=db_ref.id)
    logging.info(message)


def _handle_error(db_ref):
    message = 'Error streaming file \'%s\'. Cause: %s' % (db_ref.id, traceback.format_exc())
    doc = {
        u'success': False,
        u'error_message': message,
        u'when': _now()
    }
    db_ref.set(doc)
    PS.publish(ERROR_TOPIC, message.encode('utf-8'), file_name=db_ref.id)
    logging.error(message)


def _now():
    return datetime.utcnow().replace(tzinfo=pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z')


class BigQueryError(Exception):
    '''Exception raised whenever a BigQuery error happened''' 

    def __init__(self, errors):
        super().__init__(self._format(errors))
        self.errors = errors

    def _format(self, errors):
        err = []
        for error in errors:
            err.extend(error['errors'])
        return json.dumps(err)
