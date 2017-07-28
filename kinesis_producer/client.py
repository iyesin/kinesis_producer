import logging
import time
from multiprocessing.pool import ThreadPool

import boto3
import botocore

log = logging.getLogger(__name__)


def get_connection(aws_region,
                   aws_access_key_id=None,
                   aws_secret_access_key=None):
    session = boto3.session.Session()
    kwargs = {
        'region_name': aws_region
    }
    if aws_access_key_id is not None:
        kwargs['aws_access_key_id'] = aws_access_key_id
    if aws_secret_access_key is not None:
        kwargs['aws_secret_access_key'] = aws_secret_access_key

    connection = session.client('kinesis', **kwargs)
    return connection

def call_and_retry(boto_function, max_retries, **kwargs):
    """Retry Logic for generic boto client calls.

    This code follows the exponetial backoff pattern suggested by
    http://docs.aws.amazon.com/general/latest/gr/api-retries.html
    """
    retries = 0
    while True:
        if retries:
            log.warning('Retrying (%i) %s', retries, boto_function)

        try:
            return boto_function(**kwargs)
        except botocore.exceptions.ClientError as exc:
            if retries >= max_retries:
                raise exc
            error_code = exc.response.get("Error", {}).get("Code")
            if error_code == 'ProvisionedThroughputExceededException':
                time.sleep(2 ** retries * .1)
                retries += 1
            else:
                raise exc

def call_and_retry_put_records(boto_function, max_retries, **kwargs):
    """Retry Logic for put_records boto call.

    This code follows the exponetial backoff pattern suggested by
    http://docs.aws.amazon.com/general/latest/gr/api-retries.html
    """
    retries = 0
    while True:
        if retries:
            log.warning('Retrying (%i) %s', retries, boto_function)

        records = kwargs['Records']
        resp = boto_function(**kwargs)
        if resp[u'FailedRecordCount'] > 0:
            failed_records = _get_failed_record_responses(resp)
        else:
            return resp
        if len(failed_records) > 0:
            if retries >= max_retries:
                raise Exception("Could not send all Kinesis records")
            if (_exceeded_throughput(failed_records) or
                _internal_failure_response(failed_records)):
                time.sleep(2 ** retries * .1)
                retries += 1
                records = _get_failed_original_records(records, failed_records)
                kwargs['Records'] = records
            else:
                # Each failed record is a (index, record) tuple, so get the record value.
                exc = failed_records[0][1]
                raise Exception("{}: {}".format(exc['ErrorCode'],
                                                exc['ErrorMessage']))

def _get_failed_record_responses(resp):
    return [
        # the record object might not contain the actual record
        # data and just have AWS info instead. Store the index that
        # failed, too.
        (i, record) for (i, record)
        in enumerate(resp[u'Records'])
        if record.has_key('ErrorCode')
    ]


def _exceeded_throughput(failed_records):
    for i, record_resp in failed_records:
        if record_resp['ErrorCode'] == 'ProvisionedThroughputExceededException':
            return True
    return False

def _internal_failure_response(failed_records):
    for i, record_resp in failed_records:
        if record_resp['ErrorCode'] == 'InternalFailure':
            return True
    return False

def _get_failed_original_records(original_records, failed_records):
    new_records = []
    for i, record_resp in failed_records:
        new_records.append(original_records[i])
    return original_records

class Client(object):
    """Synchronous Kinesis client."""

    def __init__(self, config):
        self.stream = config['stream_name']
        self.max_retries = config['kinesis_max_retries']
        self.connection = get_connection(
            config['aws_region'],
            aws_access_key_id=config.get('aws_access_key_id', None),
            aws_secret_access_key=config.get('aws_secret_access_key', None),
        )

    def put_records(self, records):
        """Send records to Kinesis API.

        Records is a list of dictionaries like {'Data': data, 'PartitionKey': partition_key}.
        """
        log.debug('Sending record: %s', records[:100])
        try:
            call_and_retry_put_records(self.connection.put_records,
                           self.max_retries,
                           StreamName=self.stream,
                           Records=records)
        except:
            log.exception('Failed to send records to Kinesis')

    def close(self):
        log.debug('Closing client')

    def join(self):
        log.debug('Joining client')


class ThreadPoolClient(Client):
    """Thread pool based asynchronous Kinesis client."""

    def __init__(self, config):
        super(ThreadPoolClient, self).__init__(config)
        self.pool = ThreadPool(processes=config['kinesis_concurrency'])

    def put_records(self, records):
        task_func = super(ThreadPoolClient, self).put_records
        self.pool.apply_async(task_func, args=[records])

    def close(self):
        super(ThreadPoolClient, self).close()
        self.pool.close()

    def join(self):
        super(ThreadPoolClient, self).join()
        self.pool.join()
