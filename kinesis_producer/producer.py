import logging

import six
from six.moves import queue

from .sender import Sender
from .accumulator import RecordAccumulator
from .buffer import RawBuffer
from .client import Client, ThreadPoolClient
from .constants import KINESIS_RECORD_MAX_SIZE
from .partitioner import random_partitioner

log = logging.getLogger(__name__)


class KinesisProducer(object):
    """A Kinesis client that publishes records to a Kinesis stream."""

    def __init__(self, config, partitioner=random_partitioner):
        log.debug('Starting KinesisProducer')
        self.config = config
        self._queue = queue.Queue()
        self._closed = False
        self._partitioner = partitioner

        accumulator = RecordAccumulator(RawBuffer,
                                        config)
        if config['kinesis_concurrency'] == 1:
            client = Client(config)
        else:
            client = ThreadPoolClient(config)
        self._sender = Sender(queue=self._queue,
                              accumulator=accumulator,
                              client=client)
        self._sender.daemon = True
        self._sender.start()

    def send(self, record, partition_key=None):
        """Publish a record to Kinesis.

        Don't block. Record must be bytes type.
        """
        assert not self._closed, "KinesisProducer closed but called anyway"

        if six.PY2:
            if isinstance(record, unicode):
                record = record.encode('utf-8')

        if not isinstance(record, six.binary_type):
            raise ValueError("Record must be bytes type")

        record_size = len(record)
        if record_size > KINESIS_RECORD_MAX_SIZE:
            raise ValueError("Record is larger than max record size")

        if partition_key is None:
            partition_key = self._partitioner(record)
        self._queue.put((record, partition_key))

    def close(self):
        if self._closed:
            return
        log.debug('Closing KinesisProducer')
        self._sender.close()
        self._closed = True

    def join(self):
        self.close()
        log.debug('Joining KinesisProducer')
        self._queue.join()
        log.debug('KinesisProducer record queue was joined')
        self._sender.join()
