from six.moves import queue

import mock

from kinesis_producer.sender import Sender
from kinesis_producer.accumulator import RecordAccumulator
from kinesis_producer.buffer import RawBuffer


PARTITION_KEY = 4

def test_init(config):
    q = queue.Queue()
    accumulator = RecordAccumulator(RawBuffer, config)
    client = mock.Mock()

    sender = Sender(queue=q, accumulator=accumulator, client=client)
    sender.start()
    sender.close()
    sender.join()


def test_flush(config):
    q = queue.Queue()
    accumulator = RecordAccumulator(RawBuffer, config)
    client = mock.Mock()

    sender = Sender(queue=q, accumulator=accumulator, client=client)

    sender.flush()
    assert not client.put_record.called

    accumulator.try_append(b'-', PARTITION_KEY)

    sender.flush()
    expected_records = [{
        'Data': b'-',
        'PartitionKey': 4
    }]
    client.put_records.assert_called_once_with(expected_records)


def test_accumulate(config):
    q = queue.Queue()
    accumulator = RecordAccumulator(RawBuffer, config)
    client = mock.Mock()

    sender = Sender(queue=q, accumulator=accumulator, client=client)

    sender.run_once()
    assert not accumulator.has_records()

    q.put((b'-', PARTITION_KEY))

    sender.run_once()
    assert accumulator.has_records()


def test_flush_if_ready(config):
    q = queue.Queue()
    accumulator = RecordAccumulator(RawBuffer, config)
    client = mock.Mock()

    sender = Sender(queue=q, accumulator=accumulator, client=client)

    accumulator.try_append(b'-' * 200, PARTITION_KEY)
    sender.run_once()

    assert client.put_records.called
    assert not accumulator.has_records()


def test_flush_if_full(config):
    q = queue.Queue()
    accumulator = RecordAccumulator(RawBuffer, config)
    client = mock.Mock()

    sender = Sender(queue=q, accumulator=accumulator, client=client)

    accumulator.try_append(b'-' * (1024 * 1024 - 1), PARTITION_KEY)
    q.put((b'-' * 50, PARTITION_KEY))
    sender.run_once()

    assert client.put_records.called
    assert accumulator.has_records()
