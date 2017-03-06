import time

from kinesis_producer.accumulator import RecordAccumulator
from kinesis_producer.partitioner import random_partitioner
from kinesis_producer.buffer import RawBuffer


CONFIG = {
    'buffer_time_limit': 0.1,
    'buffer_size_limit': 100,
}


def test_append():
    acc = RecordAccumulator(RawBuffer, CONFIG, random_partitioner)
    success = acc.try_append(b'-')
    assert success

    acc.flush()
    success = acc.try_append(b'-')
    assert success


def test_append_over_buffer_size():
    acc = RecordAccumulator(RawBuffer, CONFIG, random_partitioner)
    success = acc.try_append(b'-' * 200)
    assert success
    assert acc.is_ready()


def test_append_over_kinesis_record_size():
    acc = RecordAccumulator(RawBuffer, CONFIG, random_partitioner)
    success = acc.try_append(b'-' * (1024 * 1024 + 1))
    assert not success

    acc.flush()
    success = acc.try_append(b'-')
    assert success


def test_append_timeout():
    acc = RecordAccumulator(RawBuffer, CONFIG, random_partitioner)
    acc.try_append(b'-')
    time.sleep(0.2)
    acc.try_append(b'-')
    assert acc.is_ready()

    acc.flush()
    assert not acc.is_ready()


def test_append_empty_timeout():
    acc = RecordAccumulator(RawBuffer, CONFIG, random_partitioner)
    time.sleep(0.2)
    assert not acc.is_ready()


def test_has_record():
    acc = RecordAccumulator(RawBuffer, CONFIG, random_partitioner)
    assert not acc.has_records()

    acc.try_append(b'-')
    assert acc.has_records()

    acc.flush()
    assert not acc.has_records()

    acc.try_append(b'-')
    assert acc.has_records()


def test_flush():
    acc = RecordAccumulator(RawBuffer, CONFIG, random_partitioner)
    input_records = [
        b'123',
        b'456',
        b'789',
    ]
    for record in input_records:
        acc.try_append(record)
    output_records = acc.flush()
    for input_rec, output_rec in zip(input_records, output_records):
        assert input_rec == output_rec['Data']

    acc.try_append(b'ABC')
    assert acc.flush()[0]['Data'] == b'ABC'
