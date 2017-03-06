import pytest

from kinesis_producer.buffer import RawBuffer

CONFIG = {
    'buffer_size_limit': 100,
}

PARTITION_KEY = 'bogus'

def test_append():
    buf = RawBuffer(CONFIG)

    input_values = [
        b'123',
        b'456',
        b'789',
    ]
    for input_value in input_values:
        buf.try_append(input_value, PARTITION_KEY)

    values = buf.flush()

    for input_value, output_value in zip(input_values, values):
        output_value['Data'] == input_value


def test_is_ready():
    buf = RawBuffer(CONFIG)

    buf.try_append(b'-' * 100, PARTITION_KEY)

    assert not buf.is_ready()

    buf.try_append(b'-', PARTITION_KEY)

    assert buf.is_ready()


def test_try_append_response():
    buf = RawBuffer(CONFIG)

    success = buf.try_append(b'-', PARTITION_KEY)
    assert success

    # Over buffer_size_limit
    msg = b'-' * 1024
    success = buf.try_append(msg, PARTITION_KEY)
    assert success

    # Over Kinesis record limit
    msg = b'-' * (1024 * 1023)
    success = buf.try_append(msg, PARTITION_KEY)
    assert not success


def test_closed():
    buf = RawBuffer(CONFIG)
    buf.flush()

    with pytest.raises(AssertionError):
        buf.try_append(b'-', PARTITION_KEY)

    with pytest.raises(AssertionError):
        buf.flush()
