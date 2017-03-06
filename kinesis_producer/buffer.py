from .constants import KINESIS_RECORD_MAX_SIZE


class RawBuffer(object):
    """Record buffer."""

    def __init__(self, config):
        self.size_limit = config['buffer_size_limit']
        self._size = 0
        self._buffer = []

    def try_append(self, record, partition_key):
        """Append a record if possible, return False otherwise."""
        assert self._buffer is not None, 'Buffer is closed!'

        record_length = len(record)

        if self._size + record_length > KINESIS_RECORD_MAX_SIZE:
            return False

        self._buffer.append({
            'Data': record,
            'PartitionKey': partition_key
        })
        self._size += record_length
        return True

    def is_ready(self):
        """Whether the buffer should be flushed."""
        return self._size > self.size_limit

    def flush(self):
        """Return the buffer content and close the buffer."""
        assert self._buffer is not None, 'Buffer is closed!'
        buf = list(self._buffer)
        self._buffer = None
        return buf
