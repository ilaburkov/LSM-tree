import os
import struct
from bloom_filter import BloomFilter

class DiskComponent:
    def __init__(self, path):
        self.path = path
        self.file = open(path, 'rb')
        self._read_header()
        self._load_bloom()

    @property
    def extract_num(self):
        import re
        m = re.search(r'comp_(\d+)\.dat', os.path.basename(self.path))
        return int(m.group(1)) if m else -1

    def _read_header(self):
        self.file.seek(0)
        header = self.file.read(8)
        self.num_keys, self.bloom_size = struct.unpack('II', header)
        self.offsets_start = 8

    def _load_bloom(self):
        self.file.seek(-self.bloom_size, os.SEEK_END)
        bloom_bytes = self.file.read(self.bloom_size)
        self.bloom = BloomFilter.deserialize(bloom_bytes)

    def _get_offset(self, idx):
        pos = self.offsets_start + idx * 8
        self.file.seek(pos)
        return struct.unpack('Q', self.file.read(8))[0]

    def _read_key_value(self, offset):
        self.file.seek(offset)
        key_len = struct.unpack('I', self.file.read(4))[0]
        key = self.file.read(key_len).decode('utf-8')
        value_len = struct.unpack('I', self.file.read(4))[0]
        value = self.file.read(value_len).decode('utf-8')
        return key, value

    def iter_index(self):
        for idx in range(self.num_keys):
            offset = self._get_offset(idx)
            key, _ = self._read_key_value(offset)
            yield key, offset

    def iter_items(self):
        for idx in range(self.num_keys):
            offset = self._get_offset(idx)
            key, value = self._read_key_value(offset)
            yield key, value

    def get(self, key):
        if key not in self.bloom:
            return None
        l, r = 0, self.num_keys - 1
        while l <= r:
            m = (l + r) // 2
            offset = self._get_offset(m)
            k, value = self._read_key_value(offset)
            if k == key:
                return value
            elif k < key:
                l = m + 1
            else:
                r = m - 1
        return None

    def range(self, start, end):
        res = []
        for idx in range(self.num_keys):
            offset = self._get_offset(idx)
            k, value = self._read_key_value(offset)
            if start <= k <= end:
                res.append((k, value))
        return res

    def close(self):
        self.file.close()
