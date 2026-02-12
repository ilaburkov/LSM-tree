import math
import hashlib
import struct
import threading

class BloomFilter:
    def __init__(self, size: int, num_hashes: int, seeds=None, bitarray=None):
        self.size = size
        self.num_hashes = num_hashes
        self.lock = threading.Lock()
        if seeds is None:
            self.seeds = [i * 179179 + 179 for i in range(num_hashes)]
        else:
            self.seeds = seeds
        if bitarray is None:
            self.bitarray = bytearray((size + 7) // 8)
        else:
            self.bitarray = bitarray

    def _hashes(self, key: str):
        key_bytes = key.encode('utf-8')
        for seed in self.seeds:
            h = hashlib.blake2b(key_bytes, digest_size=8, key=seed.to_bytes(8, 'little'))
            yield int.from_bytes(h.digest(), 'little') % self.size

    def add(self, key: str):
        with self.lock:
            for pos in self._hashes(key):
                self.bitarray[pos // 8] |= 1 << (pos % 8)

    def __contains__(self, key: str):
        for pos in self._hashes(key):
            if not (self.bitarray[pos // 8] & (1 << (pos % 8))):
                return False
        return True

    def serialize(self) -> bytes:
        data = struct.pack('II', self.size, self.num_hashes)
        for seed in self.seeds:
            data += struct.pack('Q', seed)
        data += self.bitarray
        return data

    @classmethod
    def deserialize(cls, data: bytes):
        size, num_hashes = struct.unpack('II', data[:8])
        seeds = [struct.unpack('Q', data[8 + i*8:16 + i*8])[0] for i in range(num_hashes)]
        bitarray = bytearray(data[8 + num_hashes*8:])
        return cls(size, num_hashes, seeds, bitarray)

    @staticmethod
    def optimal_size(n, p):
        m = -n * math.log(p) / (math.log(2) ** 2)
        k = int(m / n * math.log(2)) + 1
        return int(m), k

    def merge(self, other):
        assert self.size == other.size and self.num_hashes == other.num_hashes and self.seeds == other.seeds
        with self.lock:
            for i in range(len(self.bitarray)):
                self.bitarray[i] |= other.bitarray[i]
