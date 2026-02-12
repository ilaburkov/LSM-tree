from sortedcontainers import SortedDict
import threading

class Memtable:
    def __init__(self, max_size):
        self.data = SortedDict()
        self.max_size = max_size
        self.lock = threading.Lock()

    def put(self, key: str, value: str):
        with self.lock:
            self.data[key] = value
            return len(self.data) >= self.max_size

    def get(self, key: str):
        with self.lock:
            return self.data.get(key)

    def range(self, start: str, end: str):
        with self.lock:
            idx1 = self.data.bisect_left(start)
            idx2 = self.data.bisect_right(end)
            keys = self.data.islice(idx1, idx2)
            return [(k, self.data[k]) for k in keys]

    def flush(self):
        with self.lock:
            items = list(self.data.items())
            self.data.clear()
            return items
