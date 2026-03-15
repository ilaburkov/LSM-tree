from sortedcontainers import SortedDict
import threading

class Memtable:
    def __init__(self, max_size, merge_fn=None):
        self.data = SortedDict()
        self.max_size = max_size
        self.lock = threading.Lock()
        self.merge_fn = merge_fn

    def put(self, key: str, value: str):
        with self.lock:
            if self.merge_fn and key in self.data:
                self.data[key] = self.merge_fn(self.data[key], value)
            else:
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
