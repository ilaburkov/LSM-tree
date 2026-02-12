import heapq
import os
import asyncio
from memtable import Memtable
from component import DiskComponent
from bloom_filter import BloomFilter
import struct

class LsmTable:
    async def print_all_keys(self):
        print('--- All keys in disk components ---')
        for level, comps in enumerate(self.levels):
            for comp in reversed(comps):
                print(f'Level {level}, file: {getattr(comp, "path", None)}')
                for k, v in comp.iter_items():
                    print(k, v)

    async def _maybe_merge(self, level):
        while len(self.levels[level]) > self.r:
            async with self.locks[level]:
                next_level = level + 1
                next_dir = os.path.join(self.directory, f"level{next_level}")
                os.makedirs(next_dir, exist_ok=True)
                comp_id = len(os.listdir(next_dir))
                out_path = os.path.join(next_dir, f"comp_{comp_id}.dat")
                await self._merge_components(self.levels[level], out_path)
                for comp in self.levels[level]:
                    comp.file.close()
                    os.remove(comp.path)
                self.levels[level] = []
                if next_level >= len(self.levels):
                    self.levels.append([])
                    self.locks.append(asyncio.Lock())
                self.levels[next_level].insert(0, DiskComponent(out_path))
            await self._maybe_merge(next_level)

    async def _merge_components(self, components, out_path):
        
        iters = [comp.iter_items() for comp in components]
        heap = []
        for idx, it in enumerate(iters):
            try:
                k, v = next(it)
                heap.append((k, idx, v))
            except StopIteration:
                pass
        heapq.heapify(heap)
        offsets_path = out_path + '.offsets.tmp'
        data_path = out_path + '.data.tmp'
        offsets_file = open(offsets_path, 'wb')
        data_file = open(data_path, 'wb')
        offset = 0
        bloom_keys = []
        last_key = None
        num_keys = 0
        while heap:
            k, idx, v = heapq.heappop(heap)
            if k == 'a':
                print(f"Processing key: {k}, value: {v}, from component index: {idx}, last_key: {last_key}, file: {out_path}")
            if last_key == k:
                try:
                    k2, v2 = next(iters[idx])
                    heapq.heappush(heap, (k2, idx, v2))
                except StopIteration:
                    pass
                continue
            k_bytes = k.encode('utf-8')
            v_bytes = v.encode('utf-8')
            chunk = (
                struct.pack('I', len(k_bytes)) + k_bytes +
                struct.pack('I', len(v_bytes)) + v_bytes
            )
            offsets_file.write(struct.pack('Q', offset))
            data_file.write(chunk)
            bloom_keys.append(k)
            last_key = k
            num_keys += 1
            try:
                k2, v2 = next(iters[idx])
                heapq.heappush(heap, (k2, idx, v2))
            except StopIteration:
                pass
            offset += len(chunk)
        offsets_file.close()
        data_file.close()
        bloom_size, num_hashes = BloomFilter.optimal_size(num_keys, 0.01)
        bloom = BloomFilter(bloom_size, num_hashes)
        for k in bloom_keys:
            bloom.add(k)
        bloom_bytes = bloom.serialize()
        with open(out_path, 'wb') as f:
            f.write(struct.pack('II', num_keys, len(bloom_bytes)))
            with open(offsets_path, 'rb') as off_f:
                for _ in range(num_keys):
                    off = struct.unpack('Q', off_f.read(8))[0]
                    f.write(struct.pack('Q', off + 8 + num_keys * 8))
            with open(data_path, 'rb') as data_f:
                while True:
                    chunk = data_f.read(4096)
                    if not chunk:
                        break
                    f.write(chunk)
            f.write(bloom_bytes)
        os.remove(offsets_path)
        os.remove(data_path)
        
    def __init__(self, directory, r=10, l=1000):
        self.directory = directory
        self.r = r 
        self.l = l
        self.memtable = Memtable(l)
        self.levels = []
        self.locks = []
        self._init_storage()
  

    def _init_storage(self):
        os.makedirs(self.directory, exist_ok=True)
        
        self.levels = []
        self.locks = []
        for level in range(10):
            level_dir = os.path.join(self.directory, f"level{level}")
            comps = []
            if os.path.exists(level_dir):
                files = sorted([os.path.join(level_dir, f) for f in os.listdir(level_dir) if f.endswith('.dat')])
                comps = [DiskComponent(f) for f in files]
                comps.sort(key=lambda c: c.extract_num)
                comps = list(reversed(comps))
                print(f"Loaded level {level} components: {[c.path for c in comps]}")
            self.levels.append(comps)
            self.locks.append(asyncio.Lock())

        
    async def insert(self, key: str, value: str):
        flushed = self.memtable.put(key, value)
        if flushed:
            await self._flush_memtable()
   
    async def delete(self, key: str):
        await self.insert(key, '<DELETED>')

    async def get(self, key: str):
        val = self.memtable.get(key)
        if val is not None:
            if val == '<DELETED>':
                return None
            return val
        for level, comps in enumerate(self.levels):
            async with self.locks[level]:
                for comp in comps:
                    v = comp.get(key)
                    if v is not None:
                        if v == '<DELETED>':
                            return None
                        return v
        return None

    async def range(self, start: str, end: str):
        res = self.memtable.range(start, end)
        for level, comps in enumerate(self.levels):
            async with self.locks[level]:
                for comp in comps:
                    res.extend(comp.range(start, end))
        seen = {}
        for k, v in res:
            if k not in seen:
                seen[k] = v
        return [(k, seen[k]) for k in sorted(seen) if seen[k] != '<DELETED>']

    async def _flush_memtable(self):
        items = self.memtable.flush()
        if not items:
            return
        items.sort()
        num_keys = len(items)
        bloom_size, num_hashes = BloomFilter.optimal_size(num_keys, 0.01)
        bloom = BloomFilter(bloom_size, num_hashes)
        offsets = []
        data_chunks = []
        offset = 0
        for k, v in items:
            k_bytes = k.encode('utf-8')
            v_bytes = v.encode('utf-8')
            bloom.add(k)
            chunk = (
                struct.pack('I', len(k_bytes)) + k_bytes +
                struct.pack('I', len(v_bytes)) + v_bytes
            )
            offsets.append(offset)
            data_chunks.append(chunk)
            offset += len(chunk)
        
        level0_dir = os.path.join(self.directory, "level0")
        os.makedirs(level0_dir, exist_ok=True)
        comp_id = len(os.listdir(level0_dir))
        comp_path = os.path.join(level0_dir, f"comp_{comp_id}.dat")
        
        with open(comp_path, 'wb') as f:
            bloom_bytes = bloom.serialize()
            f.write(struct.pack('II', num_keys, len(bloom_bytes)))
            for off in offsets:
                f.write(struct.pack('Q', off + 8 + num_keys * 8))
            for chunk in data_chunks:
                f.write(chunk)
            f.write(bloom_bytes)
        
        async with self.locks[0]:
            if not self.levels:
                self.levels.append([])
                self.locks.append(asyncio.Lock())
            self.levels[0].insert(0, DiskComponent(comp_path))
        
        await self._maybe_merge(0)

    async def flush(self): 
        await self._flush_memtable()
