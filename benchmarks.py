
import os
import shutil
import time
import asyncio
from lsm_table import LsmTable
import random

TEST_DIR = 'benchdata_lsm'
N = 100000
range_size = 100
num_ranges = 10000
random.seed(179)

def generage_queries():
    range_queries = []
    for _ in range(num_ranges):
        start = random.randint(0, N - range_size)
        end = start + range_size - 1
        range_queries.append((f"key{start:06}", f"key{end:06}"))
    return range_queries

def sync_benchmark():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    table = LsmTable(TEST_DIR, r=4, l=1000)
    t0 = time.time()
    loop = asyncio.get_event_loop()
    for i in range(N):
        loop.run_until_complete(table.insert(f"key{i:06}", f"val{i}"))
    t1 = time.time()
    print(f"[SYNC] Insert {N}: {t1-t0:.3f}s")
    t0 = time.time()
    for i in range(N):
        v = loop.run_until_complete(table.get(f"key{i:06}"))
        assert v == f"val{i}"
    t1 = time.time()
    print(f"[SYNC] Get {N}: {t1-t0:.3f}s")
    range_queries = generage_queries()
    t0 = time.time()
    for start, end in range_queries:
        res = loop.run_until_complete(table.range(start, end))
        assert len(res) <= range_size
    print(f"[SYNC] range x{num_ranges} of size {range_size}: {time.time() - t0:.3f}s")
    shutil.rmtree(TEST_DIR)

async def async_benchmark():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    table = LsmTable(TEST_DIR, r=4, l=1000)
    t0 = time.time()
    for i in range(N):
        await table.insert(f"key{i:06}", f"val{i}")
    t1 = time.time()
    print(f"[ASYNC] Insert {N}: {t1-t0:.3f}s")
    t0 = time.time()
    for i in range(N):
        v = await table.get(f"key{i:06}")
        assert v == f"val{i}"
    t1 = time.time()
    print(f"[ASYNC] Get {N}: {t1-t0:.3f}s")
    t0 = time.time()
    res = await table.range(f"key100", f"key199")
    t1 = time.time()

    range_queries = generage_queries()
    t0 = time.time()
    for start, end in range_queries:
        res = await table.range(start, end)
        assert len(res) <= range_size
    print(f"[ASYNC] range x{num_ranges} of size {range_size}: {time.time() - t0:.3f}s")
    shutil.rmtree(TEST_DIR)

async def async_parallel_insert_benchmark():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    table = LsmTable(TEST_DIR, r=4, l=1000)
    t0 = time.time()
    tasks = [table.insert(f"key{i:06}", f"val{i}") for i in range(N)]
    await asyncio.gather(*tasks)
    t1 = time.time()
    print(f"[ASYNC PARALLEL] Insert {N}: {t1-t0:.3f}s")
    t0 = time.time()
    tasks = [table.get(f"key{i:06}") for i in range(N)]
    results = await asyncio.gather(*tasks)
    for i, v in enumerate(results):
        assert v == f"val{i}"
    t1 = time.time()
    print(f"[ASYNC PARALLEL] Get {N}: {t1-t0:.3f}s")
    range_queries = generage_queries()
    t0 = time.time()
    results = await asyncio.gather(*[table.range(start, end) for start, end in range_queries])
    for res in results:
        assert len(res) <= range_size
    print(f"[ASYNC PARALLEL] range x{num_ranges} of size {range_size}: {time.time() - t0:.3f}s")
    shutil.rmtree(TEST_DIR)
        
if __name__ == "__main__":
    print("--- SYNC BENCHMARK ---")
    sync_benchmark()
    print("--- ASYNC BENCHMARK ---")
    asyncio.run(async_benchmark())
    print("--- ASYNC PARALLEL INSERT BENCHMARK ---")
    asyncio.run(async_parallel_insert_benchmark())
# Бенчмарки для LSM-дерева
