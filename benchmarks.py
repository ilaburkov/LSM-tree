
import os
import shutil
import time
import asyncio
from lsm_table import LsmTable

TEST_DIR = 'benchdata_lsm'
N = 100000

def sync_benchmark():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    table = LsmTable(TEST_DIR, r=4, l=1000)
    t0 = time.time()
    loop = asyncio.get_event_loop()
    for i in range(N):
        loop.run_until_complete(table.insert(f"key{i}", f"val{i}"))
    t1 = time.time()
    print(f"[SYNC] Insert {N}: {t1-t0:.3f}s")
    t0 = time.time()
    for i in range(N):
        v = loop.run_until_complete(table.get(f"key{i}"))
        assert v == f"val{i}"
    t1 = time.time()
    print(f"[SYNC] Get {N}: {t1-t0:.3f}s")
    t0 = time.time()
    res = loop.run_until_complete(table.range(f"key100", f"key199"))
    t1 = time.time()
    print(f"[SYNC] Range 100: {t1-t0:.3f}s, found {len(res)}")
    shutil.rmtree(TEST_DIR)

async def async_benchmark():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    table = LsmTable(TEST_DIR, r=4, l=1000)
    t0 = time.time()
    for i in range(N):
        await table.insert(f"key{i}", f"val{i}")
    t1 = time.time()
    print(f"[ASYNC] Insert {N}: {t1-t0:.3f}s")
    t0 = time.time()
    for i in range(N):
        v = await table.get(f"key{i}")
        assert v == f"val{i}"
    t1 = time.time()
    print(f"[ASYNC] Get {N}: {t1-t0:.3f}s")
    t0 = time.time()
    res = await table.range(f"key100", f"key199")
    t1 = time.time()
    print(f"[ASYNC] Range 100: {t1-t0:.3f}s, found {len(res)}")
    shutil.rmtree(TEST_DIR)

async def async_parallel_insert_benchmark():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    table = LsmTable(TEST_DIR, r=4, l=1000)
    t0 = time.time()
    tasks = [table.insert(f"key{i}", f"val{i}") for i in range(N)]
    await asyncio.gather(*tasks)
    t1 = time.time()
    print(f"[ASYNC PARALLEL] Insert {N}: {t1-t0:.3f}s")
    t0 = time.time()
    tasks = [table.get(f"key{i}") for i in range(N)]
    results = await asyncio.gather(*tasks)
    for i, v in enumerate(results):
        assert v == f"val{i}"
    t1 = time.time()
    print(f"[ASYNC PARALLEL] Get {N}: {t1-t0:.3f}s")
    shutil.rmtree(TEST_DIR)
        
if __name__ == "__main__":
    print("--- SYNC BENCHMARK ---")
    sync_benchmark()
    print("--- ASYNC BENCHMARK ---")
    asyncio.run(async_benchmark())
    print("--- ASYNC PARALLEL INSERT BENCHMARK ---")
    asyncio.run(async_parallel_insert_benchmark())
# Бенчмарки для LSM-дерева
