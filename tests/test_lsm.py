import os
import shutil
import asyncio
import pytest
from lsm_table import LsmTable

TEST_DIR = 'testdata_lsm'

@pytest.mark.asyncio
async def test_insert_and_read():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    table = LsmTable(TEST_DIR, r=2, l=3)
    await table.insert('a', '1')
    await table.insert('b', '2')
    await table.insert('c', '3')
    await table.insert('d', '4')
    await table.insert('e', '5')
    await table.insert('f', '6')
    assert await table.get('a') == '1'
    assert await table.get('e') == '5'
    assert await table.get('z') is None
    res = await table.range('b', 'e')
    keys = [k for k, v in res]
    assert keys == ['b', 'c', 'd', 'e']
    del table
    table2 = LsmTable(TEST_DIR, r=2, l=3)
    assert await table2.get('a') == '1'
    assert await table2.get('f') == '6'
    shutil.rmtree(TEST_DIR)

@pytest.mark.asyncio
async def test_merge():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    table = LsmTable(TEST_DIR, r=2, l=2)
    await table.insert('a', 'x')
    await table.insert('b', 'y')
    await table.insert('c', 'z')
    await table.insert('d', 'w')
    assert await table.get('a') == 'x'
    assert await table.get('d') == 'w'
    del table
    table2 = LsmTable(TEST_DIR, r=2, l=2)
    assert await table2.get('b') == 'y'
    assert await table2.get('c') == 'z'
    shutil.rmtree(TEST_DIR)

@pytest.mark.asyncio
async def test_empty():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    table = LsmTable(TEST_DIR, r=2, l=2)
    assert await table.get('a') is None
    assert await table.range('a', 'z') == []
    shutil.rmtree(TEST_DIR)
 


@pytest.mark.asyncio
async def test_delete():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    table = LsmTable(TEST_DIR, r=2, l=3)
    await table.insert('a', '1')
    await table.delete('a')
    assert await table.get('a') is None
    await table.flush()
    del table
    table2 = LsmTable(TEST_DIR, r=2, l=3)
    assert await table2.get('a') is None
    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_rewrite():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    table = LsmTable(TEST_DIR, r=2, l=3)
    await table.insert('a', '1')
    await table.insert('a', '2')
    assert await table.get('a') == '2'
    await table.flush()
    del table
    table2 = LsmTable(TEST_DIR, r=2, l=3)
    assert await table2.get('a') == '2'
    shutil.rmtree(TEST_DIR)

@pytest.mark.asyncio
async def test_large():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    table = LsmTable(TEST_DIR, r=3, l=10)
    N = 1000
    for i in range(N):
        await table.insert(f"key{i:03d}", f"val_bad{i:03d}")
    for i in range(N):
        await table.insert(f"key{i:03d}", f"val{i:03d}")
    for i in range(N):
        assert await table.get(f"key{i:03d}") == f"val{i:03d}"
    res = await table.range("key010", "key020")
    keys = [k for k, v in res]
    assert keys == [f"key{j:03d}" for j in range(10, 21)]
    await table.flush()
    del table
    table2 = LsmTable(TEST_DIR, r=3, l=10)
    for i in range(N):
        assert await table2.get(f"key{i:03d}") == f"val{i:03d}"
    shutil.rmtree(TEST_DIR)
    
