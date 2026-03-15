import os
import shutil
import pytest

from pyroaring import BitMap

from inverted_index import (
    InvertedIndex,
    BSI_BITS,
    _bsi_range_gte,
    _bsi_range_lte,
    _bsi_range_between,
)

TEST_DIR = 'testdata_date'


def _build_slices(values: dict[int, int]) -> tuple[list[BitMap], BitMap]:
    slices = [BitMap() for _ in range(BSI_BITS)]
    universe = BitMap()
    for doc_id, val in values.items():
        universe.add(doc_id)
        for i in range(BSI_BITS):
            if (val >> i) & 1:
                slices[i].add(doc_id)
    return slices, universe

def test_bsi_range_gte():
    slices, universe = _build_slices({0: 10, 1: 20, 2: 30, 3: 5})
    result = _bsi_range_gte(slices, universe, 15)
    assert sorted(result) == [1, 2]


def test_bsi_range_lte():
    slices, universe = _build_slices({0: 10, 1: 20, 2: 30, 3: 5})
    result = _bsi_range_lte(slices, universe, 20)
    assert sorted(result) == [0, 1, 3]


def test_bsi_range_between():
    slices, universe = _build_slices({0: 10, 1: 20, 2: 30, 3: 5})
    result = _bsi_range_between(slices, universe, 10, 25)
    assert sorted(result) == [0, 1]


def test_bsi_exact_match():
    slices, universe = _build_slices({0: 100, 1: 200, 2: 100})
    gte = _bsi_range_gte(slices, universe, 100)
    lte = _bsi_range_lte(slices, universe, 100)
    exact = gte & lte
    assert sorted(exact) == [0, 2]


def test_bsi_empty_universe():
    slices = [BitMap() for _ in range(BSI_BITS)]
    result = _bsi_range_gte(slices, BitMap(), 0)
    assert len(result) == 0

@pytest.mark.asyncio
async def test_appeared_basic():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "cat", start_date=100)
    await idx.add_document(1, "dog", start_date=200)
    await idx.add_document(2, "bird", start_date=300)

    result = await idx.search_appeared(150, 250)
    assert result == [1]

    result_all = await idx.search_appeared(50, 350)
    assert result_all == [0, 1, 2]

    result_none = await idx.search_appeared(400, 500)
    assert result_none == []

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_appeared_no_dates():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "cat")
    await idx.add_document(1, "dog", start_date=100)
    await idx.add_document(2, "bird", end_date=100)

    result = await idx.search_appeared(0, 999)
    assert result == [1]

    shutil.rmtree(TEST_DIR)

@pytest.mark.asyncio
async def test_search_appeared():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "cat", start_date=100)
    await idx.add_document(1, "dog", start_date=200)
    await idx.add_document(2, "bird", start_date=300)

    result = await idx.search_appeared(100, 200)
    assert result == [0, 1]

    result2 = await idx.search_appeared(250, 350)
    assert result2 == [2]

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_search_valid_no_end():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "cat", start_date=100)
    await idx.add_document(1, "dog", start_date=200)

    result = await idx.search_valid(300, 400)
    assert result == [0, 1]

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_search_valid_with_end():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "1", start_date=100, end_date=200)
    await idx.add_document(1, "2", start_date=100, end_date=500)
    await idx.add_document(2, "3", start_date=350)
    await idx.add_document(3, "4", start_date=401)
    await idx.add_document(4, "5", start_date=400)
    await idx.add_document(5, "6", start_date=200, end_date=500)
    await idx.add_document(6, "7", start_date=200, end_date=350)
    await idx.add_document(7, "8", start_date=100)

    result = await idx.search_valid(300, 400)
    assert 0 not in result
    assert 1 in result
    assert 2 not in result
    assert 3 not in result
    assert 4 not in result
    assert 5 in result
    assert 6 not in result
    assert 7 in result

    shutil.rmtree(TEST_DIR)

@pytest.mark.asyncio
async def test_boolean_with_appeared():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "cat", start_date=100)
    await idx.add_document(1, "cat", start_date=300)
    await idx.add_document(2, "dog", start_date=100)

    result = await idx.search("cat AND APPEARED(50, 200)")
    assert result == [0]

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_boolean_with_valid():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "cat", start_date=100, end_date=200)
    await idx.add_document(1, "cat", start_date=100, end_date=500)
    await idx.add_document(2, "dog", start_date=100)

    result = await idx.search("cat AND VALID(300, 400)")
    assert result == [1]

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_boolean_with_appeared_range():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "cat", start_date=100)
    await idx.add_document(1, "cat", start_date=300)
    await idx.add_document(2, "dog", start_date=250)

    result = await idx.search("cat OR dog AND APPEARED(200, 400)")
    assert result == [1, 2]

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_boolean_not_appeared():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "cat", start_date=100)
    await idx.add_document(1, "cat", start_date=300)
    await idx.add_document(2, "catiii", start_date=300)

    result = await idx.search("cat AND NOT APPEARED(200, 400)")
    assert result == [0]

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_boolean_complex_date():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "cat", start_date=100, end_date=200)
    await idx.add_document(1, "dog", start_date=100, end_date=500)
    await idx.add_document(3, "dog", start_date=100, end_date=350)
    await idx.add_document(2, "bird", start_date=100, end_date=500)

    result = await idx.search("(cat OR dog) AND VALID(300, 400)")
    assert result == [1]

    shutil.rmtree(TEST_DIR)

@pytest.mark.asyncio
async def test_date_persistence():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "cat", start_date=100, end_date=300)
    await idx.add_document(1, "dog", start_date=200)
    await idx.flush()
    del idx
    idx2 = InvertedIndex(TEST_DIR, r=3, l=100)

    result = await idx2.search_appeared(50, 150)
    assert result == [0]

    result2 = await idx2.search_valid(250, 350)
    assert 0 not in result2
    assert 1 in result2

    result3 = await idx2.search("cat AND APPEARED(50, 150)")
    assert result3 == [0]

    shutil.rmtree(TEST_DIR)
