import os
import shutil
import pytest

from text_processor import process_with_positions
from inverted_index import InvertedIndex

TEST_DIR = 'testdata_phrase'

def test_positions_basic():
    result, total = process_with_positions("the cat sat on the mat")
    stems = [s for s, _ in result]
    positions = [p for _, p in result]
    assert "cat" in stems
    assert "sat" in stems
    assert "mat" in stems
    assert positions == [1, 2, 5]
    assert total == 6


def test_positions_offset():
    result, total = process_with_positions("cat dog", offset=10)
    assert result == [("cat", 10), ("dog", 11)]
    assert total == 12


def test_positions_stopwords_skipped():
    result, _ = process_with_positions("the and a")
    assert result == []

@pytest.mark.asyncio
async def test_phrase_adjacent():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "the cat sat on the mat")
    await idx.add_document(1, "the dog sat on the mat")

    result = await idx.phrase_search("cat sat")
    assert result == [0]

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_phrase_not_adjacent():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "the cat ran and sat down")

    result = await idx.phrase_search("cat sat")
    assert result == []

    result2 = await idx.phrase_search("cat ran a sat")
    assert result2 == [0]

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_phrase_three_words():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "big black cat sat quietly")
    await idx.add_document(1, "big white cat sat loudly")
    await idx.add_document(2, "big black dog ran fast")

    result = await idx.phrase_search("big black cat")
    assert result == [0]

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_phrase_multiple_matches():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "cat sat here")
    await idx.add_document(1, "cat sat there")
    await idx.add_document(2, "dog sat here")

    result = await idx.phrase_search("cat sat")
    assert sorted(result) == [0, 1]

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_phrase_single_word():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "cat")
    await idx.add_document(1, "dog")

    result = await idx.phrase_search("cat")
    assert result == [0]

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_phrase_with_stopwords():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "cat sat on the mat")
    await idx.add_document(1, "cat sat big mat")

    result = await idx.phrase_search("sat on the mat")
    assert result == [0]

    shutil.rmtree(TEST_DIR)

@pytest.mark.asyncio
async def test_boolean_with_phrase():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "the cat sat on the mat")
    await idx.add_document(1, "the cat sat and played with a dog")
    await idx.add_document(2, "the dog sat on the mat")

    result = await idx.search('"cat sat" AND dog')
    assert result == [1]

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_boolean_phrase_or():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "cat sat here")
    await idx.add_document(1, "dog ran there")

    result = await idx.search('"cat sat" OR "dog ran"')
    assert sorted(result) == [0, 1]

    shutil.rmtree(TEST_DIR)

@pytest.mark.asyncio
async def test_phrase_persistence():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "the cat sat on the mat")
    await idx.add_document(1, "the dog ran in the yard")
    await idx.flush()
    del idx

    idx2 = InvertedIndex(TEST_DIR, r=3, l=100)
    result = await idx2.phrase_search("cat sat")
    assert result == [0]

    result2 = await idx2.phrase_search("dog ran")
    assert result2 == [1]

    shutil.rmtree(TEST_DIR)
