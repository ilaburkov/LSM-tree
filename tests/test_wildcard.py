import os
import shutil
import pytest

from inverted_index import InvertedIndex, _generate_ngrams

TEST_DIR = 'testdata_wildcard'

def test_generate_ngrams_short():
    ngrams = _generate_ngrams("cat")
    assert "^c" in ngrams
    assert "at" in ngrams
    assert "t$" in ngrams
    assert "^ca" in ngrams
    assert "cat" in ngrams
    assert "at$" in ngrams
    assert "^cat" in ngrams
    assert "cat$" in ngrams
    assert "^cat$" in ngrams


def test_generate_ngrams_long():
    ngrams = _generate_ngrams("catalog")
    assert "^c" in ngrams
    assert "g$" in ngrams
    assert "^ca" in ngrams
    assert "og$" in ngrams
    assert "^cat" in ngrams
    assert "log$" in ngrams
    assert "^cata" in ngrams
    assert "alog$" in ngrams


def _doc_ids(result: list[tuple[str, int]]) -> set[int]:
    return {doc_id for _, doc_id in result}


def _terms(result: list[tuple[str, int]]) -> set[str]:
    return {term for term, _ in result}

@pytest.mark.asyncio
async def test_prefix_search_basic():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "cat")
    await idx.add_document(1, "catalog")
    await idx.add_document(2, "category")
    await idx.add_document(3, "dog")

    result = await idx.prefix_search("cat")
    ids = _doc_ids(result)
    assert 0 in ids
    assert 1 in ids
    assert 2 in ids
    assert 3 not in ids
    assert ("cat", 0) in result
    assert ("catalog", 1) in result

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_prefix_search_no_match():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "cat")
    await idx.add_document(1, "dog")

    result = await idx.prefix_search("xyz")
    assert result == []

    shutil.rmtree(TEST_DIR)

@pytest.mark.asyncio
async def test_wildcard_suffix():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "cat")
    await idx.add_document(1, "catalog")
    await idx.add_document(2, "dog")

    result = await idx.wildcard_search("cat*")
    ids = _doc_ids(result)
    assert 0 in ids
    assert 1 in ids
    assert 2 not in ids
    assert ("cat", 0) in result
    assert ("catalog", 1) in result

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_wildcard_prefix():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "creation")
    await idx.add_document(1, "station")
    await idx.add_document(2, "creative")

    result = await idx.wildcard_search("*tion")
    ids = _doc_ids(result)
    assert 0 in ids
    assert 1 in ids
    assert 2 not in ids

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_wildcard_middle():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "cat")
    await idx.add_document(1, "cut")
    await idx.add_document(2, "coat")
    await idx.add_document(3, "car")

    result = await idx.wildcard_search("c*t")
    ids = _doc_ids(result)
    assert 0 in ids
    assert 1 in ids
    assert 2 in ids
    assert 3 not in ids
    assert ("cat", 0) in result

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_wildcard_multiple_stars():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "catalog")
    await idx.add_document(1, "crag")
    await idx.add_document(2, "dog")

    result = await idx.wildcard_search("c*a*g")
    ids = _doc_ids(result)
    assert 0 in ids
    assert 1 in ids
    assert 2 not in ids

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_wildcard_persistence():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "catalog")
    await idx.add_document(1, "category")
    await idx.add_document(2, "dog")
    await idx.flush()
    del idx

    idx2 = InvertedIndex(TEST_DIR, r=3, l=100)
    result = await idx2.prefix_search("cat")
    ids = _doc_ids(result)
    assert 0 in ids
    assert 1 in ids
    assert 2 not in ids

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_wildcard_returns_term_and_doc():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "cat")
    await idx.add_document(1, "catalog")
    await idx.add_document(5, "cat")

    result = await idx.wildcard_search("cat*")
    assert ("cat", 0) in result
    assert ("cat", 5) in result
    assert ("catalog", 1) in result
    terms = _terms(result)
    assert "cat" in terms
    assert "catalog" in terms

    shutil.rmtree(TEST_DIR)
