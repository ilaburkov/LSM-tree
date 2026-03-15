import os
import shutil
import pytest

from text_processor import process as process_text
from inverted_index import InvertedIndex

TEST_DIR = 'testdata_inverted'

def test_process_basic():
    tokens = process_text("The cat sat on the mat")
    assert len(tokens) > 0
    assert all(isinstance(t, str) for t in tokens)


def test_process_removes_stopwords():
    tokens = process_text("the cat and the dog on a mat")
    assert "the" not in tokens
    assert "and" not in tokens
    assert "on" not in tokens
    assert "a" not in tokens


def test_process_stemming():
    t1 = process_text("running")
    t2 = process_text("runs")
    assert t1[0] == t2[0]


def test_process_lowercasing():
    t1 = process_text("Cat")
    t2 = process_text("cat")
    assert t1 == t2

@pytest.mark.asyncio
async def test_add_and_search_single_term():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "The cat sat on the mat")
    await idx.add_document(1, "The dog ran in the yard")
    await idx.add_document(2, "The cat and dog were friends")

    result = await idx.search("cat")
    assert 0 in result
    assert 1 not in result
    assert 2 in result

    result2 = await idx.search("dog")
    assert 0 not in result2
    assert 1 in result2
    assert 2 in result2

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_boolean_and():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "The cat sat on the mat")
    await idx.add_document(1, "The dog ran in the yard")
    await idx.add_document(2, "The cat and dog were friends")

    result = await idx.search("cat AND dog")
    assert result == [2]

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_boolean_or():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "The cat sat")
    await idx.add_document(1, "The dog ran")

    result = await idx.search("cat OR dog")
    assert sorted(result) == [0, 1]

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_boolean_not():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "The cat sat")
    await idx.add_document(1, "The dog ran")
    await idx.add_document(2, "The cat and dog played")

    result = await idx.search("cat AND NOT dog")
    assert result == [0]

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_persistence():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "The cat sat on the mat")
    await idx.add_document(1, "The dog ran in the yard")
    await idx.flush()
    del idx

    idx2 = InvertedIndex(TEST_DIR, r=3, l=100)
    result = await idx2.search("cat")
    assert 0 in result

    result2 = await idx2.search("dog")
    assert 1 in result2

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_compaction():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=2, l=5)

    docs = [
        "cat sits home",
        "dog walks park",
        "cat dog friends",
        "bird flies sky",
        "cat bird tree",
        "dog park ball",
        "fish swims water",
        "cat fish hunt",
        "dog cat fish",
        "bird fish nature",
        "cat house window",
        "dog yard fence",
        "bird nest spring",
        "fish lake quiet",
        "cat dog bird fish",
    ]

    for i, text in enumerate(docs):
        await idx.add_document(i, text)
    await idx.flush()

    cat_docs = await idx.search("cat")
    assert 0 in cat_docs
    assert 2 in cat_docs
    assert 4 in cat_docs
    assert 7 in cat_docs
    assert 8 in cat_docs
    assert 10 in cat_docs
    assert 14 in cat_docs

    result = await idx.search("cat AND dog")
    assert 2 in result
    assert 8 in result
    assert 14 in result

    del idx
    idx2 = InvertedIndex(TEST_DIR, r=2, l=5)
    cat_docs2 = await idx2.search("cat")
    assert sorted(cat_docs2) == sorted(cat_docs)

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_parenthesized_query():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "cat")
    await idx.add_document(1, "dog")
    await idx.add_document(2, "bird")
    await idx.add_document(3, "cat dog")
    await idx.add_document(4, "cat bird")
    await idx.add_document(4, "cat mike")

    result = await idx.search("cat AND (dog OR bird)")
    assert sorted(result) == [3, 4]

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_empty_index():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    result = await idx.search("cat")
    assert result == []

    shutil.rmtree(TEST_DIR)


@pytest.mark.asyncio
async def test_stemming_in_search():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)
    idx = InvertedIndex(TEST_DIR, r=3, l=100)

    await idx.add_document(0, "The runner runs fast")
    await idx.add_document(1, "She was running yesterday")

    result = await idx.search("running")
    assert 0 in result
    assert 1 in result

    shutil.rmtree(TEST_DIR)
