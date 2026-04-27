import os
import re
import json
import base64
import fnmatch

from pyroaring import BitMap

from lsm_table import LsmTable
from text_processor import (
    process as process_text,
    process_with_original,
    process_with_positions,
)


def _encode_bitmap(bm: BitMap) -> str:
    return base64.b64encode(bm.serialize()).decode('ascii')


def _decode_bitmap(s: str) -> BitMap:
    return BitMap.deserialize(base64.b64decode(s))


def _bitmap_merge(a: str, b: str) -> str:
    return _encode_bitmap(_decode_bitmap(a) | _decode_bitmap(b))


def _pairs_merge(a: str, b: str) -> str:
    pairs = set(a.split('\n')) | set(b.split('\n'))
    return '\n'.join(sorted(pairs))


def _positions_merge(a: str, b: str) -> str:
    da = json.loads(a)
    db = json.loads(b)
    for doc_id, positions in db.items():
        if doc_id in da:
            da[doc_id] = sorted(set(da[doc_id]) | set(positions))
        else:
            da[doc_id] = positions
    return json.dumps(da, separators=(',', ':'))


def _generate_ngrams(term: str) -> list[str]:
    padded = '^' + term + '$'
    ngrams = []
    for k in (2, 3, 4, 5):
        for i in range(len(padded) - k + 1):
            ngrams.append(padded[i:i + k])
    return ngrams


BSI_BITS = 32

def _bsi_range_gte(slices: list[BitMap], universe: BitMap, x: int) -> BitMap:
    gt = BitMap()
    eq = BitMap(universe)
    for i in range(BSI_BITS - 1, -1, -1):
        if (x >> i) & 1:
            eq = eq & slices[i]
        else:
            gt = gt | (eq & slices[i])
            eq = eq - slices[i]
    return gt | eq


def _bsi_range_lte(slices: list[BitMap], universe: BitMap, x: int) -> BitMap:
    lt = BitMap()
    eq = BitMap(universe)
    for i in range(BSI_BITS - 1, -1, -1):
        if (x >> i) & 1:
            lt = lt | (eq - slices[i])
            eq = eq & slices[i]
        else:
            eq = eq - slices[i]
    return lt | eq


def _bsi_range_between(slices: list[BitMap], universe: BitMap,
                       lo: int, hi: int) -> BitMap:
    return _bsi_range_gte(slices, universe, lo) & _bsi_range_lte(slices, universe, hi)


_QUERY_TOKEN_RE = re.compile(
    r'\s*"([^"]+)"\s*|\s*(AND|OR|NOT|[()])\s*|\s*(\w+)\s*',
    re.UNICODE | re.IGNORECASE,
)


def _tokenize_query(query: str) -> list[str]:
    tokens: list[str] = []
    for m in _QUERY_TOKEN_RE.finditer(query):
        phrase = m.group(1)
        if phrase is not None:
            tokens.append(f'"{phrase}"')
        else:
            tok = m.group(2) or m.group(3)
            if tok is not None:
                tokens.append(tok)
    return tokens


class _QueryNode:
    pass


class _TermNode(_QueryNode):
    def __init__(self, term: str):
        self.term = term


class _NotNode(_QueryNode):
    def __init__(self, child: _QueryNode):
        self.child = child


class _BinOpNode(_QueryNode):
    def __init__(self, op: str, left: _QueryNode, right: _QueryNode):
        self.op = op
        self.left = left
        self.right = right


class _PhraseNode(_QueryNode):
    def __init__(self, phrase: str):
        self.phrase = phrase


class _DateRangeNode(_QueryNode):
    def __init__(self, kind: str, date_from: int, date_to: int):
        self.kind = kind
        self.date_from = date_from
        self.date_to = date_to


class QueryParser:
    def __init__(self, tokens: list[str]):
        self.tokens = tokens
        self.pos = 0

    def _peek(self) -> str | None:
        if self.pos < len(self.tokens):
            return self.tokens[self.pos]
        return None

    def _consume(self) -> str:
        tok = self.tokens[self.pos]
        self.pos += 1
        return tok

    def parse(self) -> _QueryNode:
        return self._parse_expr()

    def _parse_expr(self) -> _QueryNode:
        left = self._parse_term()
        while self._peek() and self._peek().upper() in ('AND', 'OR'):
            op = self._consume().upper()
            right = self._parse_term()
            left = _BinOpNode(op, left, right)
        return left

    def _parse_term(self) -> _QueryNode:
        if self._peek() and self._peek().upper() == 'NOT':
            self._consume()
            return _NotNode(self._parse_term())
        return self._parse_atom()

    def _parse_atom(self) -> _QueryNode:
        if self._peek() == '(':
            self._consume()
            node = self._parse_expr()
            if self._peek() == ')':
                self._consume()
            return node
        word = self._consume()
        if word.startswith('"') and word.endswith('"'):
            return _PhraseNode(word[1:-1])
        upper = word.upper()
        if upper in ('VALID', 'APPEARED') and self._peek() == '(':
            self._consume()
            date_from = int(self._consume())
            date_to = int(self._consume())
            if self._peek() == ')':
                self._consume()
            return _DateRangeNode(upper, date_from, date_to)
        stems = process_text(word)
        stem = stems[0] if stems else word.lower()
        return _TermNode(stem)

class InvertedIndex:
    def __init__(self, directory: str, r: int = 10, l: int = 1000):
        self.directory = directory
        self.lsm = LsmTable(directory, r=r, l=l, merge_fn=_bitmap_merge)
        kgram_dir = os.path.join(directory, 'kgram')
        self.kgram_lsm = LsmTable(kgram_dir, r=r, l=l, merge_fn=_pairs_merge)
        bsi_dir = os.path.join(directory, 'bsi')
        self.bsi_lsm = LsmTable(bsi_dir, r=r, l=l, merge_fn=_bitmap_merge)
        pos_dir = os.path.join(directory, 'pos')
        self.pos_lsm = LsmTable(pos_dir, r=r, l=l, merge_fn=_positions_merge)
        self.all_docs = BitMap()
        self._load_all_docs()

    def _all_docs_path(self) -> str:
        return os.path.join(self.directory, "all_docs.bin")

    def _load_all_docs(self):
        p = self._all_docs_path()
        if os.path.exists(p):
            with open(p, 'rb') as f:
                self.all_docs = BitMap.deserialize(f.read())

    def _save_all_docs(self):
        with open(self._all_docs_path(), 'wb') as f:
            f.write(self.all_docs.serialize())

    async def add_document(self, doc_id: int, text: str,
                           start_date: int | None = None,
                           end_date: int | None = None,
                           _pos_offset: int = 0) -> int:
        self.all_docs.add(doc_id)
        pairs = process_with_original(text)
        seen_stems: set[str] = set()
        seen_words: set[str] = set()
        for original, stem in pairs:
            if stem not in seen_stems:
                seen_stems.add(stem)
                bm = BitMap([doc_id])
                await self.lsm.insert(stem, _encode_bitmap(bm))
            if original not in seen_words:
                seen_words.add(original)
                for ngram in _generate_ngrams(original):
                    await self.kgram_lsm.insert(ngram, f"{original}\t{doc_id}")
        terms_with_pos, new_offset = process_with_positions(text, _pos_offset)
        stem_positions: dict[str, list[int]] = {}
        for stem, pos in terms_with_pos:
            stem_positions.setdefault(stem, []).append(pos)
        doc_key = str(doc_id)
        for stem, positions in stem_positions.items():
            pos_data = json.dumps({doc_key: positions}, separators=(',', ':'))
            await self.pos_lsm.insert(stem, pos_data)
        if start_date is not None:
            await self._index_bsi(doc_id, "start", start_date)
        if end_date is not None:
            await self._index_bsi(doc_id, "end", end_date)
        return new_offset

    async def _index_bsi(self, doc_id: int, prefix: str, value: int):
        encoded = _encode_bitmap(BitMap([doc_id]))
        await self.bsi_lsm.insert(f"has_{prefix}", encoded)
        for i in range(BSI_BITS):
            if (value >> i) & 1:
                await self.bsi_lsm.insert(f"{prefix}:{i}", encoded)

    async def add_file(self, doc_id: int, path: str,
                       start_date: int | None = None,
                       end_date: int | None = None):
        if start_date is not None:
            await self._index_bsi(doc_id, "start", start_date)
        if end_date is not None:
            await self._index_bsi(doc_id, "end", end_date)
        self.all_docs.add(doc_id)
        offset = 0
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                offset = await self.add_document(doc_id, line, _pos_offset=offset)

    async def get_posting(self, term: str) -> BitMap:
        val = await self.lsm.get(term)
        if val is None:
            return BitMap()
        return _decode_bitmap(val)

    async def search(self, query: str) -> list[int]:
        tokens = _tokenize_query(query)
        if not tokens:
            return []
        ast = QueryParser(tokens).parse()
        result = await self._evaluate(ast)
        return sorted(result)

    async def _evaluate(self, node: _QueryNode) -> BitMap:
        if isinstance(node, _TermNode):
            return await self.get_posting(node.term)
        if isinstance(node, _NotNode):
            child = await self._evaluate(node.child)
            return self.all_docs - child
        if isinstance(node, _BinOpNode):
            left = await self._evaluate(node.left)
            right = await self._evaluate(node.right)
            if node.op == 'AND':
                return left & right
            return left | right
        if isinstance(node, _PhraseNode):
            return await self._phrase_bitmap(node.phrase)
        if isinstance(node, _DateRangeNode):
            if node.kind == 'VALID':
                return await self._valid_bitmap(node.date_from, node.date_to)
            if node.kind == 'APPEARED':
                return await self._appeared_bitmap(node.date_from, node.date_to)
        return BitMap()

    async def _fetch_bsi_slices(self, prefix: str) -> list[BitMap]:
        slices = []
        for i in range(BSI_BITS):
            val = await self.bsi_lsm.get(f"{prefix}:{i}")
            slices.append(_decode_bitmap(val) if val else BitMap())
        return slices

    async def _fetch_has(self, prefix: str) -> BitMap:
        val = await self.bsi_lsm.get(f"has_{prefix}")
        return _decode_bitmap(val) if val else BitMap()

    async def _appeared_bitmap(self, date_from: int, date_to: int) -> BitMap:
        has_start = await self._fetch_has("start")
        if not has_start:
            return BitMap()
        slices = await self._fetch_bsi_slices("start")
        return _bsi_range_between(slices, has_start, date_from, date_to)

    async def _valid_bitmap(self, date_from: int, date_to: int) -> BitMap:
        has_start = await self._fetch_has("start")
        if not has_start:
            return BitMap()
        start_slices = await self._fetch_bsi_slices("start")
        started_before_query = _bsi_range_lte(start_slices, has_start, date_from)

        has_end = await self._fetch_has("end")
        no_end = has_start - has_end
        end_slices = await self._fetch_bsi_slices("end")
        ends_after_query = _bsi_range_gte(end_slices, has_end, date_to)

        return started_before_query & (no_end | ends_after_query)

    async def search_valid(self, date_from: int, date_to: int) -> list[int]:
        return sorted(await self._valid_bitmap(date_from, date_to))

    async def search_appeared(self, date_from: int, date_to: int) -> list[int]:
        return sorted(await self._appeared_bitmap(date_from, date_to))

    async def _phrase_bitmap(self, phrase: str) -> BitMap:
        terms_with_pos, _ = process_with_positions(phrase)
        if not terms_with_pos:
            return BitMap()
        if len(terms_with_pos) == 1:
            return await self.get_posting(terms_with_pos[0][0])
        postings: list[dict[str, list[int]]] = []
        for stem, _ in terms_with_pos:
            val = await self.pos_lsm.get(stem)
            if val is None:
                return BitMap()
            postings.append(json.loads(val))
        common_docs = set(postings[0].keys())
        for p in postings[1:]:
            common_docs &= set(p.keys())
        if not common_docs:
            return BitMap()
        offsets = [tp[1] - terms_with_pos[0][1] for tp in terms_with_pos]
        result = BitMap()
        for doc_id in common_docs:
            first_positions = postings[0][doc_id]
            for p in first_positions:
                if all(
                    (p + offsets[i]) in set(postings[i][doc_id])
                    for i in range(1, len(terms_with_pos))
                ):
                    result.add(int(doc_id))
                    break
        return result

    async def phrase_search(self, phrase: str) -> list[int]:
        return sorted(await self._phrase_bitmap(phrase))

    async def wildcard_search(self, pattern: str) -> list[tuple[str, int]]:
        pattern = pattern.lower()
        segments = pattern.split('*')
        padded_segments = list(segments)
        padded_segments[0] = '^' + padded_segments[0]
        padded_segments[-1] = padded_segments[-1] + '$'

        ngrams: list[str] = []
        for seg in padded_segments:
            for k in (2, 3, 4, 5):
                for i in range(len(seg) - k + 1):
                    ngrams.append(seg[i:i + k])

        if not ngrams:
            return []
        candidates: set[str] | None = None
        for ng in ngrams:
            val = await self.kgram_lsm.get(ng)
            entries = set(val.split('\n')) if val else set()
            if candidates is None:
                candidates = entries
            else:
                candidates &= entries
            if not candidates:
                return []

        full_pattern = '^' + pattern + '$'
        result: list[tuple[str, int]] = []
        for entry in candidates:
            term, doc_id_str = entry.split('\t')
            if fnmatch.fnmatch('^' + term + '$', full_pattern):
                result.append((term, int(doc_id_str)))
        result.sort()
        return result

    async def prefix_search(self, prefix: str) -> list[tuple[str, int]]:
        return await self.wildcard_search(prefix + '*')

    async def flush(self):
        await self.lsm.flush()
        await self.kgram_lsm.flush()
        await self.bsi_lsm.flush()
        await self.pos_lsm.flush()
        self._save_all_docs()
