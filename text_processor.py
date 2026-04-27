import re
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer

_stemmer = SnowballStemmer('english')
_stop_words = set(stopwords.words('english'))
_token_re = re.compile(r'\w+', re.UNICODE)


def process(text: str) -> list[str]:
    tokens = _token_re.findall(text.lower())
    return [_stemmer.stem(t) for t in tokens if t not in _stop_words]


def process_with_original(text: str) -> list[tuple[str, str]]:
    tokens = _token_re.findall(text.lower())
    return [(t, _stemmer.stem(t)) for t in tokens if t not in _stop_words]


def process_with_positions(text: str, offset: int = 0) -> tuple[list[tuple[str, int]], int]:
    tokens = _token_re.findall(text.lower())
    result = []
    for i, t in enumerate(tokens):
        if t not in _stop_words:
            result.append((_stemmer.stem(t), offset + i))
    return result, offset + len(tokens)
