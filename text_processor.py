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
