import sys
from urllib.parse import urlparse

import requests
import toolz
from bs4 import BeautifulSoup

from streamz import Stream


def links_of_page(content_page):
    (content, page) = content_page
    uri = urlparse(page)
    domain = '%s://%s' % (uri.scheme, uri.netloc)
    try:
        soup = BeautifulSoup(content, features="html.parser")
    except Exception:
        return []
    else:
        links = [link.get('href') for link in soup.find_all('a')]
        return [domain + link
                for link in links
                if link
                and link.startswith('/')
                and '?' not in link
                and link != '/']


def topk_dict(d, k=10):
    return dict(toolz.topk(k, d.items(), key=lambda x: x[1]))


source = Stream()
pages = source.unique()
pages.sink(print)

content = (pages.map(requests.get)
                .map(lambda x: x.content))
links = (content.zip(pages)
                .map(links_of_page)
                .flatten())
links.connect(source)

"""
from nltk.corpus import stopwords
stopwords = set(stopwords.words('english'))

word_counts = (content.map(str.split)
                      .concat()
                      .filter(str.isalpha)
                      .remove(stopwords.__contains__)
                      .frequencies())
top_words = (word_counts.map(topk_dict, k=10)
                        .map(frozenset)
                        .unique(history=10))
top_words.sink(print)
"""

if len(sys.argv) > 1:
    try:
        source.emit(sys.argv[1])
    except KeyboardInterrupt:
        pass
