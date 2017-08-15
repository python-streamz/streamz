from __future__ import print_function

from time import sleep
import sys

from BeautifulSoup import BeautifulSoup  # Python 2 only, sorry.

import requests
from streamz import Stream
import toolz
import urlparse



def links_of_page((content, page)):
    uri = urlparse.urlparse(page)
    domain = '%s://%s' % (uri.scheme, uri.netloc)
    try:
        soup = BeautifulSoup(content)
    except:
        return []
    else:
        links = [link.get('href') for link in soup.findAll('a')]
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
                .concat())
links.sink(source.emit)

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
    source.emit(sys.argv[1])



#
