import json
import re
from pywb.utils.canonicalize import canonicalize, unsurt
from pathlib import Path
from functional import seq
from collections import defaultdict
import pickle
import csv
import requests
import bs4
import hashlib
from time import sleep


class URL(object):
    def __init__(self, url):
        self.url = url
        split = canonicalize(url).split(')/')
        dd = split[0].split(',')
        if len(dd) > 2:
            self.domain = ','.join(dd[:2])
        else:
            self.domain = split[0]
        self.path = split[1]
        self.pathLen = len(self.path.split('/'))

    def keepMe(self):
        return not (
            'google' in self.domain or 'facebook' in self.domain or 'amazon' in self.domain or 'openarchive' in
            self.domain
            or 'ebay' in self.domain or 'walgreens' in self.domain or 'blackboard' in self.domain)

    def __str__(self):
        return self.url

    def __repr__(self):
        return self.__str__()


def pickle_it(what, file):
    pickle.dump(what, open(file, 'wb'))


def load_pickle(file):
    return pickle.load(open(file, 'rb'))


def group_memgator_urls():
    grouper = defaultdict(list)
    nums = re.compile('[0-9,]+')
    with open('files/dset/memgator-urls.txt', 'r') as muin:
        for url in map(lambda l: URL(l.rstrip()), muin):
            if nums.match(url.domain):
                continue
            grouper[url.domain].append(url)
    pickle_it(grouper, 'pickled/memgatorGrouped.p')


def pickle_falexa_bs4():
    with open('files/memgatorDomains.tsv', 'r') as tsvin:
        for l in tsvin:
            split = l.rstrip().split('\t')
            d = split[0]
            l = split[1]
            r = requests.get("http://data.alexa.com/data?cli=10&dat=s&url=%s" % l)
            soup = bs4.BeautifulSoup(r.text, 'xml')
            pickle_it(soup, 'pickled/afb4.p')
            print(soup.prettify())
            break


def to_domain_dir():
    grouper = load_pickle('pickled/memgatorGrouped.p')
    with open('files/memgatorDomains.tsv', 'w') as out:
        for d, urls in grouper.items():
            out.write('%s\t%s\n' % (d, ','.join(map(lambda url: url.url, urls))))
            # with open('files/memgatorDomains/%s'%d,'w') as dout:
            #
            #     for url in urls:
            #         dout.write(url.url)


def test_soup():
    soup = load_pickle('pickled/afb4.p')  # type: bs4.BeautifulSoup
    print(soup.prettify())
    print(soup.find('POPULARITY')['URL'])
    print(soup.find('REACH')['RANK'])


def group_justin():
    grouper = defaultdict(list)
    nums = re.compile('[0-9,]+')
    filtered = []
    with open('files/dset/uris2.txt', 'r') as muin:
        for url in map(lambda l: URL(l.rstrip()), muin):
            grouper[url.domain].append(url)
    with open('files/dset/justin.tsv', 'w') as out:
        for d, ul in grouper.items():
            out.write('%s\t%s\n' % (d, ','.join(map(lambda x: x.url, ul))))


def dl_justing_tm():
    useragent = 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:44.0) Gecko/20100101 Firefox/44.01'
    md5 = hashlib.md5()
    with open('files/jtm_hash.txt', 'w') as hashOut:
        with requests.session() as session:
            session.headers.update({'User-Agent': useragent})
            with open('files/dset/uris2.txt', 'r') as muin:
                for url in map(lambda l: l.rstrip(), muin):
                    r = session.get("http://web.archive.org/web/timemap/json/%s" % url)
                    md5.update(url.encode('utf-8'))
                    urlhash = md5.hexdigest()
                    hashOut.write('%s\t%s\n' % (url, urlhash))
                    with open('files/justintm/%s-%d-timemap.json' % (urlhash, r.status_code), 'w') as tmout:
                        json.dump(r.json(), tmout)
                    break


if __name__ == '__main__':
    grouper = defaultdict(list)
    nums = re.compile('[0-9,]+')

    with open('files/dset/memgator-urls.txt', 'r') as muin:
        for url in map(lambda l: URL(l.rstrip()), muin):
            if nums.match(url.domain):
                continue
            if not url.keepMe():
                continue
            grouper[url.domain].append(url)
    with open('files/dset/memgator-urls-keep.txt', 'w') as mout:
        for d, urls in grouper.items():
            url = urls[0]
            mout.write('%s\n'%url.url)

# group_memgator_urls()
# to_domain_dir()
# pickle_falexa_bs4()
