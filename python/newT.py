from glob import glob
from itertools import filterfalse
from ujson import loads
from pickle import load, dump
from collections import defaultdict
import requests
import bs4
import hashlib
from urllib3.util import parse_url
from pywb.utils.canonicalize import canonicalize, unsurt
import numpy as np
import numpy_indexed as npi
import statistics
import plotly
from plotly.graph_objs import Scatter, Layout, Bar,Histogram
import plotly.graph_objs as go
from funcy.py3 import take,rest



def getAllAndPickle():
    jsons = []
    for it in filter(lambda x: 'infos' in x or 'timemap' in x, glob('memproxydata/temp/**/*log*')):
        with open(it, 'r') as itIn:
            for l in itIn:
                print(l)
                jsons.append(loads(l.rstrip()))
    with open('pickled/allLogs.pickle', 'wb') as out:
        dump(jsons, out)


def dump_pickle(obj, file):
    with open(file, 'wb') as out:
        dump(obj, out)


def read_pickle(name):
    with open(name, "rb") as input_file:
        return load(input_file)


def group_logurls_pickle():
    json = read_pickle('pickled/allLogs.pickle')
    ulrGrouper = defaultdict(list)
    for it in json:
        try:
            ulrGrouper[it['url']].append(it)
        except KeyError as ke:
            print(ke, it)
    dump_pickle(ulrGrouper, 'pickled/allUriGrouped.pickle')


def dump_uniqueByDay1():
    ug = read_pickle('pickled/allUriGrouped.pickle')  # type: dict[str,list[dict]]
    n = {}
    for k, v in ug.items():
        uniq = {}
        for vv in v:
            uniq[vv['timestamp']] = vv
        n[k] = uniq
    dump_pickle(n, 'pickled/augUniqueByDate.pickle')


def url_parse_help(url):
    lru = parse_url(url)
    return '%s://%s'%(lru.scheme,lru.host)

def newAlexia():
    useragent = 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:44.0) Gecko/20100101 Firefox/44.01'
    ubt = read_pickle('pickled/augUniqueByDate.pickle')
    aData = {}

    for url in set(map(url_parse_help, ubt.keys())):
        with requests.session() as session:
            session.headers.update({'User-Agent': useragent})
            r = requests.get("http://data.alexa.com/data?cli=10&dat=s&url=%s" % url)
            scode = r.status_code
            if scode != 200:
                print('got non 200 for', url)
            else:
                try:
                    soup = bs4.BeautifulSoup(r.text, 'xml')
                    aurl = soup.find('POPULARITY')['URL']
                    areach = soup.find('REACH')['RANK']
                    aData[url] = '%s,%s,%s\n' % (url, aurl, areach)
                    print('got result', url, aurl, areach)
                except Exception as e:
                    print('exception occured', url, e)
    dump_pickle(aData, 'pickled/memgatorAlexa.pickle')


def humanize_number(value, fraction_point=1):
    powers = [10 ** x for x in (12, 9, 6, 3, 0)]
    human_powers = ('T', 'B', 'M', 'K', '')
    is_negative = False
    if not isinstance(value, float):
        value = float(value)
    if value < 0:
        is_negative = True
        value = abs(value)
    for i, p in enumerate(powers):
        if value >= p:
            return_value = str(round(value / (p / (10.0 ** fraction_point))) /
                               (10 ** fraction_point)) + human_powers[i]
            break
    if is_negative:
        return_value = "-" + return_value

    return return_value

def maMapper(it):
    split = it.rstrip().split(',')
    return split[1],int(split[2])

def tupleUnique(tlist,pos):
    seen = set()

def unique_everseen(iterable, key=None):
    "List unique elements, preserving order. Remember all elements ever seen."
    # unique_everseen('AAAABBBCCDAABBB') --> A B C D
    # unique_everseen('ABBCcAD', str.lower) --> A B C D
    seen = set()
    seen_add = seen.add
    if key is None:
        for element in filterfalse(seen.__contains__, iterable):
            seen_add(element)
            yield element
    else:
        for element in iterable:
            k = key(element)
            if k not in seen:
                seen_add(k)
                yield element

class MAlexia(object):
    def __init__(self,reaches,domains,buckets,step,digitized,unique,groups):
        self.reaches = reaches
        self.domains = domains
        self.buckets = buckets
        self.step = step
        self.digitized = digitized
        self.unique = unique
        self.groups = groups

def raw_alexa_to_obj():
    ubt = read_pickle('pickled/memgatorAlexa.pickle')
    subt = sorted(unique_everseen(map(maMapper, ubt.values()), key=lambda x: x[0]), key=lambda x: x[1])
    reaches = []
    domains = []
    for d, r in subt:
        reaches.append(r)
        domains.append(d)
    # subt = sorted(ubt)
    start = min(reaches)
    stop = max(reaches)
    buckets, step = np.linspace(start, stop, retstep=True)
    digitized = np.digitize(reaches, buckets)
    unique, groups = npi.group_by(digitized, reaches)  # type:
    dump_pickle(MAlexia(reaches,domains,buckets,step,digitized,unique,groups),'pickled/malexia.pickle')

if __name__ == '__main__':
    raw_alexa_to_obj()


    # data = [
    #     go.Histogram(
    #         x=sorted(ubt),
    #         xbins=dict(
    #          start=start,
    #          end=stop,
    #          size=step
    #         ),
    #         histnorm='count',
    #     )
    # ]
    # plotly.offline.plot(data, filename='alexia.html',auto_open=False)
    # print(step)