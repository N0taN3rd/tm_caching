import json
from glob import glob
from itertools import filterfalse
from ujson import loads,dump as dumpj
from pickle import load, dump
from collections import defaultdict, Counter
import requests
import bs4
import arrow
from ujson import load
from json import load as load2
import re
import pprint
import hashlib
from urllib3.util import parse_url
from pywb.utils.canonicalize import canonicalize, unsurt
import numpy as np
import numpy_indexed as npi
import statistics

import plotly
from plotly import tools
from plotly.graph_objs import Scatter, Layout, Bar, Histogram
import plotly.graph_objs as go
from funcy.py3 import take, rest
from itertools import zip_longest
import colorlover as cl


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
    return '%s://%s' % (lru.scheme, lru.host)


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
    return split[1], int(split[2])


def tupleUnique(tlist, pos):
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
    def __init__(self, reaches, domains, buckets, step, digitized, unique, groups):
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
    dump_pickle(MAlexia(reaches, domains, buckets, step, digitized, unique, groups), 'pickled/malexia.pickle')


def grouper(n, iterable, fillvalue=None):
    "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)


def alexia_top_ten():
    colors = cl.scales['10']['div']['RdYlBu']
    ma = read_pickle('pickled/malexia.pickle')  # type: MAlexia
    fig = tools.make_subplots(rows=5, cols=2, subplot_titles=('Most Popular', '',
                                                              '', '',
                                                              '', '',
                                                              '', '',
                                                              '', 'Least Popular'))
    rows = 1
    q = False
    cols = 1
    tc = 0
    for group in grouper(5, ma.groups):
        if cols == 3:
            cols = 1
        for g in group:
            print(len(g))
            start = min(g)
            stop = max(g)
            buckets, step = np.linspace(start, stop, retstep=True)
            hist = go.Histogram(
                name='[%s,%s]' % (humanize_number(start), humanize_number(stop)),
                x=g,
                autobinx=False,
                xbins=dict(
                    start=start,
                    end=stop,
                    size=step
                ),
                histnorm='count',
                marker=dict(
                    color=colors[9 - tc],

                )
            )
            fig.append_trace(hist, rows, cols)
            cols += 1
            tc += 1
            if cols == 3:
                cols = 1
                rows += 1
            if rows == 6:
                break
        if rows == 6:
            break

    fig['layout'].update(title='Memgator Urls Top 10 Alexia Reach Rank Histogram')
    fig['layout']['xaxis1'].update(title='reach rank')
    fig['layout']['yaxis1'].update(title='count')
    plotly.offline.plot(fig, filename='plots/alexia/alexiaTop10.html', auto_open=False)


def alexia_all():
    colors = cl.scales['10']['div']['RdYlBu']
    ma = read_pickle('pickled/malexia.pickle')  # type: MAlexia
    fig = tools.make_subplots(rows=10, cols=5)
    count = 1
    for group in grouper(5, ma.groups):
        cc = 1
        for g in group:
            start = min(g)
            stop = max(g)
            buckets, step = np.linspace(start, stop, retstep=True)
            hist = go.Histogram(
                name='[%d,%d]' % (start, stop),
                x=g,
                autobinx=False,
                xbins=dict(
                    start=start,
                    end=stop,
                    size=step
                ),
                histnorm='count',
                marker=dict(
                    color=colors[9 - (count - 1)],

                )
            )
            fig.append_trace(hist, count, cc)
            cc += 1
        cc = 1
        count += 1

    fig['layout'].update(title='Memgator Urls Alexia Reach Ranch Histogram')
    # fig['layout']['xaxis'].update(title='Rank')
    # fig['layout']['yaxis'].update(title='Count')
    plotly.offline.plot(fig, filename='plots/alexia/mgAlexiaAll.html', auto_open=False)


def mg_alexia_powerlaw():
    ma = read_pickle('pickled/malexia.pickle')  # type: MAlexia
    data = [
        go.Histogram(
            x=ma.reaches,
            xbins=dict(
                start=min(ma.reaches),
                end=max(ma.reaches),
                size=ma.step
            ),
            histnorm='count',
        )
    ]
    layout = go.Layout(
        title='Memgator Alexia Reach Rank',
        yaxis=dict(
            title='Count'
        ),
        xaxis=dict(
            title='Reach Rank'
        )
    )
    fig = go.Figure(data=data, layout=layout)
    plotly.offline.plot(fig, filename='plots/alexia/alexia.html', auto_open=False)


class tmGroups(object):
    def __init__(self,grouped):
        self.grouped = grouped

def parse_group_tm():
    remover = re.compile('(/[a-z/]+[0-9]+/)|(/[0-9]+/)')
    grouped = {}
    tmlist = ['/home/john/research/memproxydata/temp/timemaps-08172016/**/*.json',
              '/home/john/research/memproxydata/temp/timemaps-08182016/**/*.json',
              '/home/john/research/memproxydata/temp/timemaps-08202016/**/*.json',
              '/home/john/research/memproxydata/temp/timemaps-08212016/**/*.json',
              '/home/john/research/memproxydata/temp/timemaps-08222016/**/*.json',
              '/home/john/research/memproxydata/temp/timemaps-08232016/**/*.json',
              '/home/john/research/memproxydata/temp/timemaps-08242016/**/*.json',
              '/home/john/research/memproxydata/temp/timemaps-08252016/**/*.json',
              '/home/john/research/memproxydata/temp/timemaps-08262016/**/*.json',
              '/home/john/research/memproxydata/temp/timemaps-08272016/**/*.json',
              '/home/john/research/memproxydata/temp/timemaps-08282016/**/*.json',
              '/home/john/research/memproxydata/temp/timemaps-08292016/**/*.json',
              '/home/john/research/memproxydata/temp/timemaps-09132016/**/*.json',
              ]
    for dir in tmlist:
        for it in glob(dir):
            with open(it, 'r') as jin:
                try:
                    tm = load2(jin)
                    f = tm['mementos']['first']
                    l = tm['mementos']['last']
                    uri = remover.sub('', parse_url(f['uri']).path)
                    if grouped.get(uri,None) is None:
                        grouped[uri] = set()
                    grouped[uri].add((f['datetime'], l['datetime']))
                    # print(grouped)
                except Exception as e:
                    print(it, e)
                    continue
    # for it in [it2 for it2 in tmlist ]:
    out_dict = {}
    print(grouped)
    for k,v in grouped.items():
        out_dict[k] = list(v)
    dump_pickle(out_dict,'pickled/tmsgrouped3.pickle')
    dump_pickle(grouped,'pickled/tmsgrouped4.pickle')
    with open('tmg.json','w') as jout:
            dumpj(out_dict,jout)



# ws-dl.blogspot.com/ 9898041
# ws-dl.blogspot.fr/ 18266840
if __name__ == '__main__':
    # parse_group_tm()

    with open('tmg.json','r') as jin:
       it = json.load(jin)

    for k,v in it.items():
        print(k)
        for fl in [g for g in v]:
            print(fl)
            # f = arrow.get(fl[0])
            # l = arrow.get(fl[1])

    # for k,v in tmg.items():
    #     print(k,v)
