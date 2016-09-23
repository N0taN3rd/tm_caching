import json
import pickle
from glob import glob

from memproxParsing import LogEntry
from timemap_parser import TimeMap, parse
import multiprocessing
from itertools import zip_longest


def mapper(file):
    return TimeMap(file).original


def grouper(n, iterable, padvalue=None):
    """grouper(3, 'abcdefg', 'x') -->
    ('a','b','c'), ('d','e','f'), ('g','x','x')"""

    return zip_longest(*[iter(iterable)] * n, fillvalue=padvalue)

# /home/john/research/tm_caching/alldata/logging
def parse_justinLogs():
    uris = set()
    for it in glob('/home/john/research/tm_caching/waam_*/**/*.out', recursive=True):
        # print(it)
        with open(it, 'r') as rIn:
            for line in (l.rstrip() for l in rIn if l):
                if 'running' in line and '...' in line:
                    uris.add(line.split(' ')[1].rstrip('.'))
                    continue
                if 'running' in line:
                    try:
                        uris.add(line.split(',')[1])
                    except IndexError:
                        ss = line.split(' ')
                        if len(ss) == 1:
                            continue
                        # print(ss)
                        if 'Got' in ss or 'got' in ss or 'tmResp' in ss or 'SHELLING!!!!!!!' in ss:
                            gg = ss[-1]
                            if 'tm' == gg:
                                gg = ss[5]
                            uris.add(gg)
                            continue
                        if 'running' in ss:
                            uris.add(ss[1].rstrip(':'))
                            continue
                        if 'waamAggr.sh:' in ss and 'end-of-file' in ss:
                            # print(ss[18])
                            uris.add(ss[18])
                            continue
                        if 'waamAggr.sh:' in ss and 'end-of-filrunning' in ss:
                            # print(ss[17])
                            uris.add(ss[17])
                            continue
                    continue

    with open('uris.txt', 'w') as out:
        for uri in uris:
            out.write('%s\n' % uri)

if __name__ == '__main__':
    print('hi')
    # 40577
    # logsf = []
    # logs = []
    # uris = {}
    # for it in glob('/home/john/research/tm_caching/alldata/logging/**/timemap.*', recursive=True):
    #     logsf.append(it)
    # for it in glob('/home/john/research/tm_caching/alldata/logging/**/infos.*', recursive=True):
    #     logsf.append(it)
    # for it in logsf:
    #     with open(it,'r') as lin:
    #         for it2 in map(lambda l: json.loads(l.rstrip("\n"), encoding='utf8'), lin):
    #             # print(it)
    #             le = LogEntry(it2)
    #             uris[le.url] = le
    # pickle.dump(uris, open("memgator-all.p", "wb"))
    uris = pickle.load(open("memgator-all.p", "rb"))
    with open('memgator-urls-no-google.txt','w') as out:
        for v in uris.values():
            if 'google' in v.domain or 'youtube' in v.domain or '.odu' in v.domain or 'github' in v.domain:
                continue
            out.write('%s\n'%v.url)


        # with open('dataset1.txt','w') as dtout:
        #     for uri in set(flist):
        #         dtout.write('%s\n'%uri)

        # p = multiprocessing.Pool(4)
        # for chunk in grouper(int(len(flist) / 4), flist):
        #     results = p.map(mapper, chunk)
        #     for r in results:
        #         print(r)
