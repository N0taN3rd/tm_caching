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



if __name__ == '__main__':
    useragent = 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:44.0) Gecko/20100101 Firefox/44.01'
    md5 = hashlib.md5()
    with open('files/excpts.txt','a') as eOut:
        with open('files/jtm_hash.txt', 'a') as hashOut:
            with requests.session() as session:
                session.headers.update({'User-Agent': useragent})
                with open('files/dset/uris2.txt', 'r') as muin:
                    for url in map(lambda l: l.rstrip(), muin):
                        r = session.get("http://web.archive.org/web/timemap/json/%s" % url)
                        sc = r.status_code
                        md5.update(url.encode('utf-8'))
                        urlhash = md5.hexdigest()
                        hashOut.write('%s\t%s\n' % (url, urlhash))
                        try:
                            if sc != 200:
                                with open('files/justintm/%s-%d-timemap.txt' % (urlhash, r.status_code), 'w') as tmout:
                                    tmout.write('%s\n'%r.text)
                            else:
                                with open('files/justintm/%s-%d-timemap.json' % (urlhash, r.status_code), 'w') as tmout:
                                    json.dump(r.json(), tmout)
                        except Exception as e:
                            eOut.write('%s, %s, %d'%(str(e),url,sc))

                    sleep(45)


