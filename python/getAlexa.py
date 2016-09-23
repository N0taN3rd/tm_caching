import requests
import bs4
import hashlib
from time import sleep


if __name__ == '__main__':
    useragent = 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:44.0) Gecko/20100101 Firefox/44.01'
    md5 = hashlib.md5()
    with open('files/memgator-urls-alexa.txt', 'w') as maout:
        with open('files/dset/memgator-urls-keep.txt', 'r') as muin:
            with requests.session() as session:
                session.headers.update({'User-Agent': useragent})
                for url in map(lambda l: l.rstrip(), muin):
                    print('getting alexa for',url)
                    r = requests.get("http://data.alexa.com/data?cli=10&dat=s&url=%s" % url)
                    scode = r.status_code
                    if scode != 200:
                        print('got non 200 for',url)
                        maout.write('NOT 200: %s,%d\n'%(url,scode))
                    else:
                        try:
                            soup = bs4.BeautifulSoup(r.text, 'xml')
                            aurl = soup.find('POPULARITY')['URL']
                            areach = soup.find('REACH')['RANK']
                            maout.write('%s,%s,%s\n' % (url, aurl,areach))
                            print('got result',url, aurl,areach)
                        except Exception as e:
                            maout.write('EXCEPTION: %s,%s\n' % (url, str(e)))
                            print('exception occured',url,e)
                    sleep(0.45)



                    # group_memgator_urls()
                    # to_domain_dir()
                    # pickle_falexa_bs4()
