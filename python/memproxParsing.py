import json
import pickle
import statistics
from collections import Counter
from collections import defaultdict
import math

from pywb.utils.canonicalize import canonicalize, unsurt
from urllib.parse import urlparse
from operator import add
from functional import seq
import glob
from datetime import datetime
import arrow
import pytz
import plotly
from plotly.graph_objs import Scatter, Layout, Bar
import plotly.graph_objs as go

import requests

class LogEntry(object):
    def __init__(self, jdic):
        self.message = jdic['message']
        m = self.message.replace('got timemap request url:count, ', '')
        self.url = jdic.get('url',m.replace(m[m.rfind(':'):], ''))
        self.can = canonicalize(self.url).split(')/')
        self.domain = unsurt('%s%s' % (self.can[0], ')/'))

    def csv(self):
        print(type(self.can),self.can)
        return '%s,%s,%s\n'%(self.url,self.domain,self.can[1])
