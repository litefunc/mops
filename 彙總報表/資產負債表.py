import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import cytoolz.curried
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))

import syspath
import sqlCommand as sqlc
from common.connection import conn_local_lite

conn_lite = conn_local_lite('summary.sqlite3')
cur_lite = conn_lite.cursor()


def mymerge(x, y):
    m = pd.merge(x, y, on = [col for col in list(x) if col in list(y)], how = 'outer')
    return m

def industry(ids):
    if '2801' in ids or '2834' in ids:
        return '銀行業'
    if '2855' in ids or '2856' in ids:
        return '證券業'
    if '1101' in ids or '2330' in ids:
        return '一般業'
    if '2881' in ids or '2882' in ids or '2884' in ids:
        return '金控業'
    if '2823' in ids or '2832' in ids:
        return '保險業'
    if '1409' in ids or '1718' in ids:
        return '其他業'
    return '未知業'

# #----test connection----

YEAR = '106'
SEASON = '03'
url = 'http://mops.twse.com.tw/mops/web/ajax_t163sb05'
payload = {'encodeURIComponent':'1','step':'1', 'firstin':'1', 'off':'1','year':YEAR, 'season':SEASON, 'TYPEK':'sii'}
source_code = requests.post(url, data=payload) #should use data instead of params
source_code.encoding = 'utf8'
source_code.headers
plain_text = source_code.text
soup = BeautifulSoup(plain_text, 'lxml') # don't use html.parser because it can't parse correctly
print(soup.prettify())

# ---- update data ----
for YEAR in ['107']:
    for SEASON in ['01']:
        url = 'http://mops.twse.com.tw/mops/web/ajax_t163sb05'
        payload = {'encodeURIComponent': '1', 'step': '1', 'firstin': '1', 'off': '1', 'year': YEAR, 'season': SEASON,
                   'TYPEK': 'sii'}
        source_code = requests.post(url, data=payload)  # should use data instead of params
        source_code.encoding = 'utf8'
        source_code.headers
        plain_text = source_code.text
        soup = BeautifulSoup(plain_text, 'lxml')
        for t in soup.find_all('table')[1:]:
            try:
                trs = t.find_all('tr')
                ths = list(map(lambda x: x.text, trs[0].find_all('th')))
                tb = []
                tb.append(ths)
                for r in trs[1:]:
                    tb.append(list(map(lambda x: x.text, r.find_all('td'))))
                df = pd.DataFrame(tb[1:], columns=tb[0]).replace(',', '', regex=True).replace('--', np.nan)
                df.insert(0, '年', int(YEAR) + 1911)
                df.insert(1, '季', SEASON.replace('0', ''))
                ind = industry(list(df['公司代號']))
                print(ind)
                sqlc.insertData('資產負債表-{}'.format(ind), df, conn_lite)
            except Exception as e:
                print(YEAR, SEASON, ind, e)
                pass
