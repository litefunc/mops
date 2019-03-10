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


# #----test connection----
YEAR = '106'
SEASON = '03'
url = 'http://mops.twse.com.tw/mops/web/ajax_t163sb06'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
payload = {'encodeURIComponent':'1','step':'1', 'firstin':'1', 'off':'1','year':YEAR, 'season':SEASON, 'TYPEK':'sii'}
source_code = requests.post(url, headers=headers, data=payload) #should use data instead of params
source_code.encoding = 'utf8'
plain_text = source_code.text
soup = BeautifulSoup(plain_text, 'lxml')
print(soup.prettify())

# ---- update data ----
for YEAR in range(105, 108):
    for SEASON in ['01', '02', '03', '04']:
        try:
            url = 'http://mops.twse.com.tw/mops/web/ajax_t163sb06'
            payload = {'encodeURIComponent': '1', 'step': '1', 'firstin': '1', 'off': '1', 'year': YEAR,
                       'season': SEASON,
                       'TYPEK': 'sii'}
            source_code = requests.post(url, data=payload)  # should use data instead of params
            source_code.encoding = 'utf8'
            source_code.headers
            plain_text = source_code.text
            soup = BeautifulSoup(plain_text, 'lxml')
            t = soup.find_all('table')[0]
            tb = []
            ths = t.find_all("tr", class_="tblHead")
            th = list(map(lambda x: x.text, ths[0].find_all("td")))
            tb.append(th)
            trs = t.find_all(class_=["even", "odd"])
            for tr in trs:
                tb.append(list(map(lambda x: x.text, tr.find_all("td"))))
            df = pd.DataFrame(tb[1:], columns=tb[0]).replace(',', '', regex=True).replace('--', np.nan)
            df.insert(0, '年', int(YEAR) + 1911)
            df.insert(1, '季', SEASON.replace('0', ''))
            df = df.rename(columns={'毛利率(%)(營業毛利)/(營業收入)': '毛利率(%)', '營業利益率(%)(營業利益)/(營業收入)': '營業利益率(%)',
                                    '稅前純益率(%)(稅前純益)/(營業收入)': '稅前純益率(%)', '稅後純益率(%)(稅後純益)/(營業收入)': '稅後純益率(%)'})
            sqlc.insertData('營益分析', df, conn_lite)
        except Exception as e:
            print(YEAR, SEASON, e)
            pass


# ---- create table ----
dfs = []
for YEAR in range(102, 108):
    for SEASON in ['01', '02', '03', '04']:
        try:
            url = 'http://mops.twse.com.tw/mops/web/ajax_t163sb06'
            payload = {'encodeURIComponent': '1', 'step': '1', 'firstin': '1', 'off': '1', 'year': YEAR, 'season': SEASON,
                       'TYPEK': 'sii'}
            source_code = requests.post(url, data=payload)  # should use data instead of params
            source_code.encoding = 'utf8'
            source_code.headers
            plain_text = source_code.text
            soup = BeautifulSoup(plain_text, 'lxml')
            t = soup.find_all('table')[0]
            tb = []
            ths = t.find_all("tr", class_="tblHead")
            th = list(map(lambda x: x.text, ths[0].find_all("td")))
            tb.append(th)
            trs = t.find_all(class_=["even", "odd"])
            for tr in trs:
                tb.append(list(map(lambda x: x.text, tr.find_all("td"))))
            df = pd.DataFrame(tb[1:], columns=tb[0]).replace(',', '', regex=True).replace('--', np.nan)
            df.insert(0, '年', int(YEAR) + 1911)
            df.insert(1, '季', SEASON.replace('0', ''))
            df = df.rename(columns={'毛利率(%)(營業毛利)/(營業收入)':'毛利率(%)', '營業利益率(%)(營業利益)/(營業收入)':'營業利益率(%)', '稅前純益率(%)(稅前純益)/(營業收入)':'稅前純益率(%)', '稅後純益率(%)(稅後純益)/(營業收入)':'稅後純益率(%)'})
            dfs.append(df)
        except Exception as e:
            print(YEAR, SEASON, e)
            pass

df1 = reduce(mymerge, dfs)
sqlc.createTable('營益分析', df1, ['年', '季', '公司代號'], conn_lite)
sqlc.insertData('營益分析', df1, conn_lite)
