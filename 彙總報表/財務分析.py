# #----import----
#
# from sqlite3 import *
# conn_lite = connect('C:\\Users\\user\\Documents\\db\\summary.sqlite3')
# cur_lite = conn_lite.cursor()
# from numpy import *
# from pandas import *
# from functools import *
# import sys
# sys.path.append('C:/Users/user/Dropbox/program/mypackage_py')
# import udf, sqlc
#
# def mymerge(x, y):
#     m = merge(x, y, how='outer')
#     return m
#
# import re
# import os
#
# # ----update----
# path = 'C:/Users/user/Dropbox/program/crawler/finance/公開資訊觀測站/彙總報表/csvfiles/財務分析/'
# os.chdir(path)
# l = os.listdir()
# L = []
# for i in l:
#     df = read_csv(i, encoding='cp950')
#     t = re.findall(r'\d', i)
#     t = str(int(t[0] + t[1] + t[2])+1911)
#     df.insert(0, '年', int(t))
#     df['公司代號'] = df['公司代號'].astype(str)
#     df['公司代號'], df['公司簡稱'] = df['公司代號'].str.strip(), df['公司簡稱'].str.strip()
#     # floatColumns = [col for col in list(df) if col not in ['年', '公司代號', '公司簡稱']]
#     # df[floatColumns] = df[floatColumns].astype(float)
#     L.append(df)
# # df.dtypes
# df1 = reduce(mymerge, L)
# name=list(df1)
# for i in range(len(name)):
#     name[i] = name[i].replace('財務結構-', '')
#     name[i] = name[i].replace('償債能力-', '')
#     name[i] = name[i].replace('經營能力-', '')
#     name[i] = name[i].replace('獲利能力-', '')
#     name[i] = name[i].replace('現金流量-', '')
#     name[i] = name[i].replace('<br>', '')
# df1.columns=name
# df1=df1.sort_values(['年','公司代號'],ascending=[True,True])
#
# sqlc.renameTable('財務分析', '財務分析0', conn_lite)
# sqlc.createTable('財務分析', list(df1), ['年', '公司代號'], conn_lite)
# sqlc.insertData('財務分析', df1, conn_lite)
# sqlc.dropTable('財務分析0', conn_lite)

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
YEAR = '105'
url = 'http://mops.twse.com.tw/mops/web/ajax_t51sb02'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
payload = {'encodeURIComponent':'1','step':'1', 'firstin':'1', 'off':'1','year':YEAR, 'run':'Y', 'ifrs':'Y','TYPEK':'sii'}
source_code = requests.post(url, headers=headers, data=payload) #should use data instead of params
source_code.encoding = 'utf8'
plain_text = source_code.text
soup = BeautifulSoup(plain_text, 'lxml')
print(soup.prettify())

# ---- update data ----
for YEAR in range(106, 107):
    try:
        url = 'http://mops.twse.com.tw/mops/web/ajax_t51sb02'
        payload = {'encodeURIComponent': '1', 'step': '1', 'firstin': '1', 'off': '1', 'year': YEAR, 'run': 'Y',
                   'ifrs': 'Y', 'TYPEK': 'sii'}
        source_code = requests.post(url, data=payload)  # should use data instead of params
        source_code.encoding = 'utf8'
        source_code.headers
        plain_text = source_code.text
        soup = BeautifulSoup(plain_text, 'lxml')
        t = soup.find_all('table')[2]
        tb = []
        ths = t.find_all("tr", class_="tblHead")
        th = list(map(lambda x: x.text, ths[0].find_all("th", rowspan="2"))) + list(map(lambda x: x.text, ths[1].find_all("th")))
        tb.append(th)
        trs = t.find_all(class_=["even", "odd"])
        for tr in trs:
            tb.append(list(map(lambda x: x.text, tr.find_all("td"))))
        df = pd.DataFrame(tb[1:], columns=tb[0]).replace(',', '', regex=True).replace('--', np.nan)
        df.insert(0, '年', int(YEAR) + 1911)
        sqlc.insertData('財務分析', df, conn_lite)
    except Exception as e:
        print(YEAR, e)
        pass


# ---- create table ----
dfs = []
for YEAR in range(101, 107):
    try:
        url = 'http://mops.twse.com.tw/mops/web/ajax_t51sb02'
        payload = {'encodeURIComponent': '1', 'step': '1', 'firstin': '1', 'off': '1', 'year': YEAR, 'run': 'Y',
                   'ifrs': 'Y', 'TYPEK': 'sii'}
        source_code = requests.post(url, data=payload)  # should use data instead of params
        source_code.encoding = 'utf8'
        source_code.headers
        plain_text = source_code.text
        soup = BeautifulSoup(plain_text, 'lxml')
        t = soup.find_all('table')[2]
        tb = []
        ths = t.find_all("tr", class_="tblHead")
        th = list(map(lambda x: x.text, ths[0].find_all("th", rowspan="2"))) + list(
            map(lambda x: x.text, ths[1].find_all("th")))
        tb.append(th)
        trs = t.find_all('tr', class_=['even', 'odd'])
        for tr in trs:
            tb.append(list(map(lambda x: x.text, tr.find_all("td"))))
        df = pd.DataFrame(tb[1:], columns=tb[0]).replace(',', '', regex=True).replace('--', np.nan)
        df.insert(0, '年', int(YEAR) + 1911)
        dfs.append(df)
    except Exception as e:
        print(YEAR, e)
        pass

df1 = reduce(mymerge, dfs)
sqlc.createTable('財務分析', df1, ['年', '公司代號'], conn_lite)
sqlc.insertData('財務分析', df1, conn_lite)