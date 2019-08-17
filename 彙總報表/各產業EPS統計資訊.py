from common.connection import conn_local_lite
import sqlCommand as sqlc
import syspath
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import cytoolz.curried
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))


conn_lite = conn_local_lite('TEJ.sqlite3')
cur_lite = conn_lite.cursor()


def mymerge(x, y):
    m = pd.merge(x, y, how='outer')
    return m


# ----test connection----
# 2015
YEAR = '104'
df1 = pd.DataFrame()
SEASON = '03'
url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb19'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
payload = {'encodeURIComponent': '1', 'step': '1', 'firstin': '1', 'TYPEK': 'sii',
           'year': YEAR, 'season': SEASON}
# should use data instead of params
source_code = requests.post(url, headers=headers, data=payload)
source_code.encoding = 'utf8'
plain_text = source_code.text
print(plain_text)
soup = BeautifulSoup(plain_text, 'html.parser')
h = []
for th in soup.find_all('table')[0].find_all('tr')[0].find_all('th'):
    h.append(th.text)
soup.find_all('table')[0].find_all('tr')[1].find_all('td')
row = len(soup.find_all('table')[0].find_all('tr')) - 1
L = [h]
for tr in soup.find_all('tr'):
    l = []
    for td in tr.find_all('td'):
        l.append(td.text)
    L.append(l)
df = pd.DataFrame(L)
df.columns = df.ix[0, :]
df = df.ix[1:len(df), :]
df = df.replace(',', '', regex=True)
print(df)
df = df.dropna()
df1 = df[['公司代號', '公司名稱', '產業別']]

# ----create table----
names = list(df1)
cur_lite = conn_lite.cursor()
sql = "create table `" + 'tse_ch' + "`(" + "'" + names[0] + "'"
for n in names[1:len(names)]:
    sql = sql + ',' + "'" + n + "'"
sql = sql + ')'
cur_lite.execute(sql)
# ----inserting data----
sql = 'INSERT INTO `' + 'tse_ch' '` VALUES (?'
n = [',?'] * (len(names) - 1)
for h in n:
    sql = sql + h
sql = sql + ')'
cur_lite.executemany(sql, df1.values.tolist())
conn_lite.commit()
print('done')
