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
    m = pd.merge(x, y, how='outer')
    return m

def yearfmt(x):
    if not pd.isnull(x) and len(x) == 3:
        return str(int(x) + 1911)
    else:
        return x

import os
path='C:/Users/user/Dropbox/program/crawler/finance/公開資訊觀測站/彙總報表/csvfiles/會計師查核報告/'
os.chdir(path)

#---- before ifrs 2003-2007 ----
ys_e = []
year = ['92', '93', '94', '95', '96']
season = ['01','02','03','04']
for y in year:
    for s in season:
        try:
            url = 'http://mops.twse.com.tw/mops/web/ajax_t06se09_1'
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'}
            payload = {'step': '1', 'encodeURIComponent': '1', 'firstin': '1', 'off': '1', 'TYPEK': 'sii', 'year': y, 'season': s}
            source_code = requests.post(url, headers=headers, data=payload)  # should use data instead of params
            source_code.encoding = 'utf8'
            plain_text = source_code.text
            soup = BeautifulSoup(plain_text, 'html.parser')
            h=[]
            for th in soup.find_all('table')[0].find_all('tr')[0].find_all('th'):
                h.append(th.text)
            tb=[h]
            for tr in soup.find_all('table')[0].find_all('tr')[1:]:
                r=[]
                for td in tr.find_all('td'):
                    r.append(td.text.strip())
                tb.append(r)

            df=pd.DataFrame(tb)
            df.columns = df.ix[0, :]
            df=df.ix[1:len(df), :]
            df.insert(0, '年', int(y)+1911)
            df.insert(1, '季', s[1])
            date = soup.find_all('center')[0].text
            print(y, s, date)
            df.核閱或查核日期 = df.核閱或查核日期.str.split('/').str[0].apply(yearfmt) + '-' + df.核閱或查核日期.str.split('/').str[1] + '-' + df.核閱或查核日期.str.split('/').str[2]
            df.to_csv('{}{}{}.csv'.format(path, str(int(y)+1911), str(s)), index=False, encoding='utf8')
        except Exception as e:
            print(e)
            ys_e.append([y,s])
            pass

# --- before ifrs 2008-2011 ----
ys_e = []
year = ['97', '98', '99', '100']
season = ['01', '02', '03', '04']
for y in year:
    for s in season:
        try:
            url = 'http://mops.twse.com.tw/mops/web/ajax_t06se09_1'
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'}
            payload = {'step': '1', 'encodeURIComponent': '1', 'firstin': '1', 'off': '1', 'TYPEK': 'sii',
                       'year': y, 'season': s}
            source_code = requests.post(url, headers=headers, data=payload)  # should use data instead of params
            source_code.encoding = 'utf8'
            plain_text = source_code.text
            soup = BeautifulSoup(plain_text, 'html.parser')
            h = []
            for th in soup.find_all('table')[0].find_all('tr')[0].find_all('th'):
                h.append(th.text)
            tb = [h]
            for tr in soup.find_all('table')[0].find_all('tr')[1:]:
                r = []
                for td in tr.find_all('td'):
                    r.append(td.text.strip())
                tb.append(r)

            df = pd.DataFrame(tb)
            df.columns = df.ix[0, :]
            df = df.ix[1:len(df), :]
            df.insert(0, '年', int(y) + 1911)
            df.insert(1, '季', s[1])
            date = soup.find_all('center')[0].text
            print(y, s, date)
            df.核閱或查核日期 = df.核閱或查核日期.str.split('/').str[0].apply(yearfmt) + '-' + df.核閱或查核日期.str.split('/').str[1] + '-' + df.核閱或查核日期.str.split('/').str[2]
            df.to_csv('{}{}{}.csv'.format(path, str(int(y)+1911), str(s)), index=False, encoding='utf8')
        except Exception as e:
            print(e)
            ys_e.append([y, s])
            pass

#---- before ifrs 2012 ----
path='C:\\Users\\ak66h_000\\OneDrive\\webscrap\\公開資訊觀測站\\彙總報表\\會計師查核報告\\'
os.chdir(path)
ys_e = []
year = ['101']
season = ['01','02','03','04']
for y in year:
    for s in season:
        try:
            url = 'http://mops.twse.com.tw/mops/web/ajax_t06se09_1'
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'}
            payload = {'step': '1', 'encodeURIComponent': '1', 'firstin': '1', 'off': '1', 'TYPEK': 'sii', 'year': y, 'season': s}
            source_code = requests.post(url, headers=headers, data=payload)  # should use data instead of params
            source_code.encoding = 'utf8'
            plain_text = source_code.text
            soup = BeautifulSoup(plain_text, 'html.parser')
            h=[]
            for th in soup.find_all('table')[0].find_all('tr')[0].find_all('th'):
                h.append(th.text)
            tb=[h]
            for tr in soup.find_all('table')[0].find_all('tr')[1:]:
                r=[]
                for td in tr.find_all('td'):
                    r.append(td.text.strip())
                tb.append(r)

            df=pd.DataFrame(tb)
            df.columns = df.ix[0, :]
            df=df.ix[1:len(df), :]
            df.insert(0, '年', int(y)+1911)
            df.insert(1, '季', s[1])
            date = soup.find_all('center')[0].text
            print(y, s, date)
            df.核閱或查核日期 = df.核閱或查核日期.str.split('/').str[0].apply(yearfmt) + '-' + df.核閱或查核日期.str.split('/').str[1] + '-' + df.核閱或查核日期.str.split('/').str[2]
            df.to_csv('{}{}{}.csv'.format(path, str(int(y)+1911), str(s)), index=False, encoding='utf8')
        except Exception as e:
            print(e)
            ys_e.append([y,s])
            pass

#----after ifrs----
ys_e = []
season = ['01','02','03','04']
year = ['102','103', '104']
season = ['01','02','03','04']
for y in year:
    for s in season:
        try:
            url = 'http://mops.twse.com.tw/mops/web/ajax_t163sb14'
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'}
            payload = {'step': '1', 'encodeURIComponent': '1', 'firstin': '1', 'off': '1', 'TYPEK': 'sii', 'year': y, 'season': s}
            source_code = requests.post(url, headers=headers, data=payload)  # should use data instead of params
            source_code.encoding = 'utf8'
            plain_text = source_code.text
            soup = BeautifulSoup(plain_text, 'html.parser')
            h=[]
            for th in soup.find_all('table')[0].find_all('tr')[0].find_all('th'):
                h.append(th.text)
            tb=[h]
            for tr in soup.find_all('table')[0].find_all('tr')[1:]:
                r=[]
                for td in tr.find_all('td'):
                    r.append(td.text)
                tb.append(r)

            df=pd.DataFrame(tb)
            df.columns = df.ix[0, :]
            df=df.ix[1:len(df), :]
            df.insert(0, '年', int(y)+1911)
            df.insert(1, '季', s[1])
            date = soup.find_all('center')[0].text
            print(y, s, date)
            df.核閱或查核日期 = df.核閱或查核日期.str.split('/').str[0].apply(yearfmt) + '-' + df.核閱或查核日期.str.split('/').str[1] + '-' + df.核閱或查核日期.str.split('/').str[2]
            df.to_csv('{}{}{}.csv'.format(path, str(int(y)+1911), str(s)), index=False, encoding='utf8')
        except Exception as e:
            print(e)
            ys_e.append([y,s])
            pass

#----update----
ys_e = []
year = ['106']
season = ['01']
for y in year:
    for s in season:
        try:
            url = 'http://mops.twse.com.tw/mops/web/ajax_t163sb14'
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'}
            payload = {'step': '1', 'encodeURIComponent': '1', 'firstin': '1', 'off': '1', 'TYPEK': 'sii', 'year': y, 'season': s}
            source_code = requests.post(url, headers=headers, data=payload)  # should use data instead of params
            source_code.encoding = 'utf8'
            plain_text = source_code.text
            soup = BeautifulSoup(plain_text, 'html.parser')
            h=[]
            for th in soup.find_all('table')[0].find_all('tr')[0].find_all('th'):
                h.append(th.text)
            tb=[h]
            for tr in soup.find_all('table')[0].find_all('tr')[1:]:
                r=[]
                for td in tr.find_all('td'):
                    r.append(td.text)
                tb.append(r)

            df=pd.DataFrame(tb)
            df.columns = df.ix[0, :]
            df=df.ix[1:len(df), :]
            df.insert(0, '年', int(y)+1911)
            df.insert(1, '季', int(s[1]))
            date = soup.find_all('center')[0].text
            print(y, s, date)
            df.核閱或查核日期 = df.核閱或查核日期.str.split('/').str[0].apply(yearfmt) + '-' + df.核閱或查核日期.str.split('/').str[1] + '-' + df.核閱或查核日期.str.split('/').str[2]
            df.to_csv('{}{}{}.csv'.format(path, str(int(y)+1911), str(s)), index=False, encoding='utf8')
            sqlc.insertData('會計師查核報告', df, conn_lite)
        except Exception as e:
            print(e)
            ys_e.append([y,s])
            pass

#---- create table ----
# l = os.listdir()
# L=[]
# for i in l:
#     try:
#         df = read_csv(i, encoding='utf8', index_col=False).replace('?', '')
#         L.append(df)
#     except Exception as e:
#         print(e)
#         print(i)
#
# df = reduce(mymerge, L)
# df.年=df.年.astype(int)
# df.季=df.季.astype(int).astype(str)
# df.公司代號=df.公司代號.astype(int).astype(str)
# df.drop_duplicates(['年', '季', '公司代號'], inplace=True)
# y=df['核閱或查核日期'].str.split('/').str[0].astype(int)+1911
# y=y.astype(str)
# df['核閱或查核日期']= y+'/'+df['核閱或查核日期'].str.split('/').str[1]+'/'+df['核閱或查核日期'].str.split('/').str[2]
#
# tablename = '會計師查核報告'
# sql = 'create table `{}` (`{}`, PRIMARY KEY ({}))'.format(tablename, '`,`'.join(list(df)), '`年`, `季`, `公司代號`')
# cur_lite.execute(sql)
#
# sql = 'insert into `{}`(`{}`) values({})'.format(tablename, '`,`'.join(list(df)), ','.join('?' * len(list(df))))
# cur_lite.executemany(sql, df.values.tolist())
# conn_lite.commit()
#
# df = read_sql_query("SELECT * from {}".format(tablename), conn_lite); df