import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import cytoolz.curried
import time
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))

import syspath
import sqlCommand as sqlc
import craw.crawler as crawler
from common.connection import conn_local_lite
conn_lite = conn_local_lite('TEJ.sqlite3')
cur_lite = conn_lite.cursor()

def mymerge(x,y):
    m = pd.merge(x,y,how='outer')
    return m

#----get unique id----
url = 'http://www.twse.com.tw/ch/trading/exchange/BWIBBU/BWIBBU_d.php'
payload = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36',
    'input_date':'106/01/09', 'select2': 'ALL', 'order': 'STKNO'}
source_code = requests.post(url, params=payload)
source_code.encoding = 'big5'
plain_text = source_code.text
print(plain_text)
soup = BeautifulSoup(plain_text, 'html.parser')
date = soup.find_all('thead')[0].find_all('tr')[0].find_all('th')[0].string

h = ['年月日']
for tr in soup.find_all('thead')[0].find_all('tr')[1]:
    h.append(tr.text)
l = [h]
for tr in soup.find_all('tbody')[0].find_all('tr'):
    r = [date.split()[0] + date.split()[0]]
    for td in tr.find_all('td'):
        r.append(td.string)
    l.append(r)
df = pd.DataFrame(l)
df.columns = df.ix[0, :]
df = df.ix[1:len(df), :]

id = df.ix[:, 1].unique().tolist()
for u in id:
    print(u)

#----test connection----
CO_ID = '1101'
#2015
YEAR = '2015'
df1 = pd.DataFrame()
SEASON = '4'
# url = "http://mops.twse.com.tw/server-java/t164sb01?step = 1&CO_ID = "+CO_ID+'&SYEAR = '+YEAR+'&SSEASON = '+SEASON+'&REPORT_ID = C'
# source_code = requests.get(url)
url = 'http://mops.twse.com.tw/server-java/t164sb01'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
payload = {'step':'1','CO_ID':CO_ID,'SYEAR':YEAR,'SSEASON':SEASON,'REPORT_ID':'C'}
source_code = requests.post(url,headers=headers,data=payload) #should use data instead of params
source_code.encoding = 'big5'
plain_text = source_code.text
print(plain_text)

#----test getting data----
CO_ID = '5522'
stat = 2
L = []
for YEAR in ['2015','2014','2013']:
    for SEASON in ['4','3','2','1']:
        try:
            url = "http://mops.twse.com.tw/server-java/t164sb01?step = 1&CO_ID = "+CO_ID+'&SYEAR = '+YEAR+'&SSEASON = '+SEASON+'&REPORT_ID = C'
            source_code = requests.get(url)
            source_code.encoding = 'big5'
            plain_text = source_code.text
            soup = BeautifulSoup(plain_text, 'html.parser')
            h = []
            for th in soup.find_all('table')[stat].find_all('tr')[0].find_all('th'):
                h.append(th.string)
            row = len(soup.find_all('table')[stat].find_all('tr')[2].find_all('td',{'align':""}))
            td = soup.find_all('table')[stat].find_all('tr')[2].find_all('td')
            l = []
            for j in range(0,len(h)):
                x = [h[j]]
                for i in range(0, row*len(h),len(h)):
                    x.append(td[i+j].string)
                l.append(x)
            df = pd.DataFrame(l)
            df.columns = df.ix[0,:]
            df = df.ix[1:len(df),:]
            col = []
            for i, j in enumerate(list(df)):
                if j == '\u3000\u3000 繼續營業單位淨利（淨損）':
                    col.append(i)
            cols = list(df)
            cols[col[1]] = '\u3000\u3000 稀釋繼續營業單位淨利（淨損）'
            df.columns = cols
            L.append(df) # do not use L = L.append(df)!!
        except:
            pass
df1 = cytoolz.reduce(mymerge,L)
print(df1)
df1 = df1.rename(columns={'會計項目':'年季'})
df2 = df1.drop_duplicates(subset='年季')
df2.index = np.arange(len(df2))
df2['證券代號'] = CO_ID
df2 = df2.sort(['年季', '證券代號'], ascending=[0,1])
cols = df2.columns.tolist()
cols = cols[-1:] + cols[:-1]
df2 = df2[cols]
df2 = df2.replace(',', '', regex=True)
print(df2)

# #----create table----
# names = list(df2)
# c = conn.cursor()
# sql = "create table `" + "income" + "`(" + "'" + names[0] + "'"
# for n in names[1:len(names)]:
#      sql = sql + ',' + "'" + n + "'"
# sql = sql + ')'
# c.execute(sql)

# #----test inserting data----
# sql = 'INSERT INTO `income` VALUES (?'
# n = [',?'] * (len(cols)-1)
# for h in n:
#     sql = sql+h
# sql = sql+')'
# c.executemany(sql, df3.values.tolist())
# conn.commit()

#---continue---
from os import *
getcwd()
chdir('C:/Users/ak66h_000/Dropbox/webscrap/cash')
getcwd()
listdir()
id2 = []
for i in listdir():
    id2.append(i)
for i in range(len(id2)):
    id2[i] = id2[i].replace('df', '')
    id2[i] = id2[i].replace('.csv', '')
id1 = id
len(id1)
for i in id2:
    if i in id1:
        id1.remove(i)
len(id1)

def repl(str,str1):
    col = []
    for i, j in enumerate(list(df)):
        if j  ==  str:
            col.append(i)
    if len(col)  ==  2:
        cols = list(df)
        cols[col[1]] = str+str1
        df.columns = cols
def repl2(str, str1):
    col = []
    for i, j in enumerate(list(df)):
        if j  ==  str:
            col.append(i)
    if len(col)  ==  2:
        cols = list(df)
        cols[col[0]] = str + str1
        df.columns = cols
def repl3(str, str1,str2):
    col = []
    for i, j in enumerate(list(df)):
        if j  ==  str:
            col.append(i)
    if len(col)  ==  3:
        cols = list(df)
        cols[col[1]] = str + str1
        cols[col[2]] = str + str2
        df.columns = cols

#----main----
# tse好像會阻擋,而且資料會有少
stat = 3
id_e = []
dupl = []
for CO_ID in id2:
    try:
        L = []
        for YEAR in ['2015']:
            print('Wait 4 seconds')
            time.sleep(4)
            for SEASON in ['4']:
                print(CO_ID+' '+YEAR+' '+SEASON)
                try:
                    url = 'http://mops.twse.com.tw/server-java/t164sb01'
                    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
                    payload = {'step':'1', 'CO_ID':CO_ID, 'SYEAR':YEAR, 'SSEASON':SEASON, 'REPORT_ID':'C'}
                    source_code = requests.post(url, headers=headers, data=payload)
                    source_code.encoding = 'big5'
                    plain_text = source_code.text
                    soup = BeautifulSoup(plain_text, 'html.parser')
                    h = []
                    for th in soup.find_all('table')[stat].find_all('tr')[0].find_all('th'):
                        h.append(th.string)
                    row = len(soup.find_all('table')[stat].find_all('tr')[2].find_all('td',{'align':""}))
                    td = soup.find_all('table')[stat].find_all('tr')[2].find_all('td')
                    l = []
                    for j in range(0, len(h)):
                        x = [h[j]]
                        for i in range(0, row*len(h), len(h)):
                            x.append(td[i+j].string)
                        l.append(x)
                    df = pd.DataFrame(l)
                    df.columns = df.ix[0, :]
                    df = df.ix[1:len(df), :]
                    # col = []
                    # for i, j in enumerate(list(df)):
                    #     if j  =  =  '\u3000\u3000 繼續營業單位淨利（淨損）':
                    #         col.append(i)
                    # if len(col) =  = 2:
                    #     cols = list(df)
                    #     cols[col[1]] = '\u3000\u3000 稀釋繼續營業單位淨利（淨損）'
                    #     df.columns = cols
                    # col = []
                    # for i, j in enumerate(list(df)):
                    #     if j  =  =  '\u3000\u3000 停業單位淨利（淨損）':
                    #         col.append(i)
                    # if len(col) =  = 2:
                    #     cols = list(df)
                    #     cols[col[1]] = '\u3000\u3000 稀釋停業單位淨利（淨損）'
                    #     df.columns = cols
                    repl('\u3000\u3000 取得避險之衍生金融資產', '-籌資活動')
                    repl('\u3000\u3000 處分避險之衍生金融資產', '-籌資活動')
                    repl('\u3000\u3000 除列避險之衍生金融負債', '-籌資活動')
                    repl2('\u3000\u3000 收取之利息', '-營業活動')
                    repl2('\u3000\u3000 支付之利息', '-營業活動')
                    repl3('\u3000\u3000 退還（支付）之所得稅', '-投資活動', '-籌資活動')
                    dup = [item for idx, item in enumerate(list(df)) if item in list(df)[:idx]]
                    if len(dup) > 0:
                        cols = [CO_ID + ' ' + YEAR + ' ' + SEASON]
                        for i in dup:
                            cols.append(i)
                            du = pd.DataFrame(cols)
                            du.columns = du.ix[0, :]
                            du = du.ix[1:len(du), :]
                        print(du)
                        dupl = pd.concat([dupl,du], axis=1)
                        print(dupl)
                        dupl.to_csv('C:/Users/ak66h_000/Dropbox/webscrap/cashdu' + str(stat) + '.csv', index=False)
                    L.append(df) # do not use L = L.append(df)
                except Exception as e:
                    print(e)
                    pass
        df1 = cytoolz.reduce(mymerge,L)
        df1 = df1.rename(columns={'會計項目':'年季'})
        df2 = df1.drop_duplicates(subset='年季')
        df2.index = np.arange(len(df2))
        df2['證券代號'] = CO_ID
        df2 = df2.sort(['年季', '證券代號'], ascending=[0, 1])
        cols = df2.columns.tolist()
        cols = cols[-1:] + cols[:-1]
        df2 = df2[cols]
        df2 = df2.replace(',', '', regex=True)
        print(df2)
        #df4 = concat([df4, df3], ignore_index = True)
        #df4.to_csv('C:/Users/ak66h_000/Dropbox/webscrap/income/df4.csv',index = False)
        path = 'C:/Users/ak66h_000/Dropbox/webscrap/cash/df'+CO_ID+'.csv'
        df2.to_csv(path, index=False)
    except Exception as e:
        id_e.append(CO_ID)
        print(e)
        print('Wait 20 seconds...')
        time.sleep(20)
        print('Continue')
        pass
id_e1 = id_e
print(id_e1)

# for i in id2:
#     makedirs(i)

# for i, v in enumerate(id2):
#     print(i, v)

#----main_new----
# tse好像會阻擋,而且資料會有少

def f(CO_ID, YEAR, SEASON): 
    # global dupl
    source_code.encoding = 'big5'
    plain_text = source_code.text
    soup = BeautifulSoup(plain_text, 'html.parser')
    h = []
    for th in soup.find_all('table')[stat].find_all('tr')[0].find_all('th'):
        h.append(th.string)
    row = len(soup.find_all('table')[stat].find_all('tr')[2].find_all('td',{'align':""}))
    td = soup.find_all('table')[stat].find_all('tr')[2].find_all('td')
    l = []
    for j in range(0, len(h)):
        x = [h[j]]
        for i in range(0, row*len(h), len(h)):
            x.append(td[i+j].string)
        l.append(x)
    df = pd.DataFrame(l)
    df.columns = df.ix[0, :]
    df = df.ix[1:len(df), :]
    repl('\u3000\u3000 取得避險之衍生金融資產', '-籌資活動')
    repl('\u3000\u3000 處分避險之衍生金融資產', '-籌資活動')
    repl('\u3000\u3000 除列避險之衍生金融負債', '-籌資活動')
    repl2('\u3000\u3000 收取之利息', '-營業活動')
    repl2('\u3000\u3000 支付之利息', '-營業活動')
    repl3('\u3000\u3000 退還（支付）之所得稅', '-投資活動', '-籌資活動')
    # dup = [item for idx, item in enumerate(list(df)) if item in list(df)[:idx]]
    # if len(dup) > 0:
    #     cols = [CO_ID + ' ' + YEAR + ' ' + SEASON]
    #     for i in dup:
    #         cols.append(i)
    #         du = pd.DataFrame(cols)
    #         du.columns = du.ix[0, :]
    #         du = du.ix[1:len(du), :]
    #     print(du)
    #     dupl = concat([dupl,du], axis = 1)
    #     print(dupl)
    #     dupl.to_csv('C:/Users/ak66h_000/Dropbox/webscrap/cashdu' + str(stat) + '.csv', index = False)

    df = df.rename(columns={'會計項目': '年季'})
    df1 = df.drop_duplicates(subset='年季')
    df1.index = np.arange(len(df1))
    df1['公司代號'] = CO_ID
    df1 = df1.sort(['年季', '公司代號'], ascending=[0, 1])
    cols = df1.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    df1 = df1[cols]
    df1 = df1.replace(',', '', regex=True)
    print(df1)
    # df4 = concat([df4, df3], ignore_index = True)
    # df4.to_csv('C:/Users/ak66h_000/Dropbox/webscrap/income/df4.csv',index = False)
    path = 'C:/Users/ak66h_000/Dropbox/webscrap/cash/{}/{}-{}{}.csv'.format(CO_ID, CO_ID, YEAR, SEASON)
    df1.to_csv(path, index=False)

# CO_ID = '9944'
stat = 3
id_e = []
dupl = []
x = 1
for YEAR in ['2016']:
    for SEASON in ['1', '2', '3']:
        for CO_ID in id2[:-1]:
            print(CO_ID+' '+YEAR+' '+SEASON)
            path = 'C:/Users/ak66h_000/Dropbox/webscrap/cash/{}/'.format(CO_ID)
            chdir(path)
            if len(listdir()) == x:
                url = 'http://mops.twse.com.tw/server-java/t164sb01'
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
                try:
                    payload = {'step':'1','CO_ID':CO_ID,'SYEAR':YEAR,'SSEASON':SEASON,'REPORT_ID':'C'}
                    source_code = requests.post(url, headers=headers, data=payload)
                    f(CO_ID, YEAR, SEASON)
                    print('Wait 4 seconds')
                    time.sleep(4)
                except:
                    try:
                        payload = {'step':'1','CO_ID':CO_ID,'SYEAR':YEAR,'SSEASON':SEASON,'REPORT_ID':'A'}
                        source_code = requests.post(url, headers=headers, data=payload)
                        f(CO_ID, YEAR, SEASON)
                        print('Wait 4 seconds')
                        time.sleep(4)
                    except Exception as e:
                        print(e)
                        id_e.append(CO_ID)
                        print('Wait 4 seconds')
                        time.sleep(4)
                        pass
            
        x+= 1
path = 'C:/Users/ak66h_000/Dropbox/webscrap/cash'
chdir(path)
id_e1 = id_e
print(id_e1)

#---continue---
import os
os.getcwd()
dir()
os.listdir()
path = 'C:/Users/ak66h_000/Dropbox/webscrap/cash'
os.chdir(path)
L = os.listdir()

id1 = [x.replace('.csv', '') for x in L]
id1 = [x.replace('df', '') for x in id1]
id2 = []
for i in id:
    if i not in id1:
        id2.append(i)
print(id2)
print(len(id2))

#---read csv---
l = []
for i in L:
    df = pd.read_csv(i,encoding='big5')
    l.append(df)
df = cytoolz.reduce(mymerge,l)
list(df)
name = list(df)
import re
# dup = [x for x in name if re.search(r'\[duplicate].*',x) is not None ]
# dup
# [x for x in name if re.search(r'\.1',x) is not None ]
df['證券代號']  = [str(x).replace('.0', '') for x in df['證券代號'].values.tolist()]
df.to_csv('C:/Users/ak66h_000/Dropbox/webscrap/cash.csv',index=False)

from sqlite3 import *
conn = connect('C:/Users/ak66h_000/Documents/db/xbrlcash.sqlite3')
def getyear(x):
    for i in ['2012', '2013', '2014', '2015', '2016']:
        if i + '年' in x:
            return i
def getmonth(x):
    if '03' in x:
        return '1'
    if '06' in x:
        return '2'
    if '09' in x:
        return '3'
    if '12月31日' or '度' in x:
        return '4'

path = 'C:/Users/ak66h_000/Dropbox/webscrap/cash/'
os.chdir(path)
err = []
for i in os.listdir(path+'till20153/'):
    try:
        os.chdir(path + 'till20153')
        l = []
        df = pd.read_csv(i, encoding='big5')
        df['年'] = df['年季'].apply(getyear)
        df['季'] = df['年季'].apply(getmonth)
        df = df[['年', '季'] + [i for i in list(df) if i not in ['年', '季']]]
        df = df[~df['年季'].str.contains('12月31日')].reset_index(drop=True)
        del df['年季']
        df['公司代號'] = df['公司代號'].astype(int).astype(str)
        # print(df)
        l.append(df)
        os.chdir(path+i.replace('df', '').replace('.csv', ''))
        for j in os.listdir():
            df = pd.read_csv(j, encoding='big5')
            df['年'] = df['年季'].apply(getyear)
            df['季'] = df['年季'].apply(getmonth)
            df = df[['年', '季'] + [i for i in list(df) if i not in ['年', '季']]]
            df = df[~df['年季'].str.contains('12月31日')].reset_index(drop=True)
            del df['年季']
            df['公司代號'] = df['公司代號'].astype(int).astype(str)
            # print(df)
            l.append(df)
        df1 = cytoolz.reduce(mymerge, l).drop_duplicates(subset=['年', '季', '公司代號']).sort_values(['年', '季'], ascending=[1, 1]).reset_index(drop=True)

        c = conn.cursor()
        table = 'ifrs現金流量表-' + str(df1.公司代號[0])
        sql = 'create table `{}` (`{}`, PRIMARY KEY ({}))'.format(table, '`,`'.join(list(df1)), '`年`, `季`, `公司代號`')
        c.execute(sql)
        sql = 'insert into `{}`(`{}`) values({})'.format(table, '`,`'.join(list(df1)), ','.join('?' * len(list(df1))))
        c.executemany(sql, df1.values.tolist())
        conn.commit()
        print(i.replace('df', '').replace('.csv', ''))
    except Exception as e:
        err.append(i)
        print(e)
        pass
print('finish')

# os.chdir(path + 'till20153')
# err = []
# for i in os.listdir(path+'till20153/'):
#     try:
#         df = pd.read_csv(i, encoding = 'big5').rename(columns = {'證券代號':'公司代號'})
#         df.to_csv(i, index = False)
#     except Exception as e:
#         err.append(i)
#         print(e)
#         pass

140/25