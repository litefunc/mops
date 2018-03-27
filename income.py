import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import cytoolz.curried
import re
import time
import os
import sys
if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))
import syspath

import sqlCommand as sqlc
import craw.crawler as crawler
from common.connection import conn_local_lite

conn_lite = conn_local_lite('tse.sqlite3')

def mymerge(x, y):
    m = pd.merge(x, y, how='outer')
    return m

# tablename = 'ifrs前後-綜合損益表'
# sql = 'select `公司代號` from `{}` where `年`=2016 and `季`="2"'.format(tablename)
# df = pd.read_sql_query(sql, conn_lite)
# list(df['公司代號'])
# result=conn_lite.execute(sql)
# result.fetchall()
#
# df = sqlCommand.selectAll(tablename, conn_lite).replace('--', 'NaN')
# inColumns = ['年']
# stringColumns = ['季', '公司代號', '公司名稱']
# sqlCommand.changeTypesAndReplace(tablename, inColumns, stringColumns, conn_lite, ['--'], ['NaN'])


# ----get unique id----
# url = 'http://www.twse.com.tw/ch/trading/exchange/BWIBBU/BWIBBU_d.php'
# payload = {
#     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36',
#     'input_date': '105/02/15', 'select2': 'ALL', 'order': 'STKNO'}
# source_code = requests.post(url, params=payload)
# source_code.encoding = 'big5'
# plain_text = source_code.text
# print(plain_text)
# soup = BeautifulSoup(plain_text, 'html.parser')
# date = soup.find_all('thead')[0].find_all('tr')[0].find_all('th')[0].string
#
# h = ['年月日']
# for tr in soup.find_all('thead')[0].find_all('tr')[1]:
#     h.append(tr.text)
# l = [h]
# for tr in soup.find_all('tbody')[0].find_all('tr'):
#     r = [date.split()[0] + date.split()[0]]
#     for td in tr.find_all('td'):
#         r.append(td.string)
#     l.append(r)
# df = DataFrame(l)
# df.columns = df.ix[0, :]
# df = df.ix[1:len(df), :]
#
# id = df.ix[:, 1].unique().tolist()
# for u in id:
#     print(u)
sql = '''select `年月日` from `每日收盤行情(全部(不含權證、牛熊證))` order by `年月日` DESC limit 1'''
lastDay = pd.read_sql_query(sql, conn_lite)['年月日'][0]
sql = 'select `證券代號`, `證券名稱` from `每日收盤行情(全部(不含權證、牛熊證))` where `年月日` ="{}"'.format(lastDay)
ids = list(pd.read_sql_query(sql, conn_lite)['證券代號'])
ids = [id for id in ids if id not in list(filter(lambda x:re.search('^0', x), ids))]

#----test connection----
# CO_ID='2033'
# #2015
# YEAR='2015'
# df1=DataFrame()
# SEASON='2'
# # url = "http://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID="+CO_ID+'&SYEAR='+YEAR+'&SSEASON='+SEASON+'&REPORT_ID=C'
# # source_code = requests.get(url)
# url='http://mops.twse.com.tw/server-java/t164sb01'
# headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
# payload = {'step':'1','CO_ID':CO_ID,'SYEAR':YEAR,'SSEASON':SEASON,'REPORT_ID':'A'}
# source_code = requests.post(url,headers=headers,data=payload) #should use data instead of params
# source_code.encoding = 'big5'
# plain_text = source_code.text
# print(plain_text)
#
# #----test getting data----
# CO_ID='5522'
# stat=2
# L=[]
# for YEAR in ['2015','2014','2013']:
#     for SEASON in ['4', '3', '2', '1']:
#         try:
#             url = "http://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID="+CO_ID+'&SYEAR='+YEAR+'&SSEASON='+SEASON+'&REPORT_ID=C'
#             source_code = requests.get(url)
#             source_code.encoding = 'big5'
#             plain_text = source_code.text
#             soup = BeautifulSoup(plain_text, 'html.parser')
#             h=[]
#             for th in soup.find_all('table')[stat].find_all('tr')[0].find_all('th'):
#                 h.append(th.string)
#             row=len(soup.find_all('table')[stat].find_all('tr')[2].find_all('td', {'align': ""}))
#             td=soup.find_all('table')[stat].find_all('tr')[2].find_all('td')
#             l=[]
#             for j in range(0,len(h)):
#                 x=[h[j]]
#                 for i in range(0, row*len(h),len(h)):
#                     x.append(td[i+j].string)
#                 l.append(x)
#             df = DataFrame(l)
#             df.columns=df.ix[0,:]
#             df=df.ix[1:len(df),:]
#             col = []
#             for i, j in enumerate(list(df)):
#                 if j == '\u3000\u3000 繼續營業單位淨利（淨損）':
#                     col.append(i)
#             cols = list(df)
#             cols[col[1]] = '\u3000\u3000 稀釋繼續營業單位淨利（淨損）'
#             df.columns = cols
#             L.append(df) # do not use L=L.append(df)!!
#         except:
#             pass
# df1 = reduce(mymerge, L)
# print(df1)
# df1=df1.rename(columns={'會計項目':'年季'})
# df2=df1.drop_duplicates(subset='年季')
# df2.index=arange(len(df2))
# df2['證券代號']=CO_ID
# df2=df2.sort_values(['年季', '證券代號'], ascending=[0, 1])
# cols = df2.columns.tolist()
# cols = cols[-1:] + cols[:-1]
# df2=df2[cols]
# df2 = df2.replace(',', '', regex=True)
# print(df2)

# #----create table----
# names = list(df2)
# c = conn_lite.cursor()
# sql = "create table `" + "income" + "`(" + "'" + names[0] + "'"
# for n in names[1:len(names)]:
#      sql = sql + ',' + "'" + n + "'"
# sql = sql + ')'
# c.execute(sql)

# #----test inserting data----
# sql='INSERT INTO `income` VALUES (?'
# n=[',?'] * (len(cols)-1)
# for h in n:
#     sql=sql+h
# sql=sql+')'
# c.executemany(sql, df3.values.tolist())
# conn_lite.commit()

def repl(str, str1):
    global col
    global df
    col = []
    for i, j in enumerate(list(df)):
        if j == str:
            col.append(i)
    if len(col) == 2:
        cols = list(df)
        cols[col[1]] = str+str1
        df.columns = cols

def repl1(str, str1='-推銷費用', str2='-管理費用', str3='-研發費用'):
    global col
    global df
    col = []
    for i, j in enumerate(list(df)):
        if j == '\u3000 營業費用':
            aa=i
        if j == '\u3000\u3000 推銷費用':
            a=i
        if j == '\u3000\u3000 管理費用':
            b=i
        if j == '\u3000\u3000 研究發展費用':
            c=i
        if j == '\u3000\u3000 營業費用合計':
            bb=i
        if j == str:
            col.append(i)
    if len(col) == 2:
        if col[0]>aa and col[1]<bb:
            cols = list(df)
            if col[0]>b:
                cols[col[0]] = str+str2
                cols[col[1]] = str+str3
                df.columns = cols
            else:
                if col[1]>c:
                    cols[col[0]] = str+str1
                    cols[col[1]] = str+str3
                    df.columns = cols
                else:
                    cols[col[0]] = str+str1
                    cols[col[1]] = str+str2
                    df.columns = cols
    if len(col) == 3:
        if col[0] > aa and col[2] < bb:
            cols = list(df)
            cols[col[0]] = str+str1
            cols[col[1]] = str+str2
            cols[col[2]] = str+str3
            df.columns = cols

def repl2(str, str1):
    global col
    global df
    col = []
    for i, j in enumerate(list(df)):
        if j == str:
            col.append(i)
    if len(col) == 2:
        cols = list(df)
        cols[col[0]] = str + str1
        df.columns = cols
# def repl2(str,str1='-推銷費用',str2='-管理費用',str3='-研發費用'):
#     col = []
#     for i, j in enumerate(list(df)):
#         if j == '\u3000 營業費用':
#             aa=i
#         if j == '\u3000\u3000 營業費用合計':
#             bb=i
#         if j == str:
#             col.append(i)
#         if len(col) == 2:
#             cols = list(df)
#             if col[0] > aa and col[1] < bb:
#                 if '\u3000\u3000 推銷費用' not in cols:
#                     cols[col[0]] = str + str2
#                     cols[col[1]] = str + str3
#                     df.columns = cols
#                 if '\u3000\u3000 管理費用' not in cols:
#                     cols[col[0]] = str + str1
#                     cols[col[1]] = str + str3
#                     df.columns = cols
#                 if '\u3000\u3000 研發費用' not in cols:
#                     cols[col[0]] = str + str1
#                     cols[col[1]] = str + str2
#                     df.columns = cols


#----main----
# tse好像會阻擋,而且資料會有少

stat=2
id_e=[]
dupl=pd.DataFrame()
for CO_ID in id:
    try:
        L = []
        for YEAR in ['2017']:
            print('Wait 4 seconds')
            time.sleep(4)
            for SEASON in ['1']:
                print(CO_ID+' '+YEAR+' '+SEASON)
                try:
                    url='http://mops.twse.com.tw/server-java/t164sb01'
                    headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
                    payload = {'step':'1','CO_ID':CO_ID,'SYEAR':YEAR,'SSEASON':SEASON,'REPORT_ID':'C'}
                    source_code = requests.post(url,headers=headers, data=payload)
                    source_code.encoding = 'big5'
                    plain_text = source_code.text
                    soup = BeautifulSoup(plain_text, 'html.parser')
                    h=[]
                    for th in soup.find_all('table')[stat].find_all('tr')[0].find_all('th'):
                        h.append(th.string)
                    row=len(soup.find_all('table')[stat].find_all('tr')[2].find_all('td',{'align':""}))
                    td=soup.find_all('table')[stat].find_all('tr')[2].find_all('td')
                    l=[]
                    for j in range(0,len(h)):
                        x=[h[j]]
                        for i in range(0, row*len(h),len(h)):
                            x.append(td[i+j].string)
                        l.append(x)
                    df = pd.DataFrame(l)
                    df.columns=df.ix[0,:]
                    df=df.ix[1:len(df),:]
                    col = []
                    for i, j in enumerate(list(df)):
                        if j == '\u3000\u3000 繼續營業單位淨利（淨損）':
                            col.append(i)
                    if len(col)==2:
                        cols = list(df)
                        cols[col[1]] = '\u3000\u3000 稀釋繼續營業單位淨利（淨損）'
                        df.columns = cols
                    col = []
                    for i, j in enumerate(list(df)):
                        if j == '\u3000\u3000 停業單位淨利（淨損）':
                            col.append(i)
                    if len(col)==2:
                        cols = list(df)
                        cols[col[1]] = '\u3000\u3000 稀釋停業單位淨利（淨損）'
                        df.columns = cols
                    col = []
                    for i, j in enumerate(list(df)):
                        if j == '\u3000\u3000\u3000 與待出售非流動資產直接相關之權益':
                            col.append(i)
                    if len(col)==2:
                        cols = list(df)
                        cols[col[1]] = '\u3000\u3000\u3000 與待出售非流動資產直接相關之權益-後續可能'
                        df.columns = cols
                    col = []
                    for i, j in enumerate(list(df)):
                        if j == '\u3000\u3000 母公司業主':
                            col.append(i)
                    if len(col)==2:
                        cols = list(df)
                        cols[col[1]] = '\u3000\u3000 母公司業主-綜合損益'
                        df.columns = cols
                    col = []
                    for i, j in enumerate(list(df)):
                        if j == '\u3000\u3000 非控制股權':
                            col.append(i)
                    if len(col)==2:
                        cols = list(df)
                        cols[col[1]] = '\u3000\u3000 非控制股權-綜合損益'
                        df.columns = cols
                    col = []
                    for i, j in enumerate(list(df)):
                        if j == '\u3000\u3000 非控制股權':
                            col.append(i)
                    if len(col)==2:
                        cols = list(df)
                        cols[col[1]] = '\u3000\u3000 非控制股權-綜合損益'
                        df.columns = cols
                    col = []
                    for i, j in enumerate(list(df)):
                        if j == '\u3000\u3000 非控制股權':
                            col.append(i)
                    if len(col)==2:
                        cols = list(df)
                        cols[col[1]] = '\u3000\u3000 非控制股權-綜合損益'
                        df.columns = cols
                    repl('\u3000\u3000\u3000 與待出售非流動資產直接相關之權益', '-後續可能')
                    repl('\u3000\u3000 繼續營業單位淨利（損）', '-稀釋')
                    repl('\u3000\u3000 母公司業主', '-綜合損益')
                    repl('\u3000\u3000 非控制股權', '-綜合損益')
                    repl('\u3000\u3000 繼續營業單位稅後淨利（淨損）', '-稀釋')
                    repl('\u3000\u3000 共同控制下前手權益', '-綜合損益')
                    repl1('\u3000\u3000\u3000 薪資支出')
                    repl1('\u3000\u3000\u3000 租金支出')
                    repl1('\u3000\u3000\u3000 文具用品')
                    repl1('\u3000\u3000\u3000 旅費')
                    repl1('\u3000\u3000\u3000 運費')
                    repl1('\u3000\u3000\u3000 郵電費')
                    repl1('\u3000\u3000\u3000 廣告費')
                    repl1('\u3000\u3000\u3000 修繕費')
                    repl1('\u3000\u3000\u3000 水電瓦斯費')
                    repl1('\u3000\u3000\u3000 保險費')
                    repl1('\u3000\u3000\u3000 交際費')
                    repl1('\u3000\u3000\u3000 捐贈')
                    repl1('\u3000\u3000\u3000 稅捐')
                    repl1('\u3000\u3000\u3000 折舊')
                    repl1('\u3000\u3000\u3000 各項攤提')
                    repl1('\u3000\u3000\u3000 伙食費')
                    repl1('\u3000\u3000\u3000 職工福利')
                    repl1('\u3000\u3000\u3000 訓練費')
                    repl1('\u3000\u3000\u3000 其他費用')
                    repl2('\u3000\u3000\u3000 利息費用', '-營業成本')
                    repl2('\u3000\u3000\u3000 利息收入', '-營業收入')

                    dup=[item for idx, item in enumerate(list(df)) if item in list(df)[:idx]]
                    if len(dup)>0:
                        cols = [CO_ID+' '+YEAR+' '+SEASON]
                        for i in dup:
                            cols.append(i)
                            du = pd.DataFrame(cols)
                            du.columns = du.ix[0, :]
                            du = du.ix[1:len(du), :]
                        dupl=pd.concat([dupl,du],axis=1)
                        print(dupl)
                        dupl.to_csv('C:/Users/user/Dropbox/program/crawler/finance/公開資訊觀測站/csvfiles/du2'+str(stat)+'.csv', index=False)
                    # dup=[idx for idx, item in enumerate(list(df)) if item in list(df)[:idx]]
                    # if len(dup)>0:
                    #     cols = list(df)
                    #     for i in dup:
                    #         cols[i] = cols[i]+'[duplicate]'+str(i)
                    #     df.columns = cols
                    L.append(df) # do not use L=L.append(df)
                except Exception as e:
                    print(e)
                    pass
        df1=cytoolz.reduce(mymerge,L)
        df1=df1.rename(columns={'會計項目':'年季'})
        df2=df1.drop_duplicates(subset='年季')
        df2.index=np.arange(len(df2))
        df2['證券代號']=CO_ID
        df2=df2.sort_values(['年季', '證券代號'], ascending=[0, 1])
        cols = df2.columns.tolist()
        cols = cols[-1:] + cols[:-1]
        df2=df2[cols]
        df2 = df2.replace(',', '', regex=True)
        print(df2)
        #df4=concat([df4, df3], ignore_index=True)
        #df4.to_csv('C:/Users/ak66h_000/OneDrive/webscrap/income/df4.csv',index=False)
        path='C:/Users/user/Dropbox/program/crawler/finance/公開資訊觀測站/csvfiles/incomedu/df'+CO_ID+'.csv'
        df2.to_csv(path,index=False)
    except Exception as e:
        id_e.append(CO_ID)
        print(e)
        print('Wait 20 seconds...')
        time.sleep(20)
        print('Continue')
        pass
id2 = id_e
print(id2)

#---continue---
# import os
# os.getcwd()
# dir()
# os.listdir()
# path='C:/Users/user/Dropbox/program/crawler/finance/公開資訊觀測站/csvfiles/income'
# os.chdir(path)
# L=os.listdir()
#
# id1=[x.replace('.csv', '') for x in L]
# id1=[x.replace('df', '') for x in id1]
# id2=[]
# for i in id:
#     if i not in id1:
#         id2.append(i)
# print(id2)
# print(len(id2))
#
# #---read csv---
# import os
# path = 'C:/Users/user/Dropbox/program/crawler/finance/公開資訊觀測站/csvfiles/income'
# L = os.listdir()
# l = []
# for i in L:
#     df = pd.read_csv(i, encoding='big5')
#     l.append(df)
# # df=concat(l, ignore_index=True)
# df= reduce(mymerge, l)
#
# name=list(df)
# list(df)
#
# import re
# dup=[x for x in name if re.search(r'\[duplicate].*', x) is not None]
# dup
# [x for x in name if re.search(r'\.1', x) is not None]
# df.to_csv('C:/Users/user/Dropbox/program/crawler/finance/公開資訊觀測站/csvfiles/income.csv', index=False)
# df=read_csv('C:/Users/user/Dropbox/program/crawler/finance/公開資訊觀測站/csvfiles/income.csv', encoding='big5')
# print(read_csv('C:/Users/user/Dropbox/program/crawler/finance/公開資訊觀測站/csvfiles/du2.csv', encoding='big5'))
#
# c=[list(x) for x in l]
# c1=[x for x in l if '\u3000\u3000\u3000 與待出售非流動資產直接相關之權益.1' in x]
# [str(x['證券代號'][0]) for x in c1]
# len(c1)
#
# x=['a', 'b', 'c']
# y=['a', 'b']
# y in x
# # DataFrame(name).transpose().to_csv('C:/Users/ak66h_000/OneDrive/webscrap/incomename.csv',index=False)
# DataFrame(name).to_csv('C:/Users/ak66h_000/OneDrive/webscrap/incomename.csv', index=False)
# read_csv('C:/Users/ak66h_000/OneDrive/webscrap/incomename.csv', encoding='big5')['0'].tolist()
# print(read_csv('C:/Users/ak66h_000/OneDrive/webscrap/incomename.csv', encoding='big5')['0'].tolist())
#
# phoneNumRegex = re.compile(r'[abc]{3}')
# phoneNumRegex.search('abc')
#
#
# df.年=df.年.astype(int)
# df.季=df.季.astype(int)
# df1
# path='C:/Users/ak66h_000/OneDrive/webscrap/公開資訊觀測站/彙總報表/綜合損益表/綜合損益表-一般業/'
# os.chdir(path)
# L=os.listdir()
# df = read_csv(L[-1], encoding='cp950')
# df=df[0:1]
# col = list(df)
# df['年']=int(df['年季'][0][12:16])
# df['季']=int(df['年季'][0][18])-2
# col1 = ['年', '季']
# df=df[col1+col]
# del df['年季']
#
# from sqlite3 import *
# conn_lite = connect('C:\\Users\\ak66h_000\\Documents\\mops.sqlite3')
# c = conn_lite.cursor()
#
# import requests
# from bs4 import BeautifulSoup
# from numpy import *
# from pandas import *
# from functools import *
#
# get_option("display.max_rows")
# get_option("display.max_columns")
# set_option("display.max_rows", 1000)
# set_option("display.max_columns", 1000)
# set_option('display.expand_frame_repr', False)
# set_option('display.unicode.east_asian_width', True)
# df0 = read_sql_query("SELECT * from `ifrs前後-綜合損益表`", conn_lite).replace('', nan)
# df0['季']=df0['季'].str[1].astype(int)
# df0['年']=df0['年'].astype(int)
#
# df1 = mymerge(df0, df)
# df = df.rename(columns={'證券代號': '公司代號'})
# list(df0)
# len(df0.columns)
# len(df.columns)
#
#
# conn_lite = connect('C:\\Users\\ak66h_000\\Documents\\mops.sqlite3')
# c = conn_lite.cursor()
# # df0 = read_sql_query("SELECT * from `ifrs前後-綜合損益表`", conn_lite)
# # df0['季']=df0['季'].str[1].astype(int)
# # df0['年']=df0['年'].astype(int)
#
# table='ifrs前後-綜合損益表'
# df = read_sql_query("SELECT * from `{}`".format(table, conn_lite)
# df.年=df.年.astype(int)
# df.季=df.季.astype(int)
# # df.公司代號=df.公司代號.astype(int)
# sql='ALTER TABLE `{}` RENAME TO `{}0`'.format(table, table)
# c.execute(sql)
# sql='create table `{}` (`{}`, PRIMARY KEY ({}))'.format(table, '`,`'.join(list(df)), '`年`, `季`, `公司代號`')
# c.execute(sql)
# sql='insert into `{}`(`{}`) values({})'.format(table, '`,`'.join(list(df)), ','.join('?'*len(list(df))))
# c.executemany(sql, df.values.tolist())
# conn_lite.commit()
# sql="drop table `{}0`".format(table
# c.execute(sql)
#
# table='ifrs前後-綜合損益表'
# import os
# path='C:/Users/ak66h_000/OneDrive/webscrap/incomedu/'
# os.chdir(path)
# L=os.listdir()
# d=dict({'3':1, '6':2, '9':3, '12':4})
# for f in L:
#     try:
#         df = read_csv(f, encoding='cp950')
#         df.columns = [x.strip() for x in list(df)]
#         df=df.rename(columns={'證券代號': '公司代號', '營業收入合計':'營業收入', '銷貨收入':'營業收入', '銷貨成本':'營業成本', '營業成本合計':'營業成本'})
#         df0 = df[~df.年季.str.contains('季')]
#         df0.insert(0, '年', df0.年季.str.extract('(\d{4})'))
#         df0.insert(1, '季', df0.年季.str.extract('(.+)(\d{2})(.{4})')[1].str[1].replace(d))
#         del df0['年季']
#         for i in range(len(df0)):
#             sql='insert into `%s`(`%s`) values(%s)'%(table, '`,`'.join(list(df0)), ','.join('?'*len(list(df0))))
#             c.executemany(sql, df0.iloc[0+i:1+i,:].values.tolist())
#             conn_lite.commit()
#     except Exception as e:
#         print(e)
#         print(f)




