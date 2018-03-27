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

conn_lite = conn_local_lite('mops.sqlite3')
cur_lite = conn_lite.cursor()

def mymerge(x, y):
    m = pd.merge(x, y, on=[i for i in list(x) if i in list(y)], how='outer')
    return m


#---read from sqlite---
# df = pd.read_sql_query("SELECT * from `ifrs前後-綜合損益表`", conn_lite)
# # df['年']=[x.split('/')[0] for x in df['年季']]
# # df['季']=[x.split('/')[1] for x in df['年季']]
# list(df)
# print(list(df))
# col=['年', '季', '公司代號', '公司名稱', '營業收入', '營業成本', '營業毛利（毛損）', '未實現銷貨（損）益', '已實現銷貨（損）益', '營業毛利（毛損）淨額', '營業費用', '其他收益及費損淨額', '營業利益（損失）', '營業外收入及支出', '稅前淨利（淨損）', '所得稅費用（利益）', '繼續營業單位本期淨利（淨損）', '停業單位損益', '合併前非屬共同控制股權損益', '本期淨利（淨損）', '其他綜合損益（淨額）', '合併前非屬共同控制股權綜合損益淨額', '本期綜合損益總額', '淨利（淨損）歸屬於母公司業主', '淨利（淨損）歸屬於共同控制下前手權益', '淨利（淨損）歸屬於非控制權益', '綜合損益總額歸屬於母公司業主', '綜合損益總額歸屬於共同控制下前手權益', '綜合損益總額歸屬於非控制權益', '基本每股盈餘（元）', '利息淨收益', '利息以外淨收益', '呆帳費用及保證責任準備提存（各項提存）', '淨收益', '保險負債準備淨變動', '支出及費用', '收入', '支出', '會計原則變動累積影響數', '呆帳費用', '會計原則變動之累積影響數', '稀釋每股盈餘', '利息收入', '減：利息費用', '收回(提存)各項保險責任準備淨額', '費用', '列計非常損益及會計原則變動累積影響數前損益', '營業支出']
# col1=['營業收入', '營業成本', '營業毛利（毛損）', '未實現銷貨（損）益', '已實現銷貨（損）益', '營業毛利（毛損）淨額', '營業費用', '其他收益及費損淨額', '營業利益（損失）', '營業外收入及支出', '稅前淨利（淨損）', '所得稅費用（利益）', '繼續營業單位本期淨利（淨損）', '停業單位損益', '合併前非屬共同控制股權損益', '本期淨利（淨損）', '其他綜合損益（淨額）', '合併前非屬共同控制股權綜合損益淨額', '本期綜合損益總額', '淨利（淨損）歸屬於母公司業主', '淨利（淨損）歸屬於共同控制下前手權益', '淨利（淨損）歸屬於非控制權益', '綜合損益總額歸屬於母公司業主', '綜合損益總額歸屬於共同控制下前手權益', '綜合損益總額歸屬於非控制權益', '基本每股盈餘（元）', '利息淨收益', '利息以外淨收益', '呆帳費用及保證責任準備提存（各項提存）', '淨收益', '保險負債準備淨變動', '支出及費用', '收入', '支出', '會計原則變動累積影響數', '呆帳費用', '會計原則變動之累積影響數', '稀釋每股盈餘', '利息收入', '減：利息費用', '收回(提存)各項保險責任準備淨額', '費用', '列計非常損益及會計原則變動累積影響數前損益', '營業支出']
# df=df[col]
# df=df.replace('--','NaN')
# for i in col1:
#     try:
#         if df[i].dtypes is dtype('O'):
#             df[[i]] = df[[i]].astype(float)
#     except Exception as e:
#         print(i)
#         print(e)
# df1=df.copy()
# for i in col1:
#     try:
#         df1[i]=df1.groupby(['公司代號', '年'])[i].apply(lambda x: x-x.shift(1)) # 1 when time is ascending
#     except Exception as e:
#         print(i)
#         print(e)
# df1
# # df2=df1.copy()
#
# df1 = df1.sort_values(['年', '季', '公司代號'], ascending=[True, True])
# df1[df1['季'] == '01'] = df[df['季'] == '01']
# df1=df1.sort_values(['公司代號', '年季'], ascending=[True, True])
# df1['grow'] = df.groupby(['公司代號'])['本期綜合損益總額'].pct_change(1)
# df1['grow.ma'] = df1.groupby(['公司代號'])['grow'].apply(rolling_mean, 24)*4
# df1['本期綜合損益總額.wma'] = df1.groupby(['公司代號'])['本期綜合損益總額'].apply(ewma, com=19)*4
# df1['本期綜合損益總額.ma'] = df1.groupby('公司代號')['本期綜合損益總額'].apply(rolling_mean, 12)*4
# df1.to_csv('C:/Users/ak66h_000/OneDrive/webscrap/公開資訊觀測站/彙總報表/季/綜合損益表/綜合損益表(季)-dashboard.csv')
#
# df1 = df1[['公司代號', '公司名稱', '年季', '營業收入', '營業成本', '營業毛利（毛損）', '未實現銷貨（損）益', '已實現銷貨（損）益', '營業毛利（毛損）淨額', '營業費用', '其他收益及費損淨額', '營業利益（損失）', '營業外收入及支出', '稅前淨利（淨損）', '所得稅費用（利益）', '繼續營業單位本期淨利（淨損）', '停業單位損益', '合併前非屬共同控制股權損益', '本期淨利（淨損）', '其他綜合損益（淨額）', '合併前非屬共同控制股權綜合損益淨額', '本期綜合損益總額', '淨利（淨損）歸屬於母公司業主', '淨利（淨損）歸屬於共同控制下前手權益', '淨利（淨損）歸屬於非控制權益', '綜合損益總額歸屬於母公司業主', '綜合損益總額歸屬於共同控制下前手權益', '綜合損益總額歸屬於非控制權益', '基本每股盈餘（元）', '利息淨收益', '利息以外淨收益', '呆帳費用及保證責任準備提存（各項提存）', '淨收益', '保險負債準備淨變動', '支出及費用', '收入', '支出', '會計原則變動累積影響數', '呆帳費用', '會計原則變動之累積影響數', '稀釋每股盈餘', '利息收入', '減：利息費用', '收回(提存)各項保險責任準備淨額', '費用', '列計非常損益及會計原則變動累積影響數前損益', '營業支出']]
# df1=df1.sort_values(['年季', '公司代號'], ascending=[False,True])
# print(df1)
# df1.to_csv('C:/Users/ak66h_000/OneDrive/webscrap/公開資訊觀測站/彙總報表/季/綜合損益表/綜合損益表(季).csv', index=False)

#---new---

sql="SELECT * FROM `%s` " % ('ifrs前後-綜合損益表')
inc = pd.read_sql_query(sql, conn_lite)
inc['年']=[x.split('/')[0] for x in inc['年季']]
inc['季']=[x.split('/')[1] for x in inc['年季']]
inc['公司代號']=inc['公司代號'].astype(str).replace('\.0', '', regex=True)
col=['年季', '年', '季', '公司代號', '公司名稱', '營業收入', '營業成本', '營業毛利（毛損）', '未實現銷貨（損）益', '已實現銷貨（損）益', '營業毛利（毛損）淨額', '營業費用', '其他收益及費損淨額', '營業利益（損失）', '營業外收入及支出', '稅前淨利（淨損）', '所得稅費用（利益）', '繼續營業單位本期淨利（淨損）', '停業單位損益', '合併前非屬共同控制股權損益', '本期淨利（淨損）', '其他綜合損益（淨額）', '合併前非屬共同控制股權綜合損益淨額', '本期綜合損益總額', '淨利（淨損）歸屬於母公司業主', '淨利（淨損）歸屬於共同控制下前手權益', '淨利（淨損）歸屬於非控制權益', '綜合損益總額歸屬於母公司業主', '綜合損益總額歸屬於共同控制下前手權益', '綜合損益總額歸屬於非控制權益', '基本每股盈餘（元）', '利息淨收益', '利息以外淨收益', '呆帳費用及保證責任準備提存（各項提存）', '淨收益', '保險負債準備淨變動', '支出及費用', '收入', '支出', '會計原則變動累積影響數', '呆帳費用', '會計原則變動之累積影響數', '稀釋每股盈餘', '利息收入', '減：利息費用', '收回(提存)各項保險責任準備淨額', '費用', '列計非常損益及會計原則變動累積影響數前損益', '營業支出']
col1=['營業收入', '營業成本', '營業毛利（毛損）', '未實現銷貨（損）益', '已實現銷貨（損）益', '營業毛利（毛損）淨額', '營業費用', '其他收益及費損淨額', '營業利益（損失）', '營業外收入及支出', '稅前淨利（淨損）', '所得稅費用（利益）', '繼續營業單位本期淨利（淨損）', '停業單位損益', '合併前非屬共同控制股權損益', '本期淨利（淨損）', '其他綜合損益（淨額）', '合併前非屬共同控制股權綜合損益淨額', '本期綜合損益總額', '淨利（淨損）歸屬於母公司業主', '淨利（淨損）歸屬於共同控制下前手權益', '淨利（淨損）歸屬於非控制權益', '綜合損益總額歸屬於母公司業主', '綜合損益總額歸屬於共同控制下前手權益', '綜合損益總額歸屬於非控制權益', '基本每股盈餘（元）', '利息淨收益', '利息以外淨收益', '呆帳費用及保證責任準備提存（各項提存）', '淨收益', '保險負債準備淨變動', '支出及費用', '收入', '支出', '會計原則變動累積影響數', '呆帳費用', '會計原則變動之累積影響數', '稀釋每股盈餘', '利息收入', '減：利息費用', '收回(提存)各項保險責任準備淨額', '費用', '列計非常損益及會計原則變動累積影響數前損益', '營業支出']
inc=inc[col]
inc=inc.replace('--','NaN')
def change(df):
    a = np.array(df)
    return pd.DataFrame(np.vstack((a[0], a[1:] - a[0:len(df) - 1])), columns=list(df))
for i in col1:
    if inc[i].dtypes is np.dtype('O'):
        inc[[i]] = inc[[i]].astype(float)
inc[col1]=inc.groupby(['公司代號', '年'])[col1].apply(change).reset_index(drop=True)
inc[col1]=inc[col1].rolling(window=4).sum()
inc['grow'] = inc.groupby(['公司代號'])['本期綜合損益總額'].pct_change(1)
inc['grow.ma'] = inc.groupby(['公司代號'])['grow'].apply(pd.rolling_mean, 24)*4
inc['本期綜合損益總額.wma'] = inc.groupby(['公司代號'])['本期綜合損益總額'].apply(pd.ewma, com=19)*4
inc['本期綜合損益總額.ma'] = inc.groupby('公司代號')['本期綜合損益總額'].apply(pd.rolling_mean, 12)*4


path='C:/Users/ak66h_000/OneDrive/webscrap/公開資訊觀測站/彙總報表/綜合損益表/綜合損益表-一般業/'
os.chdir(path)
L=os.listdir()
df = pd.read_csv(L[-1], encoding='cp950')
table='ifrs前後-綜合損益表'
sql='insert into `%s`(`%s`) values(%s)'%(table, '`,`'.join(list(df)), ','.join('?'*len(list(df))))
cur_lite.executemany(sql, df.values.tolist())
conn_lite.commit()

table='會計師查核報告'
df = pd.read_sql_query("SELECT * from `%s`"%table, conn_lite)
df.年=df.年.astype(int)
df.季=df.季.astype(int)
df.證券代號=df.證券代號.astype(str)
# df.duplicated(['年', '季', '公司代號'])
sql='ALTER TABLE `%s` RENAME TO `%s0`'%(table, table)
cur_lite.execute(sql)
sql='create table `%s` (`%s`, PRIMARY KEY (%s))'%(table, '`,`'.join(list(df)), '`年`, `季`, `證券代號`')
cur_lite.execute(sql)
sql='insert into `%s`(`%s`) values(%s)'%(table, '`,`'.join(list(df)), ','.join('?'*len(list(df))))
cur_lite.executemany(sql, df.values.tolist())
conn_lite.commit()
sql="drop table `%s0`"%table
cur_lite.execute(sql)
print('finish')

#---- from summary to ifrs前後-綜合損益表 ----
from sqlite3 import *
conn_lite = connect('mops.sqlite3')
cur_lite = conn_lite.cursor()
dic = {1: "綜合損益表-銀行業", 2: "綜合損益表-證券業", 3: "綜合損益表-一般業", 4: "綜合損益表-金控業", 5: "綜合損益表-保險業", 6: "綜合損益表-未知業"}
import os
for key in dic:
    try:
        path='C:/Users/ak66h_000/OneDrive/webscrap/公開資訊觀測站/彙總報表/綜合損益表/%s/'%dic[key]
        os.chdir(path)
        L=os.listdir()
        df = pd.read_csv(L[-1], encoding='cp950').rename(columns={'利息以外淨損益':'利息以外淨收益', '繼續營業單位稅前淨利（淨損）':'稅前淨利（淨損）', '所得稅（費用）利益':'所得稅費用（利益）','繼續營業單位本期稅後淨利（淨損）':'繼續營業單位本期淨利（淨損）','本期稅後淨利（淨損）':'本期淨利（淨損）','其他綜合損益（稅後）':'其他綜合損益（淨額）','本期綜合損益總額（稅後）':'本期綜合損益總額','淨利（損）歸屬於母公司業主':'淨利（淨損）歸屬於母公司業主','呆帳費用及保證責任準備提存':'呆帳費用及保證責任準備提存（各項提存）','收益':'營業收入','繼續營業單位稅前純益（純損）':'稅前淨利（淨損）','其他綜合損益':'其他綜合損益（淨額）', '淨利（損）歸屬於共同控制下前手權益':'淨利（淨損）歸屬於共同控制下前手權益', '營業利益':'營業利益（損失）', '繼續營業單位稅前損益':'稅前淨利（淨損）', '繼續營業單位本期純益（純損）':'繼續營業單位本期淨利（淨損）', '淨利（損）歸屬於非控制權益':'淨利（淨損）歸屬於非控制權益', '營業外損益':'營業外收入及支出', '本期其他綜合損益（稅後淨額）':'其他綜合損益（淨額）', '其他綜合損益（稅後淨額）':'其他綜合損益（淨額）','所得稅利益（費用）':'所得稅費用（利益）'})
        df.公司代號=df.公司代號.astype(str)
        sql='insert into `%s`(`%s`) values(%s)'%('ifrs前後-綜合損益表', '`,`'.join(list(df)), ','.join('?'*len(list(df))))
        cur_lite.executemany(sql, df.values.tolist())
        conn_lite.commit()
    except Exception as e:
        print(dic[key], e)

df.dtypes

df = df.sort_values(['公司代號', '年', '季']).reset_index(drop=True)
df0=df[['公司代號','公司名稱', '年', '季']]
df1 = df.groupby(['公司代號', '年']).cumsum()
del df1['季']
df2=pd.concat([df0, df1], axis=1)
print(len(df0), len(df1), len(df2))
list(df)
list(df0)
list(df1)
list(df2)
df2=df2[list(df)].sort_values(['年', '季', '公司代號']).reset_index(drop=True)

table='ifrs前後-資產負債表-一般業'
df2.年=df2.年.astype(int)
df2.季=df2.季.astype(int)
df2.公司代號=df2.公司代號.astype(str)
sql='create table `%s` (`%s`, PRIMARY KEY (%s))'%(table, '`,`'.join(list(df2)), '`年`, `季`, `公司代號`')
cur_lite.execute(sql)
sql='insert into `%s`(`%s`) values(%s)'%(table, '`,`'.join(list(df2)), ','.join('?'*len(list(df2))))
cur_lite.executemany(sql, df2.values.tolist())
conn_lite.commit()
print('finish')

