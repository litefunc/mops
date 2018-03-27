from sqlite3 import *
conn = connect('C:/Users/ak66h_000/Documents/db/mops.sqlite3')
c = conn.cursor()

from numpy import *
from pandas import *
from functools import *

def mymerge(x, y):
    m = merge(x, y, how='outer')
    return m

import os
import re
os.getcwd()
dir()
os.listdir()

col= {'營業毛利(毛損)': '營業毛利（毛損）', '營業毛利': '營業毛利（毛損）', '營業利益': '營業利益（損失）', '營業淨利(淨損)': '營業利益（損失）',
      '營業利益(損失)': '營業利益（損失）', '繼續營業單位淨利(淨損)': '繼續營業單位本期淨利（淨損）',
      '非常損益': '其他綜合損益（淨額）', '非常損益(稅後)': '其他綜合損益（淨額）', '合併淨損益': '綜合損益總額歸屬於母公司業主',
      '少數股權損益': '綜合損益總額歸屬於非控制權益',
      '共同控制下前手權益損益': '綜合損益總額歸屬於共同控制下前手權益', '基本每股盈餘': '基本每股盈餘（元）',
      '繼續營業單位稅後純益(純損)': '繼續營業單位本期淨利（淨損）', '繼續營業單位稅後淨利(淨損)': '繼續營業單位本期淨利（淨損）',
      '繼續營業單位稅後淨利': '繼續營業單位本期淨利（淨損）', '繼續營業單位稅後合併淨利(淨損)': '繼續營業單位本期淨利（淨損）',
      '停業單位損益(稅後)': '停業單位損益', '合併總損益': '本期綜合損益總額', '合併總損益歸屬予_母公司股東': '綜合損益總額歸屬於母公司業主',
      '繼續營業單位稅前純益(純損)': '稅前淨利（淨損）', '繼續營業單位稅前淨利（淨損）': '稅前淨利（淨損）', '繼續營業單位稅前合併淨利(淨損)': '稅前淨利（淨損）',
      '繼續營業單位稅前淨利': '稅前淨利（淨損）', '繼續營業單位稅前淨利(淨損)': '稅前淨利（淨損）',  '合併總損益歸屬予_少數股權': '綜合損益總額歸屬於非控制權益',
      '會計原則變動之累積影響數(稅後)': '會計原則變動之累積影響數', '所得稅費用(利益)': '所得稅費用（利益）', '所得稅(費用)利益': '所得稅費用（利益）',
      '每股盈餘': '基本每股盈餘（元）', '每股稅後盈餘': '基本每股盈餘（元）', '利息以外淨損益': '利息以外淨收益'}
industry=['9904', '9903', '9902', '9901', '9804', '9803', '9802', '9801', '9704', '9703', '9702', '9701', '9604', '9603', '9602', '9601', '9504', '9503', '9502', '9501', '9404', '9403', '9402', '9401', '9304', '9303', '9302', '9301']
for ind in industry:
    path = 'C:/Users/ak66h_000/OneDrive/webscrap/公開資訊觀測站/彙總報表/ifrs前/合併損益表/'+ind
    os.chdir(path)
    l = os.listdir()
    L = []
    for i in l:
        df = read_csv(i, encoding='cp950')
        if '營業外收入及利益' and '營業外費用及損失' in list(df):
            df['營業外收入及支出']= df['營業外收入及利益']-df['營業外費用及損失']
            df=df.drop(['營業外收入及利益', '營業外費用及損失'], 1)
        t = re.findall(r'\d', ind)
        t = str(int(t[0] + t[1])+1911) + '/' + t[2] + t[3]
        d = {'年季': repeat(t, len(df))}
        df1 = DataFrame(d)
        df = concat([df1, df], axis=1)
        if '所得稅(費用)利益' in list(df):
            df['所得稅(費用)利益'] = df['所得稅(費用)利益'] * (-1)
        df = df.rename(columns=col)
        L.append(df)
    if len(l) > 0:
        if len(l)>1:
            df1 = reduce(mymerge, L)
        else :
            df1=L[0]
        df1=df1.sort_values(['年季','公司代號'],ascending=[True,True])
        df1.to_csv('C:/Users/ak66h_000/OneDrive/webscrap/公開資訊觀測站\彙總報表/ifrs前/合併損益表/合併/merge'+ind+'.csv',index=False)

industry = ['10104', '10103', '10102', '10101', '10004', '10003', '10002', '10001']
for ind in industry:
    path = 'C:/Users/ak66h_000/OneDrive/webscrap/公開資訊觀測站\彙總報表/ifrs前/合併損益表/' + ind
    os.chdir(path)
    l = os.listdir()
    L = []
    for i in l:
        df = read_csv(i, encoding='cp950')
        if '營業外收入及利益' and '營業外費用及損失' in list(df):
            df['營業外收入及支出']= df['營業外收入及利益']-df['營業外費用及損失']
            df=df.drop(['營業外收入及利益', '營業外費用及損失'], 1)
        t = re.findall(r'\d', ind)
        t = str(int(t[0] + t[1] + t[2]) + 1911) + '/' + t[3] + t[4]
        d = {'年季': repeat(t, len(df))}
        df1 = DataFrame(d)
        df = concat([df1, df], axis=1)
        if '所得稅(費用)利益' in list(df):
            df['所得稅(費用)利益'] = df['所得稅(費用)利益'] * (-1)
        df = df.rename(columns=col)
        L.append(df)
    if len(l) > 0:
        if len(l)>1:
            df1 = reduce(mymerge, L)
        else :
            df1=L[0]
        df1 = df1.sort_values(['年季', '公司代號'], ascending=[True, True])
        df1.to_csv('C:/Users/ak66h_000/OneDrive/webscrap/公開資訊觀測站\彙總報表/ifrs前/合併損益表/合併/merge' + ind + '.csv', index=False)
print('finish')

import os
path = 'C:/Users/ak66h_000/OneDrive/webscrap/公開資訊觀測站/彙總報表/ifrs前/合併損益表/合併'
os.chdir(path)
l = os.listdir()
L = []
for i in l:
    df = read_csv(i, encoding='cp950')
    # df1 = DataFrame()
    # df = concat([df1, df], axis=1)
    L.append(df)
df1 = reduce(mymerge, L)
df1=df1.dropna(subset = ['年季'])
df1=df1.dropna(subset = ['公司代號'])
df1['公司代號']=df1['公司代號'].replace('\.0','',regex=True)
df1['公司代號']=df1['公司代號'].astype(int)
df1=df1.sort_values(['年季','公司代號'],ascending=[True,True]) # 2004/02 only contains 9104, so 9104 is in the 1st record
df1.dtypes
print(df1)
df0=df1.copy()
list(df0)

# ----create table----
names = list(df1)
conn = connect('C:\\Users\\ak66h_000\\Documents\\TEJ.sqlite3')
c = conn.cursor()
sql = "create table `" + '合併損益表'+ "`(" + "'" + names[0] + "'"
for n in names[1:len(names)]:
    sql = sql + ',' + "'" + n + "'"
sql = sql + ',PRIMARY KEY (`年`,`季`,`公司代號`))'
c.execute(sql)
# ----inserting data----
sql = 'INSERT INTO `' + '合併損益表'+ '` VALUES (?'
n = [',?'] * (len(names) - 1)
for h in n:
    sql = sql + h
sql = sql + ')'
c.executemany(sql, df1.values.tolist())
conn.commit()
print('done')

list(df0)
list(df1)

# df = read_sql_query("SELECT * from `綜合損益表-一般業`", conn)
# df1 = read_sql_query("SELECT * from `綜合損益表-保險業`", conn)
# df1 = df1.rename(
#     columns={'繼續營業單位本期純益（純損）': '繼續營業單位本期淨利（淨損）', '繼續營業單位稅前純益（純損）': '稅前淨利（淨損）', '其他綜合損益（稅後淨額）': '其他綜合損益（淨額）'})
# df = merge(df, df1, how='outer')
# df1 = read_sql_query("SELECT * from `綜合損益表-銀行業`", conn)
# df1['所得稅（費用）利益'] = df1['所得稅（費用）利益'] * (-1)
# df1 = df1.rename(columns={'所得稅（費用）利益': '所得稅費用（利益）', '利息以外淨損益': '利息以外淨收益', '繼續營業單位稅前淨利（淨損）': '稅前淨利（淨損）'})
# df1 = df1.rename(columns={'繼續營業單位本期稅後淨利（淨損）': '繼續營業單位本期淨利（淨損）', '本期稅後淨利（淨損）': '本期淨利（淨損）', '其他綜合損益（稅後）': '其他綜合損益（淨額）',
#                           '本期綜合損益總額（稅後）': '本期綜合損益總額', '淨利（損）歸屬於母公司業主': '淨利（淨損）歸屬於母公司業主',
#                           '淨利（損）歸屬於共同控制下前手權益': '淨利（淨損）歸屬於共同控制下前手權益', '淨利（損）歸屬於非控制權益': '淨利（淨損）歸屬於非控制權益'})
# df = merge(df, df1, how='outer')
# df1 = read_sql_query("SELECT * from `綜合損益表-金控業`", conn)
# df1['所得稅（費用）利益'] = df1['所得稅（費用）利益'] * (-1)
# df1 = df1.rename(columns={'所得稅（費用）利益': '所得稅費用（利益）', '繼續營業單位稅前損益': '稅前淨利（淨損）', '呆帳費用及保證責任準備提存': '呆帳費用及保證責任準備提存（各項提存）'})
# df1 = df1.rename(columns={'本期稅後淨利（淨損）': '本期淨利（淨損）', '本期其他綜合損益（稅後淨額）': '其他綜合損益（淨額）'})
# df = merge(df, df1, how='outer')
# df1 = read_sql_query("SELECT * from `綜合損益表-證券業`", conn)
# df1['所得稅利益（費用）'] = df1['所得稅利益（費用）'] * (-1)
# df1 = df1.rename(columns={'所得稅利益（費用）': '所得稅費用（利益）', '收益': '營業收入', '營業利益': '營業利益（損失）', '營業外損益': '營業外收入及支出'})
# df1 = df1.rename(columns={'淨利（損）歸屬於母公司業主': '淨利（淨損）歸屬於母公司業主', '淨利（損）歸屬於共同控制下前手權益': '淨利（淨損）歸屬於共同控制下前手權益',
#                           '淨利（損）歸屬於非控制權益': '淨利（淨損）歸屬於非控制權益', '本期其他綜合損益（稅後淨額）': '其他綜合損益（淨額）'})
# df = merge(df, df1, how='outer')
# df1 = read_sql_query("SELECT * from `綜合損益表-未知業`", conn)
# df1 = df1.rename(columns={'其他綜合損益': '其他綜合損益（淨額）'})
# df2 = merge(df, df1, how='outer')

col = {'繼續營業單位本期純益（純損）': '繼續營業單位本期淨利（淨損）', '繼續營業單位稅前純益（純損）': '稅前淨利（淨損）', '其他綜合損益（稅後淨額）': '其他綜合損益（淨額）',
       '所得稅（費用）利益': '所得稅費用（利益）', '利息以外淨損益': '利息以外淨收益', '繼續營業單位稅前淨利（淨損）': '稅前淨利（淨損）',
       '繼續營業單位本期稅後淨利（淨損）': '繼續營業單位本期淨利（淨損）', '本期稅後淨利（淨損）': '本期淨利（淨損）', '其他綜合損益（稅後）': '其他綜合損益（淨額）',
       '本期綜合損益總額（稅後）': '本期綜合損益總額', '淨利（損）歸屬於母公司業主': '淨利（淨損）歸屬於母公司業主', '淨利（損）歸屬於共同控制下前手權益': '淨利（淨損）歸屬於共同控制下前手權益',
       '淨利（損）歸屬於非控制權益': '淨利（淨損）歸屬於非控制權益', '繼續營業單位稅前損益': '稅前淨利（淨損）', '呆帳費用及保證責任準備提存': '呆帳費用及保證責任準備提存（各項提存）',
       '本期其他綜合損益（稅後淨額）': '其他綜合損益（淨額）', '所得稅利益（費用）': '所得稅費用（利益）', '收益': '營業收入', '營業利益': '營業利益（損失）', '營業外損益': '營業外收入及支出',
       '其他綜合損益': '其他綜合損益（淨額）'}
df0 = read_sql_query("SELECT * from `合併損益表`", conn)
# df0['公司代號'].tolist()
# df0['公司名稱'].tolist()
df = read_sql_query("SELECT * from `綜合損益表-一般業`", conn)
df1 = read_sql_query("SELECT * from `綜合損益表-保險業`", conn)
df1 = df1.rename(columns=col)
df = mymerge(df, df1)
df1 = read_sql_query("SELECT * from `綜合損益表-銀行業`", conn)
df1['所得稅（費用）利益']=df1['所得稅（費用）利益']*(-1)
df1 = df1.rename(columns=col)
df = mymerge(df, df1)
df1 = read_sql_query("SELECT * from `綜合損益表-金控業`", conn)
df1['所得稅（費用）利益']=df1['所得稅（費用）利益']*(-1)
df1 = df1.rename(columns=col)
df = mymerge(df, df1)
df1 = read_sql_query("SELECT * from `綜合損益表-證券業`", conn)
df1['所得稅利益（費用）']=df1['所得稅利益（費用）']*(-1)
df1 = df1.rename(columns=col)
df = mymerge(df, df1)
df1 = read_sql_query("SELECT * from `綜合損益表-未知業`", conn)
df1 = df1.rename(columns=col)
df2 = mymerge(df, df1)

# df1 = read_sql_query("SELECT * from `合併損益表`", conn)
df3=mymerge(df2, df0)
df3=df3.sort_values(['年', '季', '公司代號'])
df3
list(df0)
list(df3)

# rename table
report='ifrs前後-綜合損益表'
sql='ALTER TABLE `' + report +'` RENAME TO`' + report +'0`'
c.execute(sql)

# df = read_sql_query('SELECT * from `'+report +'0`', conn)
# df3['年'] = [x.split('/')[0] for x in df3['年季']]
# df3['季'] = [x.split('/')[1] for x in df3['年季']]
# df3=df3.drop(['年季'], axis=1)
# df3=df3[['年', '季']+[col for col in df3.columns if col not in ['年', '季']]]
df3['公司代號'] = df3['公司代號'].astype(str).replace('\.0', '', regex=True)
df3['公司代號'] = df3['公司代號'].str.strip()
df3['公司名稱'] = df3['公司名稱'].str.strip()
df3 = df3.drop_duplicates(['年', '季', '公司代號'])
# ----create table----
names = list(df3)
c = conn.cursor()
sql = "create table `" + report + "`(" + "'" + names[0] + "'"
for n in names[1:len(names)]:
    sql = sql + ',' + "'" + n + "'"
sql = sql + ', PRIMARY KEY (`年`, `季`, `公司代號`))'
c.execute(sql)
# ----inserting data----
sql = 'INSERT INTO `' + report + '` VALUES (?'
n = [',?'] * (len(names) - 1)
for h in n:
    sql = sql + h
sql = sql + ')'
c.executemany(sql, df3.values.tolist())
conn.commit()
print('done')

c = conn.cursor()
sql = "drop table `" + report + "0`"
c.execute(sql)
