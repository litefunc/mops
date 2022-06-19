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
    m = pd.merge(x, y, on=[col for col in list(x)
                           if col in list(y)], how='outer')
    return m


# ind=['一般業', '銀行業', '金控業', '證券業', '保險業', '其他業', '未知業']

col = {'資產總計': '資產總額', '負債總計': '負債總額', '少數股權': '非控制權益', '股東權益其他調整項目合計': '其他權益',
       '庫藏股票(自98年第4季起併入「其他項目」表達)': '庫藏股票', '股東權益總計': '權益總額', '預收股款(股東權益項下)之約當發行股數(單位:股)': '預收股款（權益項下）之約當發行股數（單位：股）', '母公司暨子公司所持有之母公司庫藏股股數(單位:股)': '母公司暨子公司所持有之母公司庫藏股股數（單位：股）', '每股淨值': '每股參考淨值'}
df0 = pd.read_sql_query("SELECT * from `資產負債表-一般業`", conn_lite)
del df0['Unnamed: 21']
del df0['Unnamed: 22']
df1 = pd.read_sql_query("SELECT * from `ifrs前-合併資產負債表-一般業`", conn_lite)
df1 = df1.rename(columns=col)

columns = list(df0)
varcharColumns = ['年', '季', '公司代號', '公司名稱']
realColumns = list(filter(lambda x: x not in varcharColumns, columns))
df0[varcharColumns] = df0[varcharColumns].astype(str)
df0[realColumns] = df0[realColumns].astype(float)

columns = list(df1)
varcharColumns = ['年', '季', '公司代號', '公司名稱']
realColumns = list(filter(lambda x: x not in varcharColumns, columns))
df1[varcharColumns] = df1[varcharColumns].astype(str)
df1[realColumns] = df1[realColumns].astype(float)

df = mymerge(df0, df1)

# df.dtypes

col1 = ['年', '季', '公司代號',
        '公司名稱',
        '流動資產',
        '非流動資產',
        '基金與投資',
        '固定資產',
        '無形資產',
        '其他資產',
        '資產總額',
        '流動負債',
        '非流動負債',
        '長期負債',
        '各項準備',
        '其他負債',
        '負債總額',
        '股本',
        '權益－具證券性質之虛擬通貨',
        '資本公積',
        '保留盈餘',
        '其他權益',
        '庫藏股票',
        '歸屬於母公司業主之權益合計',
        '共同控制下前手權益',
        '合併前非屬共同控制股權',
        '非控制權益',
        '權益總額',
        '預收股款（權益項下）之約當發行股數（單位：股）',
        '待註銷股本股數（單位：股）',
        '母公司暨子公司所持有之母公司庫藏股股數（單位：股）',
        '每股參考淨值'
        ]

df = df[col1]


df.dtypes
table = 'ifrs前後-資產負債表-' + '一般業'
df = df.sort_values(['年', '季', '公司代號']).reset_index(drop=True)
sql = 'ALTER TABLE `%s` RENAME TO `%s0`' % (table, table)
cur_lite.execute(sql)
sql = 'create table `%s` (`%s`, PRIMARY KEY (%s))' % (
    table, '`,`'.join(list(df)), '`年`, `季`, `公司代號`')
cur_lite.execute(sql)
sql = 'insert into `%s`(`%s`) values(%s)' % (
    table, '`,`'.join(list(df)), ','.join('?' * len(list(df))))
cur_lite.executemany(sql, df.values.tolist())
conn_lite.commit()
sql = "drop table `%s0`" % table
cur_lite.execute(sql)

# ,'':''

# col2={
#  '流動資產':'&emsp;流動資產',
#  '非流動資產':'&emsp;非流動資產',
#      '基金與投資':'&emsp;&emsp;基金與投資',
#      '固定資產':'&emsp;&emsp;固定資產',
#      '無形資產':'&emsp;&emsp;無形資產',
#      '其他資產':'&emsp;&emsp;其他資產',
#
#  '流動負債':'&emsp;流動負債',
#  '非流動負債':'&emsp;非流動負債',
#      '長期負債':'&emsp;&emsp;長期負債',
#      '各項準備':'&emsp;&emsp;各項準備',
#      '其他負債':'&emsp;&emsp;其他負債',
#
#  '股本':'&emsp;股本',
#  '資本公積':'&emsp;資本公積',
#  '保留盈餘':'&emsp;保留盈餘',
#  '其他權益':'&emsp;其他權益',
#  '庫藏股票':'&emsp;庫藏股票',
#  '歸屬於母公司業主之權益合計':'&emsp;歸屬於母公司業主之權益合計',
#  '共同控制下前手權益':'&emsp;共同控制下前手權益',
#  '合併前非屬共同控制股權':'&emsp;合併前非屬共同控制股權',
#  '非控制權益':'&emsp;非控制權益'
# }
