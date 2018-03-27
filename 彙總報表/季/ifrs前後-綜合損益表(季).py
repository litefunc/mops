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
sql = "SELECT * FROM `%s` " % ('ifrs前後-綜合損益表')
inc = pd.read_sql_query(sql, conn_lite).replace('--', np.nan).replace('', np.nan)
# inc['年'] = [x.split('/')[0] for x in inc['年季']]
# inc['季'] = [x.split('/')[1] for x in inc['年季']]
# inc['公司代號'] = inc['公司代號'].astype(str).replace('\.0', '', regex=True)
col = ['年', '季', '公司代號', '公司名稱', '營業收入', '營業成本', '營業毛利（毛損）', '未實現銷貨（損）益', '已實現銷貨（損）益', '營業毛利（毛損）淨額', '營業費用',
       '其他收益及費損淨額', '營業利益（損失）', '營業外收入及支出', '稅前淨利（淨損）', '所得稅費用（利益）', '繼續營業單位本期淨利（淨損）', '停業單位損益', '合併前非屬共同控制股權損益',
       '本期淨利（淨損）', '其他綜合損益（淨額）', '合併前非屬共同控制股權綜合損益淨額', '本期綜合損益總額', '淨利（淨損）歸屬於母公司業主', '淨利（淨損）歸屬於共同控制下前手權益',
       '淨利（淨損）歸屬於非控制權益', '綜合損益總額歸屬於母公司業主', '綜合損益總額歸屬於共同控制下前手權益', '綜合損益總額歸屬於非控制權益', '基本每股盈餘（元）', '利息淨收益', '利息以外淨收益',
       '呆帳費用及保證責任準備提存（各項提存）', '淨收益', '保險負債準備淨變動', '支出及費用', '收入', '支出', '會計原則變動累積影響數', '呆帳費用', '會計原則變動之累積影響數', '稀釋每股盈餘',
       '利息收入', '減：利息費用', '收回(提存)各項保險責任準備淨額', '費用', '列計非常損益及會計原則變動累積影響數前損益', '營業支出']
col1 = ['營業收入', '營業成本', '營業毛利（毛損）', '未實現銷貨（損）益', '已實現銷貨（損）益', '營業毛利（毛損）淨額', '營業費用', '其他收益及費損淨額', '營業利益（損失）', '營業外收入及支出',
        '稅前淨利（淨損）', '所得稅費用（利益）', '繼續營業單位本期淨利（淨損）', '停業單位損益', '合併前非屬共同控制股權損益', '本期淨利（淨損）', '其他綜合損益（淨額）',
        '合併前非屬共同控制股權綜合損益淨額', '本期綜合損益總額', '淨利（淨損）歸屬於母公司業主', '淨利（淨損）歸屬於共同控制下前手權益', '淨利（淨損）歸屬於非控制權益', '綜合損益總額歸屬於母公司業主',
        '綜合損益總額歸屬於共同控制下前手權益', '綜合損益總額歸屬於非控制權益', '基本每股盈餘（元）', '利息淨收益', '利息以外淨收益', '呆帳費用及保證責任準備提存（各項提存）', '淨收益',
        '保險負債準備淨變動', '支出及費用', '收入', '支出', '會計原則變動累積影響數', '呆帳費用', '會計原則變動之累積影響數', '稀釋每股盈餘', '利息收入', '減：利息費用',
        '收回(提存)各項保險責任準備淨額', '費用', '列計非常損益及會計原則變動累積影響數前損益', '營業支出']
inc = inc[col]
inc.dtypes
# def change(s):
#     a = np.array(s)
#     return pd.Series(append(a[0], a[1:] - a[0:len(s) - 1]),name=s.name)
for i in col1:
    if inc[i].dtypes is np.dtype('O'):
        inc[[i]] = inc[[i]].astype(float)

inc.年, inc.季, inc.公司代號 = inc.年.astype(str), inc.季.astype(str), inc.公司代號.astype(str)

# def change0(s):
#     if s.dtypes == 'object':
#         return s
#     else:
#         return s-s
# inc.apply(change0)
# inc.groupby(['公司代號', '年']).apply(change0)    # apply applies function to each series of one dataframe(dataframe object)
#
# def change1(df):
#     df1 = df[[x for x in list(df) if df[x].dtype != 'object']]
#     a1 = np.array(df1)
#     v = np.vstack((a1[0], a1[1:] - a1[0:len(df) - 1]))
#     return pd.DataFrame(v, columns=list(df1))
# inc0 = inc.groupby(['公司代號', '年']).apply(change1).reset_index(drop=True);inc0
# list(inc0)

def change1(df):
    df0 = df[[x for x in list(df) if df[x].dtype == 'object']]
    df1 = df[[x for x in list(df) if df[x].dtype != 'object']]
    a0 = np.array(df0)
    a1 = np.array(df1)
    v = np.vstack((a1[0], a1[1:] - a1[0:len(df) - 1]))
    h = np.hstack((a0, v))
    return pd.DataFrame(h, columns=list(df0) + list(df1))
inc = inc.groupby(['公司代號', '年']).apply(change1).sort_values(['年', '季', '公司代號']).reset_index(drop=True);inc  # apply applies function to each datafrme(group) of one dataframe(groupby object)

table='ifrs前後-綜合損益表(季)'
# df = pd.read_sql_query("SELECT * from `{}`".format(table), conn_lite)
df=inc
df['年'], df['季'], df['公司代號']= df['年'].astype(int), df['季'].astype(str), df['公司代號'].astype(str)
df=df.drop_duplicates(subset=['年','季', '公司代號']).sort_values(['年','季', '公司代號'])
sql='ALTER TABLE `{}` RENAME TO `{}0`'.format(table, table)
cur_lite.execute(sql)
sql='create table `{}` (`{}`, PRIMARY KEY ({}))'.format(table, '`,`'.join(list(df)), '`年`, `季`, `公司代號`')
cur_lite.execute(sql)
sql='insert into `{}`(`{}`) values({})'.format(table, '`,`'.join(list(df)), ','.join('?'*len(list(df))))
cur_lite.executemany(sql, df.values.tolist())
conn_lite.commit()
sql="drop table `{}0`".format(table)
cur_lite.execute(sql)
print('finish')
