from sqlite3 import *
import os
from numpy import *
from pandas import *
from functools import *

def mymerge(x, y):
    m = merge(x, y, on=[col for col in list(x) if col in list(y)], how='outer')
    return m

import os
import re
os.getcwd()
dir()
os.listdir()

l1, l2, l3, l4, l5, l6, l7 = [], [], [], [], [], [], []
d={'2308':'一般業', '2801':'銀行業', '2880':'金控業', '2855':'證券業', '2832':'保險業', '1718':'其他業'}
dl={'2308':l1, '2801':l2, '2880':l3, '2855':l4, '2832':l5, '1718':l6}
path = 'C:/Users/ak66h_000/OneDrive/webscrap/公開資訊觀測站/彙總報表/ifrs前/合併損益表/'
os.chdir(path)
l = os.listdir()
for folder in l[:-1]:
    print(folder)
    os.chdir(path+folder)
    ll = os.listdir()
    for i in ll:
        try:
            df = read_csv(i, encoding='cp950', index_col=False)
            df.公司代號=df.公司代號.astype(str)
            if len(folder)==4:
                df.insert(0, '年', int(folder[:2])+1911)
                df.insert(1, '季', int(folder[-1]))
            else:
                df.insert(0, '年', int(folder[:3])+1911)
                df.insert(1, '季', int(folder[-1]))
            idl=[]
            for i in dl:
                if i in df.公司代號.tolist():
                    idl.append(i)
                    dl[i].append(df)
                    print(d[i])
            if idl==[]:
                l7.append(df)
                print('未知業')
        except Exception as e:
            print(e)
            print(i)
            pass

l1, l2, l3, l4, l5, l6, l7=reduce(mymerge, l1), reduce(mymerge, l2), reduce(mymerge, l3), reduce(mymerge, l4), reduce(mymerge, l5), reduce(mymerge, l6), reduce(mymerge, l7)

l1.dtypes

path = 'C:/Users/ak66h_000/Documents/db/'
os.chdir(path)

conn = connect('summary.sqlite3')
c = conn.cursor()
ind=['一般業', '銀行業', '金控業', '證券業', '保險業', '其他業', '未知業']
for i, df in enumerate([l1, l2, l3, l4, l5, l6, l7]):
    table='ifrs前-合併損益表-'+ind[i]
    df.年=df.年.astype(int)
    df.季=df.季.astype(int)
    df.公司代號=df.公司代號.astype(str)
    df=df.sort_values(['年', '季', '公司代號']).reset_index(drop=True)
    sql='create table `%s` (`%s`, PRIMARY KEY (%s))'%(table, '`,`'.join(list(df)), '`年`, `季`, `公司代號`')
    c.execute(sql)
    sql='insert into `%s`(`%s`) values(%s)'%(table, '`,`'.join(list(df)), ','.join('?'*len(list(df))))
    c.executemany(sql, df.values.tolist())
    conn.commit()
print('finish')


