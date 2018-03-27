from sqlite3 import *
conn = connect('C:\\Users\\ak66h_000\\Documents\\TEJ.sqlite3')
c = conn.cursor()

import requests
from bs4 import BeautifulSoup
from numpy import *
from pandas import *
from functools import *
import threading
import re

def mymerge(x, y):
    m = merge(x, y, how='outer')
    return m

# ----資產負債表----
year = ['91', '92', '93', '94', '95', '96', '97', '98', '99', '100', '101']
season = ['01', '02', '03', '04']
error = []
for y in year:
    for s in season:
        try:
            print(y, s)
            url = 'http://mops.twse.com.tw/mops/web/ajax_t51sb07'
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
            payload = {'encodeURIComponent': '1', 'step': '1', 'firstin': '1', 'off': '1', 'TYPEK': 'sii', 'year': y,
                       'season': s}
            source_code = requests.post(url, headers=headers, data=payload)  # should use data instead of params
            source_code.encoding = 'utf8'
            plain_text = source_code.text
            soup = BeautifulSoup(plain_text, 'html.parser')
            y = re.findall('\d\d\d?', soup.find_all('center')[0].text.split()[0])[0]
            s = soup.find_all('center')[0].text.split()[1][1]
            s = s.replace('一', '01').replace('二', '02').replace('三', '03').replace('四', '04')
            date = soup.find_all('center')[0].text.split()[0] + soup.find_all('center')[0].text.split()[1]
            ntable = len(soup.find_all('table', 'hasBorder'))
            for table in range(ntable):
                try:
                    print(date, y, s, table)
                    h = ['年季']
                    for th in soup.find_all('table', 'hasBorder')[table].find_all('tr')[0].find_all('th'):
                        h.append(th.text)
                    l = [h]
                    for tr in soup.find_all('table', 'hasBorder')[table].find_all('tr')[1:]:
                        r = [y+'/'+s]
                        for td in tr.find_all('td'):
                            r.append(td.text)
                        l.append(r)
                    df = DataFrame(l)
                    df.columns = df.ix[0, :]
                    df = df.ix[1:len(df), :]
                    df= df.replace(',', '', regex=True)
                    df['公司代號'] = df['公司代號'].str.strip()
                    df = df.dropna(subset=['公司代號'])
                    # names = list(df)
                    # c = conn.cursor()
                    # sql = "create table `" + '資產負債表ifrs前' + "`(" + "'" + names[0] + "'"
                    # for n in names[1:len(names)]:
                    #     sql = sql + ',' + "'" + n + "'"
                    # sql = sql + ',PRIMARY KEY (`年`,`季`))'
                    df.to_csv('C:/Users/ak66h_000/OneDrive/webscrap/公開資訊觀測站/彙總報表/ifrs前/資產負債表/' + y + s + '_' + str(
                        table) + '.csv', index=False)
                except Exception as e:
                    print(e)
        except Exception as e:
            print(e)
            error.append([y, s])
            pass
error1=error.copy()
print(error1)

# ----合併資產負債表----
year = ['93', '94', '95', '96', '97', '98', '99', '100', '101']
season = ['01', '02', '03', '04']
error = []
for y in year:
    for s in season:
        try:
            print(y, s)
            url = 'http://mops.twse.com.tw/mops/web/ajax_t51sb12'
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
            payload = {'encodeURIComponent': '1', 'step': '1', 'firstin': '1', 'off': '1', 'TYPEK': 'sii', 'year': y,'season': s}
            source_code = requests.post(url, headers=headers, data=payload)  # should use data instead of params
            source_code.encoding = 'utf8'
            plain_text = source_code.text
            soup = BeautifulSoup(plain_text, 'html.parser')
            y = re.findall('\d\d\d?', soup.find_all('h2')[0].text.split()[0])[0]
            s = soup.find_all('h2')[0].text.split()[1][1]
            s = s.replace('一', '01').replace('二', '02').replace('三', '03').replace('四', '04')
            date = soup.find_all('h2')[0].text.split()[0] + soup.find_all('h2')[0].text.split()[1]
            ntable = len(soup.find_all('table', class_ =''))
            for table in range(ntable):
                try:
                    print(date, y, s, table)
                    h = ['年季']
                    for th in soup.find_all('table', class_ ='')[table].find_all('tr')[0].find_all('th'):
                        h.append(th.text)
                    l = [h]
                    for tr in soup.find_all('table', class_ ='')[table].find_all('tr')[1:]:
                        r = [y+'/'+s]
                        for td in tr.find_all('td'):
                            r.append(td.text)
                        l.append(r)
                    df = DataFrame(l)
                    df.columns = df.ix[0, :]
                    df = df.ix[1:len(df), :]
                    df = df.replace(',', '', regex=True)
                    df['公司代號'] = df['公司代號'].str.strip()
                    df = df.dropna(subset=['公司代號'])
                    # names = list(df)
                    # c = conn.cursor()
                    # sql = "create table `" + '資產負債表ifrs前' + "`(" + "'" + names[0] + "'"
                    # for n in names[1:len(names)]:
                    #     sql = sql + ',' + "'" + n + "'"
                    # sql = sql + ',PRIMARY KEY (`年`,`季`))'
                    df.to_csv('C:/Users/ak66h_000/OneDrive/webscrap/公開資訊觀測站/彙總報表/ifrs前/合併資產負債表/' + y + s + '_' + str(
                        table) + '.csv', index=False)
                except Exception as e:
                    print(e)
        except Exception as e:
            print(e)
            error.append([y, s])
            pass
error1=error.copy()
print(error1)

#--- read from csv ---
import os
path = 'C:/Users/ak66h_000/OneDrive/webscrap/公開資訊觀測站/彙總報表/ifrs前/合併資產負債表/'
os.chdir(path)
l = os.listdir()
l=[x for x in l if re.search('_0', x)]
L = []
for i in l:
    df = read_csv(i, encoding='cp950')
    df = df.rename(columns={'資產總計': '資產總額', '資產合計': '資產總額', '負債總計': '負債總額', '負債合計': '負債總額', '股東權益其他調整項目合計': '其他權益',
                              '股東權益其他項目': '其他權益', '少數股權': '非控制權益',
                              '每股淨值': '每股參考淨值', '預收股款(股東權益項下)之約當發行股數(單位:股)': '預收股款（權益項下）之約當發行股數（單位：股）',
                              '母公司暨子公司所持有之母公司庫藏股股數(單位:股)': '母公司暨子公司所持有之母公司庫藏股股數（單位：股）', '股東權益合計': '權益總額',
                              '股東權益總計': '權益總額'})
    L.append(df)
df1 = reduce(mymerge, L)
df1=df1.dropna(subset = ['年季'])
df1=df1.dropna(subset = ['公司代號'])
df1['公司代號']=df1['公司代號'].astype(str)
df1['公司代號']=df1['公司代號'].replace('\.0', '', regex=True)
df1['年']=df1['年季'].str.split('/').str[0].astype(int)
df1['年']=(df1['年']+1911).astype(str)
df1['季']=df1['年季'].str.split('/').str[1]
col = [x for x in list(df1) if x not in ['年季', '年', '季']]
df1 = df1[['年', '季']+col]
df1=df1.sort_values(['年', '季', '公司代號']).reset_index(drop=True)
print(df1)
list(df1)
df1.dtypes

bal1 = read_sql_query("SELECT * from `資產負債表-一般業`", conn)
bal1['公司代號']=bal1['公司代號'].astype(str)
bal1['公司代號']=bal1['公司代號'].replace('\.0', '', regex=True)
list(bal1)
bal=df1.copy()
list(bal)
bal['流動資產']=bal['流動資產']+bal['基金與投資']
bal['非流動資產']=bal['固定資產']+bal['無形資產']+bal['其他資產']
bal['非流動負債']=bal['長期負債']+bal['各項準備']+bal['其他負債']
# [x for x in list(bal) if x not in list(bal1)]
m=mymerge(bal, bal1)
m=m[list(bal1)].sort_values(['年', '季', '公司代號']).reset_index(drop=True)

report='ifrs前後-資產負債表-一般業'
names = list(m)
c = conn.cursor()
sql = "create table `" + report + "`(" + "'" + names[0] + "'"
for n in names[1:len(names)]:
    sql = sql + ',' + "'" + n + "'"
sql = sql + ',PRIMARY KEY (`年`,`季`,`公司代號`))'
c.execute(sql)
# ----inserting data----
sql = 'INSERT INTO `' + report +  '` VALUES (?'
n = [',?'] * (len(names) - 1)
for h in n:
    sql = sql + h
sql = sql + ')'
c.executemany(sql, m.values.tolist())
conn.commit()
print('done')

#--- read from sqlite ---
com = "'5522%'"
sql = "SELECT * FROM `%s` WHERE 公司代號 LIKE %s" % ('ifrs前後-資產負債表', com)
bal = read_sql_query(sql, conn)
sql = "SELECT * FROM `%s` WHERE 公司代號 LIKE %s" % ('財務分析', com)
fin = read_sql_query(sql, conn)
sql = "SELECT * FROM `%s` WHERE 公司代號 LIKE %s" % ('ifrs前後-綜合損益表', com)
inc = read_sql_query(sql, conn)
inc['公司代號'] = inc['公司代號'].astype(str).replace('\.0', '', regex=True)
m = merge(inc, bal, how='outer')
list(inc)
list(bal)
list(fin)
m['流動比率']=m['流動資產']/m['流動負債']
m['負債佔資產比率']=m['負債總額']/m['資產總額']
m['權益報酬率']=m['本期淨利（淨損）']*2/(m['權益總額']+m['權益總額'].shift())



# rename table
report='ifrs前後-資產負債表'
sql='ALTER TABLE `'+ report +'` RENAME TO`' + report  +'0`'
c.execute(sql)

df0 = read_sql_query('SELECT * from `'+report +'0`', conn)
industry=['銀行業','證券業','一般業','金控業','保險業','未知業']
# rename table
report='資產負債表'
# key=['年月日', '成交統計']
# key=['年', '季', '公司代號']
l=[]
for ind in industry:
    df = read_sql_query('SELECT * from `'+report +'-'+ ind +'`', conn)
    l.append(df)
df1 = reduce(mymerge, l)
df2 = mymerge(df0, df1).sort_values(['年', '季', '公司代號'])
# df['年'] = [x.split('/')[0] for x in df['年季']]
# df['季'] = [x.split('/')[1] for x in df['年季']]
# df=df.drop(['年季'], axis=1)
# df=df[['年', '季']+[col for col in df.columns if col not in ['年', '季']]]
# df['公司代號'] = df['公司代號'].astype(str).replace('\.0', '', regex=True)
# df['公司代號'], df['公司名稱'] = df['公司代號'].str.strip(), df['公司名稱'].str.strip()
df2 = df2.drop_duplicates(['年', '季', '公司代號'])
# ----create table----
names = list(df2)
c = conn.cursor()
report='ifrs前後-資產負債表'
sql = "create table `" + report  + "`(" + "'" + names[0] + "'"
for n in names[1:len(names)]:
    sql = sql + ',' + "'" + n + "'"
sql = sql + ', PRIMARY KEY (`年`, `季`, `公司代號`))'
c.execute(sql)
# ----inserting data----
sql = 'INSERT INTO `' + report  + '` VALUES (?'
n = [',?'] * (len(names) - 1)
for h in n:
    sql = sql + h
sql = sql + ')'
c.executemany(sql, df2.values.tolist())
conn.commit()
print('done')

c = conn.cursor()
sql = "drop table `" + report  + "0`"
c.execute(sql)


