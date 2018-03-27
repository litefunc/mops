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

conn_lite = conn_local_lite('mops.sqlite3')
cur_lite = conn_lite.cursor()

def mymerge(x, y):
    m = pd.merge(x, y, how='outer')
    return m

# ----get unique id----

url = 'http://www.twse.com.tw/ch/trading/exchange/BWIBBU/BWIBBU_d.php'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
payload = {'input_date': '105/02/15', 'select2': 'ALL', 'sorting': 'STKNO'}
source_code = requests.post(url, headers=headers, data=payload)
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

##---- ifrs ----

#----test connection----
CO_ID='2033'
#2015
YEAR='2015'
df1=pd.DataFrame()
SEASON='2'
# url = "http://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID="+CO_ID+'&SYEAR='+YEAR+'&SSEASON='+SEASON+'&REPORT_ID=C'
# source_code = requests.get(url)
url='http://mops.twse.com.tw/server-java/t164sb01'
headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
payload = {'step':'1','CO_ID':CO_ID,'SYEAR':YEAR,'SSEASON':SEASON,'REPORT_ID':'A'}
source_code = requests.post(url,headers=headers,data=payload) #should use data instead of params
source_code.encoding = 'big5'
plain_text = source_code.text
print(plain_text)

# ----create table----
url = 'http://mops.twse.com.tw/server-java/t164sb01'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
payload = {'step': '1', 'CO_ID': '5522', 'SYEAR': '2015', 'SSEASON': '3', 'REPORT_ID': 'C'}
source_code = requests.post(url, headers=headers, data=payload)  # should use data instead of params
source_code.encoding = 'big5'
plain_text = source_code.text
soup = BeautifulSoup(plain_text, 'html.parser')
h = []
for th in soup.find_all('table')[stat].find_all('tr')[0].find_all('th'):
    h.append(th.string)
td = soup.find_all('table')[stat].find_all('tr')[4].find_all('td')
date = soup.find_all('span')[0].text
print(date)
ymd = re.findall(r"\d\d?\d?", date.split()[1])
ymd
r = [str(int(ymd[0]) + 1911) + '/0' + ymd[1]]
h = ['證券代號', '年季', td[0].text]
l = [h]
l.append([date.split()[0], str(int(ymd[0]) + 1911) + '/0' + ymd[1], td[1].text])
df = pd.DataFrame(l)
df.columns = df.ix[0, :]
df = df.ix[1:len(df), :]
names = list(df)
cur_lite = conn_lite.cursor()
sql = "create table `" + '會計師查核報告' + "`(" + "'" + names[0] + "'"
for n in names[1:len(names)]:
    sql = sql + ',' + "'" + n + "'"
sql = sql + ',PRIMARY KEY (`年季`,`證券代號`))'

sql = "SELECT * FROM `REPORT_ID_C`(`年`,`季`,`證券代號`,PRIMARY KEY (`年`,`季`,`證券代號`)) "
cur_lite.execute(sql)
cur_lite.fetchone()

# ----inserting data----
it=[]
y=['2016']
s=['1', '2']
for YEAR in y:
    for SEASON in s:
        for CO_ID in id:
            it.append([YEAR, SEASON, CO_ID])

tablename='會計師查核報告'
stat=6
it_e=[]
for i in it:
    for report in ['A']:
        try:
            time.sleep(3)
            print(i[0], i[1], i[2])
            url = 'http://mops.twse.com.tw/server-java/t164sb01'
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
            payload = {'step': '1', 'CO_ID': i[2], 'SYEAR': i[0], 'SSEASON': i[1], 'REPORT_ID': report}
            source_code = requests.post(url, headers=headers, data=payload)  # should use data instead of params
            source_code.encoding = 'big5'
            plain_text = source_code.text
            soup = BeautifulSoup(plain_text, 'html.parser')
            h=[]
            for th in soup.find_all('table')[stat].find_all('tr')[0].find_all('th'):
                h.append(th.string)
            td=soup.find_all('table')[stat].find_all('tr')[4].find_all('td')
            date = soup.find_all('span')[0].text
            print(date)
            ymd = re.findall(r"\d\d?\d?", date.split()[1])
            ymd
            r = [str(int(ymd[0]) + 1911) + '/0' + ymd[1]]
            h = ['證券代號', '年季', td[0].text]
            l = [h]
            l.append([date.split()[0],str(int(ymd[0]) + 1911) + '/0' + ymd[1],td[1].text])
            df = pd.DataFrame(l)
            df.columns = df.ix[0, :]
            df=df.ix[1:len(df), :]
            if h == ['證券代號', '年季', '\u3000 核閱或查核日期']:
                cur_lite.executemany('INSERT INTO `' + tablename + '` VALUES (?,?,?)', df.values.tolist())
                conn_lite.commit()
                if report == 'A':
                    cur_lite.execute('INSERT INTO `REPORT_ID_A` VALUES (?,?,?)', i)
        except Exception as e:
            print(e)
            it_e.append(i)
            # print('wait 6 seconds')
            # time.sleep(3)
            pass
it1=it_e.copy()
len(it)
len(it1)

df = read_sql_query("SELECT * from `會計師查核報告`", conn_lite)
df['年']=df['年季'].str.split('/').str[0]
df['季']=df['年季'].str.split('/').str[1].replace('0', '', regex=True)
it1=[item for item in it if item not in df[['年','季','證券代號']].values.tolist()]
print(it1)

##---- before ifrs ----
# ----inserting data----
it = []
y = ['92', '93', '94', '95', '96', '97', '98', '99', '100', '101']
s = ['01', '02', '03', '04']
for YEAR in y:
    for SEASON in s:
        for CO_ID in id:
            it.append([YEAR, SEASON, CO_ID])

df = read_sql_query("SELECT * from `會計師查核報告`", conn_lite)
df['年'] = df['年季'].str.split('/').str[0]
df['年'] = (df['年'].astype(int) - 1911).astype(str)
df['季'] = df['年季'].str.split('/').str[1]
it1 = [item for item in it if item not in df[['年', '季', '證券代號']].values.tolist()]

tablename = '會計師查核報告'
it_e = []
n = 0
for i in it1:
    try:
        time.sleep(2)
        n += 1
        url = 'http://mops.twse.com.tw/mops/web/ajax_t05st37'  # 個別,非合併
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
        payload = {'co_id': i[2], 'year': i[0], 'season': i[1], 'encodeURIComponent': '1', 'step': '1', 'firstin': '1',
                   'off': '1', 'queryName': 'co_id', 'TYPEK': 'all', 'isnew': 'false'}
        source_code = requests.post(url, headers=headers, data=payload)  # should use data instead of params
        print(n, i[0], i[1], i[2], source_code.status_code)
        source_code.encoding = 'utf-8'
        plain_text = source_code.text
        soup = BeautifulSoup(plain_text, 'html.parser')
        ym = []
        for th in soup.find_all('table')[1].find_all('tr')[0].find_all('td'):
            ym.append(th.string)
        th = soup.find_all('table')[1].find_all('tr')[2].find_all('th')
        td = soup.find_all('table')[1].find_all('tr')[2].find_all('td')
        date = soup.find_all('table')[1].find_all('tr')[0].text
        print(date, i[2])
        h = ['證券代號', '年季', th[0].text]
        l = [h]
        l.append([i[2], str(int(ym[0]) + 1911) + '/' + ym[1], td[0].text])
        df = pd.DataFrame(l)
        df.columns = df.ix[0, :]
        df = df.ix[1:len(df), :]
        if h == ['證券代號', '年季', '查核日期']:
            cur_lite.executemany('INSERT INTO `' + tablename + '` VALUES (?,?,?)', df.values.tolist())
            conn_lite.commit()
        source_code.raise_for_status()
    except Exception as e:
        print(e)
        it_e.append(i)
        # print('wait 6 seconds')
        # time.sleep(3)
        pass
it1 = it_e.copy()
len(it)
len(it1)

#------
df = read_sql_query("SELECT * from `會計師查核報告`", conn_lite)
df=df.sort_values(['年', '季', '證券代號'])
df['\u3000 核閱或查核日期']= df['\u3000 核閱或查核日期'].replace('-', '/', regex=True)
# df['年']=df['\u3000 核閱或查核日期'].str.split('/').str[0]
# df['月']=df['\u3000 核閱或查核日期'].str.split('/').str[1]
# df['日']=df['\u3000 核閱或查核日期'].str.split('/').str[2]
# for y in range(91,103):
#     df['年']=df['年'].replace(str(y), str(y+1911), regex=True)
# df['年']=df['年'].replace('\xa0','', regex=True)
# df['\u3000 核閱或查核日期']=df['年']+'/'+df['月']+'/'+df['日']
# df=df[['證券代號', '年季', '\u3000 核閱或查核日期']]
df1=df[df['證券代號']=='5522']
df1=df1.rename(columns={'\u3000 核閱或查核日期': '年月日'})
com="'5522%'"
sql="SELECT * FROM `%s` WHERE 公司代號 LIKE %s" % ('ifrs前後-綜合損益表', com)
inc = read_sql_query(sql, conn_lite)
# inc['年']=[x.split('/')[0] for x in inc['年季']]
# inc['季']=[x.split('/')[1] for x in inc['年季']]
# inc['公司代號']=inc['公司代號'].astype(str).replace('\.0', '', regex=True)
col=['年季', '年', '季', '公司代號', '公司名稱', '營業收入', '營業成本', '營業毛利（毛損）', '未實現銷貨（損）益', '已實現銷貨（損）益', '營業毛利（毛損）淨額', '營業費用', '其他收益及費損淨額', '營業利益（損失）', '營業外收入及支出', '稅前淨利（淨損）', '所得稅費用（利益）', '繼續營業單位本期淨利（淨損）', '停業單位損益', '合併前非屬共同控制股權損益', '本期淨利（淨損）', '其他綜合損益（淨額）', '合併前非屬共同控制股權綜合損益淨額', '本期綜合損益總額', '淨利（淨損）歸屬於母公司業主', '淨利（淨損）歸屬於共同控制下前手權益', '淨利（淨損）歸屬於非控制權益', '綜合損益總額歸屬於母公司業主', '綜合損益總額歸屬於共同控制下前手權益', '綜合損益總額歸屬於非控制權益', '基本每股盈餘（元）', '利息淨收益', '利息以外淨收益', '呆帳費用及保證責任準備提存（各項提存）', '淨收益', '保險負債準備淨變動', '支出及費用', '收入', '支出', '會計原則變動累積影響數', '呆帳費用', '會計原則變動之累積影響數', '稀釋每股盈餘', '利息收入', '減：利息費用', '收回(提存)各項保險責任準備淨額', '費用', '列計非常損益及會計原則變動累積影響數前損益', '營業支出']
col1=['營業收入', '營業成本', '營業毛利（毛損）', '未實現銷貨（損）益', '已實現銷貨（損）益', '營業毛利（毛損）淨額', '營業費用', '其他收益及費損淨額', '營業利益（損失）', '營業外收入及支出', '稅前淨利（淨損）', '所得稅費用（利益）', '繼續營業單位本期淨利（淨損）', '停業單位損益', '合併前非屬共同控制股權損益', '本期淨利（淨損）', '其他綜合損益（淨額）', '合併前非屬共同控制股權綜合損益淨額', '本期綜合損益總額', '淨利（淨損）歸屬於母公司業主', '淨利（淨損）歸屬於共同控制下前手權益', '淨利（淨損）歸屬於非控制權益', '綜合損益總額歸屬於母公司業主', '綜合損益總額歸屬於共同控制下前手權益', '綜合損益總額歸屬於非控制權益', '基本每股盈餘（元）', '利息淨收益', '利息以外淨收益', '呆帳費用及保證責任準備提存（各項提存）', '淨收益', '保險負債準備淨變動', '支出及費用', '收入', '支出', '會計原則變動累積影響數', '呆帳費用', '會計原則變動之累積影響數', '稀釋每股盈餘', '利息收入', '減：利息費用', '收回(提存)各項保險責任準備淨額', '費用', '列計非常損益及會計原則變動累積影響數前損益', '營業支出']
inc=inc[col]
inc=inc.replace('--','NaN')
# def change(df):
#     a = np.array(df)
#     return pd.DataFrame(np.vstack((a[0], a[1:] - a[0:len(df) - 1])), columns=list(df))
for i in col1:
    if inc[i].dtypes is dtype('O'):
        inc[[i]] = inc[[i]].astype(float)
def change1(df):
    df0 = df[[x for x in list(df) if df[x].dtype == 'O']]
    df1 = df[[x for x in list(df) if df[x].dtype != 'O']]
    a0 = np.array(df0)
    a1 = np.array(df1)
    v=np.vstack((a1[0], a1[1:] - a1[0:len(df) - 1]))
    h=np.hstack((a0, v))
    return pd.DataFrame(h, columns=list(df0)+list(df1))
inc=inc.groupby(['公司代號', '年']).apply(change1).reset_index(drop=True)
inc[col1]=inc[col1].rolling(window=4).sum()

sql="SELECT * FROM `%s` WHERE 證券代號 LIKE %s" % ('個股日本益比、殖利率及股價淨值比', com)
value = read_sql_query(sql, conn_lite)
sql="SELECT * FROM `%s` WHERE 證券代號 LIKE %s" % ('當日融券賣出與借券賣出成交量值(元)', com)
margin = read_sql_query(sql, conn_lite)
sql="SELECT * FROM `%s` WHERE 證券代號 LIKE %s" % ('三大法人買賣超日報(股)', com)
ins = read_sql_query(sql, conn_lite)
sql="SELECT * FROM `%s` WHERE 證券代號 LIKE %s" % ('自營商買賣超彙總表 (股)', com)
deal = read_sql_query(sql, conn_lite)
sql="SELECT * FROM `%s` WHERE 證券代號 LIKE %s" % ('外資及陸資買賣超彙總表 (股)', com)
fore = read_sql_query(sql, conn_lite)
sql="SELECT * FROM `%s` WHERE 證券代號 LIKE %s" % ('投信買賣超彙總表 (股)', com)
trust = read_sql_query(sql, conn_lite)
list(fore)
value['證券代號'].tolist()
deal['證券代號'].tolist()
fore['證券代號'].tolist()
trust['證券代號'].tolist()
value['證券名稱'].tolist()
deal['證券名稱'].tolist()
fore['證券名稱'].tolist()
trust['證券名稱'].tolist()
fore=fore.rename(columns={'買進股數':'外資買進股數','賣出股數':'外資賣出股數','買賣超股數':'外資買賣超股數','鉅額交易': '外資鉅額交易'})
trust=trust.rename(columns={'買進股數':'投信買進股數','賣出股數':'投信賣出股數','買賣超股數':'投信買賣超股數','鉅額交易': '投信鉅額交易'})
merge(fore, trust, on=['證券代號', '年月日', '證券名稱'], how='outer')
deal['證券代號']=deal['證券代號'].str.strip()
fore['證券代號']=fore['證券代號'].str.strip()
trust['證券代號']=trust['證券代號'].str.strip()
deal['證券名稱']=deal['證券名稱'].str.strip()
fore['證券名稱']=fore['證券名稱'].str.strip()
trust['證券名稱']=trust['證券名稱'].str.strip()

df1.head(5)
inc.head(5)
value.head(5)
m.head(5)
inc =inc.rename(columns={'公司代號': '證券代號'})
inc=inc[['年季', '證券代號', '公司名稱','營業收入', '營業成本', '營業毛利（毛損）', '未實現銷貨（損）益', '已實現銷貨（損）益', '營業毛利（毛損）淨額', '營業費用', '其他收益及費損淨額', '營業利益（損失）', '營業外收入及支出', '稅前淨利（淨損）', '所得稅費用（利益）', '繼續營業單位本期淨利（淨損）', '停業單位損益', '合併前非屬共同控制股權損益', '本期淨利（淨損）', '其他綜合損益（淨額）', '合併前非屬共同控制股權綜合損益淨額', '本期綜合損益總額', '淨利（淨損）歸屬於母公司業主', '淨利（淨損）歸屬於共同控制下前手權益', '淨利（淨損）歸屬於非控制權益', '綜合損益總額歸屬於母公司業主', '綜合損益總額歸屬於共同控制下前手權益', '綜合損益總額歸屬於非控制權益', '基本每股盈餘（元）', '利息淨收益', '利息以外淨收益', '呆帳費用及保證責任準備提存（各項提存）', '淨收益', '保險負債準備淨變動', '支出及費用', '收入', '支出', '會計原則變動累積影響數', '呆帳費用', '會計原則變動之累積影響數', '稀釋每股盈餘', '利息收入', '減：利息費用', '收回(提存)各項保險責任準備淨額', '費用', '列計非常損益及會計原則變動累積影響數前損益', '營業支出']]
inc['證券代號']=inc['證券代號'].astype(str)
inc['證券代號']=inc['證券代號'].replace('.0','', regex=True)
list(inc)
list(m)
m=merge(df1,inc,on=['證券代號','年季'],how='outer')
m=merge(value,m, on=['證券代號','年月日'], how='outer')
m=merge(deal,m, on=['證券代號','年月日','證券名稱'], how='outer')
m=merge(fore,m, on=['證券代號','年月日','證券名稱'], how='outer')
m=merge(trust,m, on=['證券代號','年月日','證券名稱'], how='outer')
m.年月日=to_datetime(m.年月日, format='%Y/%m/%d') # should convert to datetime before sort, or the result is  wrong
m=m.sort_values(['年月日','證券代號']).reset_index(drop=True) # reset_index make the index ascending
m1=m.copy()
m[~isnull(m['公司名稱'])]
m[~isnull(m['公司名稱'])].index
r=m[~isnull(m['公司名稱'])].index

def fill(s):
    a = np.array(0)
    r = s[~isnull(s)].index
    a = append(a, r)
    a = append(a, len(s))
    le=a[1:]-a[:len(a)-1]
    l=[]
    for i in range(len(le)):
        l=l+repeat(s[a[i]], le[i]).tolist()
    return Series(l,name=s.name)
fill(m['營業收入'])
m[['營業收入']].apply(fill)
# for col in col1:
#     m[[col]] = m[[col]].apply(fill)  # can't use m[col] = m[col].apply(fill)
# m[col1]
m[col1]=m[col1].apply(fill)
list(inc)
m['毛利率']=m['營業毛利（毛損）淨額']/m['營業收入']
m['營業利益率']=m['營業利益（損失）']/m['營業收入']
m['營業利益率']=m['營業利益（損失）']/m['營業收入']
m['稅後純益率']=m['淨利（淨損）歸屬於母公司業主']/m['營業收入']
m['綜合稅後純益率']=m['綜合損益總額歸屬於母公司業主']/m['營業收入']

m[col1].apply(sum)
m.營業收入.apply(sum)
m[['營業收入']].apply(fill)

print(list(m))
df1['證券代號'].tolist()
inc['證券代號'].tolist()
value['證券代號'].tolist()
df1['年月日'].tolist()
m['年月日'].head(200)
m.tail(5)
list(m)
print(list(m))
m=m[['證券代號', '年季', '年月日', '\u3000 核閱或查核日期', '公司名稱', '營業收入', '營業成本', '營業毛利（毛損）', '未實現銷貨（損）益', '已實現銷貨（損）益', '營業毛利（毛損）淨額', '營業費用', '其他收益及費損淨額', '營業利益（損失）', '營業外收入及支出', '稅前淨利（淨損）', '所得稅費用（利益）', '繼續營業單位本期淨利（淨損）', '停業單位損益', '合併前非屬共同控制股權損益', '本期淨利（淨損）', '其他綜合損益（淨額）', '合併前非屬共同控制股權綜合損益淨額', '本期綜合損益總額', '淨利（淨損）歸屬於母公司業主', '淨利（淨損）歸屬於共同控制下前手權益', '淨利（淨損）歸屬於非控制權益', '綜合損益總額歸屬於母公司業主', '綜合損益總額歸屬於共同控制下前手權益', '綜合損益總額歸屬於非控制權益', '基本每股盈餘（元）', '利息淨收益', '利息以外淨收益', '呆帳費用及保證責任準備提存（各項提存）', '淨收益', '保險負債準備淨變動', '支出及費用', '收入', '支出', '會計原則變動累積影響數', '呆帳費用', '會計原則變動之累積影響數', '稀釋每股盈餘', '利息收入', '減：利息費用', '收回(提存)各項保險責任準備淨額', '費用', '列計非常損益及會計原則變動累積影響數前損益', '營業支出', '換算匯率', '換算匯率參考依據', '證券名稱', '本益比', '殖利率(%)', '股價淨值比']]
for j in list(m)[4:]:
    for i in range(1,len(m)):
        if isnull(m[j][i]):
            m[j][i] = m[j][i-1]
            print(i, m['年季'][i], j, m[j][i])

# rename table
report='會計師查核報告'
sql='ALTER TABLE `' + report +'` RENAME TO`' + report +'0`'
cur_lite.execute(sql)

df = read_sql_query('SELECT * from `'+report +'0`', conn_lite)
df['年'] = [x.split('/')[0] for x in df['年季']]
df['季'] = [x.split('/')[1] for x in df['年季']]
df=df.drop(['年季'], axis=1)
df=df[['年', '季']+[col for col in df.columns if col not in ['年', '季']]]
df['證券代號'] = df['證券代號'].astype(str).replace('\.0', '', regex=True)
df['證券代號'] = df['證券代號'].str.strip()
df = df.drop_duplicates(['年', '季', '證券代號'])
# ----create table----
names = list(df)
cur_lite = conn_lite.cursor()
sql = "create table `" + report + "`(" + "'" + names[0] + "'"
for n in names[1:len(names)]:
    sql = sql + ',' + "'" + n + "'"
sql = sql + ', PRIMARY KEY (`年`, `季`, `證券代號`))'
cur_lite.execute(sql)
# ----inserting data----
sql = 'INSERT INTO `' + report + '` VALUES (?'
n = [',?'] * (len(names) - 1)
for h in n:
    sql = sql + h
sql = sql + ')'
cur_lite.executemany(sql, df.values.tolist())
conn_lite.commit()
print('done')

cur_lite = conn_lite.cursor()
sql = "drop table `" + report + "0`"
cur_lite.execute(sql)

#---- update ----
year = ['105']
season = ['02']
for y in year:
    for s in season:
        print(y, s)
        url = 'http://mops.twse.com.tw/mops/web/ajax_t163sb14'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'}
        payload = {'step': '1', 'encodeURIComponent': '1', 'firstin': '1', 'off': '1', 'TYPEK': 'sii', 'year': y, 'season': s}
        source_code = requests.post(url, headers=headers, data=payload)  # should use data instead of params
        source_code.encoding = 'utf8'
        plain_text = source_code.text
        soup = BeautifulSoup(plain_text, 'html.parser')
        print(soup)
        h = []
        for th in soup.find_all('tbody')[0].find_all('tr')[0].find_all('th'):
            h.append(th.string)
        tr=soup.find_all('table')[0].find_all('tr')
        td = soup.find_all('tbody')[0].find_all('tr')[1].find_all('td')
        date = soup.find_all('span')[0].text
        print(date)
        ymd = re.findall(r"\d\d?\d?", date.split()[1])
        ymd
        r = [str(int(ymd[0]) + 1911) + '/0' + ymd[1]]
        h = ['證券代號', '年季', td[0].text]
        l = [h]
        l.append([date.split()[0], str(int(ymd[0]) + 1911) + '/0' + ymd[1], td[1].text])
        df = pd.DataFrame(l)
        df.columns = df.ix[0, :]
        df = df.ix[1:len(df), :]
        if h == ['證券代號', '年季', '\u3000 核閱或查核日期']:
            cur_lite.executemany('INSERT INTO `' + tablename + '` VALUES (?,?,?)', df.values.tolist())
            conn_lite.commit()
            if report == 'A':
                cur_lite.execute('INSERT INTO `REPORT_ID_A` VALUES (?,?,?)', i)
    except Exception as e:
    print(e)
    it_e.append(i)
    pass