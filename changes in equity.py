#----import----
from sqlite3 import *
conn = connect('C:\\Users\\ak66h_000\\Documents\\TEJ.sqlite3')
c = conn.cursor()

import requests
from bs4 import BeautifulSoup
from numpy import *
from pandas import *
from functools import *
def mymerge(x,y):
    m=merge(x,y,how='outer')
    return m

#----get unique id----
url = 'http://www.twse.com.tw/ch/trading/exchange/BWIBBU/BWIBBU_d.php'
payload = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36',
    'input_date':'105/02/15', 'select2': 'ALL', 'order': 'STKNO'}
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
df = DataFrame(l)
df.columns = df.ix[0, :]
df = df.ix[1:len(df), :]

id=df.ix[:, 1].unique().tolist()
for u in id:
    print(u)

#----test connection----
CO_ID='5522'
#2015
YEAR='2015'
df1=DataFrame()
SEASON='2'
# url = "http://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID="+CO_ID+'&SYEAR='+YEAR+'&SSEASON='+SEASON+'&REPORT_ID=C'
# source_code = requests.get(url)
url='http://mops.twse.com.tw/server-java/t164sb01'
headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
payload = {'step':'1','CO_ID':CO_ID,'SYEAR':YEAR,'SSEASON':SEASON,'REPORT_ID':'C'}
source_code = requests.post(url,headers=headers,data=payload) #should use data instead of params
source_code.encoding = 'big5'
plain_text = source_code.text
print(plain_text)

#----test getting data----
CO_ID='5522'
stat=4
L=[]
for YEAR in ['2015','2014','2013']:
    for SEASON in ['4','3','2','1']:
        try:
            url = "http://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID="+CO_ID+'&SYEAR='+YEAR+'&SSEASON='+SEASON+'&REPORT_ID=C'
            source_code = requests.get(url)
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
            df = DataFrame(l)
            df.columns=df.ix[0,:]
            df=df.ix[1:len(df),:]
            col = []
            for i, j in enumerate(list(df)):
                if j == '\u3000\u3000 繼續營業單位淨利（淨損）':
                    col.append(i)
            cols = list(df)
            cols[col[1]] = '\u3000\u3000 稀釋繼續營業單位淨利（淨損）'
            df.columns = cols
            L.append(df) # do not use L=L.append(df)!!
        except:
            pass
df1=reduce(mymerge,L)
print(df1)
df1=df1.rename(columns={'會計項目':'年季'})
df2=df1.drop_duplicates(subset='年季')
df2.index=arange(len(df2))
df2['證券代號']=CO_ID
df2=df2.sort(['年季', '證券代號'], ascending=[0,1])
cols = df2.columns.tolist()
cols = cols[-1:] + cols[:-1]
df2=df2[cols]
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
# sql='INSERT INTO `income` VALUES (?'
# n=[',?'] * (len(cols)-1)
# for h in n:
#     sql=sql+h
# sql=sql+')'
# c.executemany(sql, df3.values.tolist())
# conn.commit()

#---continue---
from os import *
getcwd()
chdir('C:/Users/ak66h_000/OneDrive/webscrap/cash')
getcwd()
listdir()
id2=[]
for i in listdir():
    id2.append(i)
for i in range(len(id2)):
    id2[i] = id2[i].replace('df', '')
    id2[i] = id2[i].replace('.csv', '')
id1=id
len(id1)
for i in id2:
    if i in id1:
        id1.remove(i)
len(id1)

#----main----
# tse好像會阻擋,而且資料會有少
stat=4
id_e=[]
for CO_ID in id:
    try:
        L = []
        for YEAR in ['2015','2014','2013']:
            print('Wait 4 seconds')
            time.sleep(4)
            for SEASON in ['4','3','2','1']:
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
                    df = DataFrame(l)
                    df.columns=df.ix[0,:]
                    df=df.ix[1:len(df),:]
                    # col = []
                    # for i, j in enumerate(list(df)):
                    #     if j == '\u3000\u3000 繼續營業單位淨利（淨損）':
                    #         col.append(i)
                    # if len(col)==2:
                    #     cols = list(df)
                    #     cols[col[1]] = '\u3000\u3000 稀釋繼續營業單位淨利（淨損）'
                    #     df.columns = cols
                    # col = []
                    # for i, j in enumerate(list(df)):
                    #     if j == '\u3000\u3000 停業單位淨利（淨損）':
                    #         col.append(i)
                    # if len(col)==2:
                    #     cols = list(df)
                    #     cols[col[1]] = '\u3000\u3000 稀釋停業單位淨利（淨損）'
                    #     df.columns = cols
                    dup=[idx for idx, item in enumerate(list(df)) if item in list(df)[:idx]]
                    if len(dup)>0:
                        cols = list(df)
                        for i in dup:
                            cols[i] = cols[i]+'[duplicate]'+str(i)
                        df.columns = cols
                    L.append(df) # do not use L=L.append(df)
                except Exception as e:
                    print(e)
                    pass
        df1=reduce(mymerge,L)
        df1=df1.rename(columns={'會計項目':'年季'})
        df2=df1.drop_duplicates(subset='年季')
        df2.index=arange(len(df2))
        df2['證券代號']=CO_ID
        df2=df2.sort(['年季', '證券代號'], ascending=[0,1])
        cols = df2.columns.tolist()
        cols = cols[-1:] + cols[:-1]
        df2=df2[cols]
        df2 = df2.replace(',', '', regex=True)
        print(df2)
        #df4=concat([df4, df3], ignore_index=True)
        #df4.to_csv('C:/Users/ak66h_000/OneDrive/webscrap/income/df4.csv',index=False)
        path='C:/Users/ak66h_000/OneDrive/webscrap/cash/df'+CO_ID+'.csv'
        df2.to_csv(path,index=False)
    except Exception as e:
        id_e.append(CO_ID)
        print(e)
        print('Wait 10 seconds...')
        time.sleep(10)
        print('Continue')
        pass
id_e1=id_e
print(id_e1)