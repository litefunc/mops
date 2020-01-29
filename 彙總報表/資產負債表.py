from common.connection import conn_local_lite
import sqlCommand as sqlc
import syspath
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import datetime as dt
import cytoolz.curried
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))


conn_lite = conn_local_lite('summary.sqlite3')
cur_lite = conn_lite.cursor()


def mymerge(x, y):
    m = pd.merge(x, y, on=[col for col in list(x)
                           if col in list(y)], how='outer')
    return m


def industry(ids):
    if '2801' in ids or '2834' in ids:
        return '銀行業'
    if '2855' in ids or '2856' in ids:
        return '證券業'
    if '1101' in ids or '2330' in ids:
        return '一般業'
    if '2881' in ids or '2882' in ids or '2884' in ids:
        return '金控業'
    if '2823' in ids or '2832' in ids:
        return '保險業'
    if '1409' in ids or '1718' in ids:
        return '其他業'
    return '其他業'


def get_season(m):
    if m == 1 or m == 2 or m == 3:
        return 1
    if m == 4 or m == 5 or m == 6:
        return 2
    if m == 7 or m == 8 or m == 9:
        return 3
    if m == 10 or m == 11 or m == 12:
        return 4
    return 0


def next_season(y, s):
    if s == 4:
        return (y+1, 1)
    return (y, s+1)


def year_seasons(y, s):
    today = dt.date.today()
    end = dt.date(today.year, get_season(today.month), 1)
    start = dt.date(y, s, 1)
    print('start:', start, 'end:', end)

    yss = []
    ys = (y, s)

    while True:
        curr = dt.date(ys[0], ys[1], 1)
        if curr >= end:
            return tuple(yss[1:])
        yss.append((f'{ys[0]-1911}', f'0{ys[1]}'))
        ys = next_season(ys[0], ys[1])


def latest(table):
    sql = f"""SELECT DISTINCT "年", "季" FROM "{table}" ORDER BY "年" desc, "季" desc LIMIT 1;"""
    return pd.read_sql_query(sql, conn_lite)


def get_yss(table):
    df = latest(table).astype(int)
    return year_seasons(df.年[0], df.季[0])


# #----test connection----


# YEAR = '106'
# SEASON = '03'
# url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb05'
# payload = {'encodeURIComponent': '1', 'step': '1', 'firstin': '1',
#            'off': '1', 'year': YEAR, 'season': SEASON, 'TYPEK': 'sii'}
# # should use data instead of params
# source_code = requests.post(url, data=payload)
# source_code.encoding = 'utf8'
# source_code.headers
# plain_text = source_code.text
# # don't use html.parser because it can't parse correctly
# soup = BeautifulSoup(plain_text, 'lxml')
# print(soup.prettify())

# ---- update data ----
yss = get_yss("資產負債表-一般業")
for ys in yss:
    YEAR = ys[0]
    SEASON = ys[1]
    url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb05'
    payload = {'encodeURIComponent': '1', 'step': '1', 'firstin': '1', 'off': '1', 'year': YEAR, 'season': SEASON,
               'TYPEK': 'sii'}
    # should use data instead of params
    source_code = requests.post(url, data=payload)
    source_code.encoding = 'utf8'
    source_code.headers
    plain_text = source_code.text
    soup = BeautifulSoup(plain_text, 'lxml')
    for t in soup.find_all('table')[1:]:
        try:
            trs = t.find_all('tr')
            ths = list(map(lambda x: x.text, trs[0].find_all('th')))
            tb = []
            tb.append(ths)
            for r in trs[1:]:
                tb.append(list(map(lambda x: x.text, r.find_all('td'))))
            df = pd.DataFrame(tb[1:], columns=tb[0]).replace(
                ',', '', regex=True).replace('--', np.nan)
            df.insert(0, '年', int(YEAR) + 1911)
            df.insert(1, '季', SEASON.replace('0', ''))
            ind = industry(list(df['公司代號']))
            print(ind)
            sqlc.insertData('資產負債表-{}'.format(ind), df, conn_lite)
        except Exception as e:
            print(YEAR, SEASON, ind, e)
            pass

# for YEAR in ['107']:
#     for SEASON in ['02']:
#         url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb05'
#         payload = {'encodeURIComponent': '1', 'step': '1', 'firstin': '1', 'off': '1', 'year': YEAR, 'season': SEASON,
#                    'TYPEK': 'sii'}
#         # should use data instead of params
#         source_code = requests.post(url, data=payload)
#         source_code.encoding = 'utf8'
#         source_code.headers
#         plain_text = source_code.text
#         soup = BeautifulSoup(plain_text, 'lxml')
#         for t in soup.find_all('table')[1:]:
#             try:
#                 trs = t.find_all('tr')
#                 ths = list(map(lambda x: x.text, trs[0].find_all('th')))
#                 tb = []
#                 tb.append(ths)
#                 for r in trs[1:]:
#                     tb.append(list(map(lambda x: x.text, r.find_all('td'))))
#                 df = pd.DataFrame(tb[1:], columns=tb[0]).replace(
#                     ',', '', regex=True).replace('--', np.nan)
#                 df.insert(0, '年', int(YEAR) + 1911)
#                 df.insert(1, '季', SEASON.replace('0', ''))
#                 ind = industry(list(df['公司代號']))
#                 print(ind)
#                 sqlc.insertData('資產負債表-{}'.format(ind), df, conn_lite)
#             except Exception as e:
#                 print(YEAR, SEASON, ind, e)
#                 pass


# ALTER TABLE "資產負債表-其他業" ADD COLUMN "資產總計";
# ALTER TABLE "資產負債表-一般業" ADD COLUMN "資產總計";
# ALTER TABLE "資產負債表-金控業" ADD COLUMN "透過其他綜合損益按公允價值衡量之金融資產";
# ALTER TABLE "資產負債表-保險業" ADD COLUMN "本期所得稅資產";
# ALTER TABLE "資產負債表-銀行業" ADD COLUMN "透過其他綜合損益按公允價值衡量之金融資產";
# ALTER TABLE "資產負債表-金控業" ADD COLUMN "透過其他綜合損益按公允價值衡量之金融資產";
# ALTER TABLE "資產負債表-保險業" ADD COLUMN "本期所得稅資產";
# ALTER TABLE "資產負債表-證券業" ADD COLUMN "資產總計";
# ALTER TABLE "資產負債表-一般業" ADD COLUMN "負債總計";
# ALTER TABLE "資產負債表-其他業" ADD COLUMN "負債總計";
# ALTER TABLE "資產負債表-其他業" ADD COLUMN "現金及約當現金";
# ALTER TABLE "資產負債表-金控業" ADD COLUMN "透過其他綜合損益按公允價值衡量之金融資產"
# ALTER TABLE "資產負債表-保險業" ADD COLUMN "本期所得稅資產"
# ALTER TABLE "資產負債表-其他業" ADD COLUMN "存放央行及拆借銀行同業"
# ALTER TABLE "資產負債表-一般業" ADD COLUMN "權益總計"
# ALTER TABLE "資產負債表-其他業" ADD COLUMN "透過其他綜合損益按公允價值衡量之金融資產"

cur_lite.execute("""
ALTER TABLE "資產負債表-一般業" ADD COLUMN "權益總計"
""")
cur_lite.execute("""
ALTER TABLE "資產負債表-其他業" ADD COLUMN "透過其他綜合損益按公允價值衡量之金融資產"
""")
cur_lite.execute("""
ALTER TABLE "資產負債表-保險業" ADD COLUMN "本期所得稅資產"
""")


conn_lite.commit()
