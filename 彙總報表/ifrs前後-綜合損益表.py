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
    m = pd.merge(x, y, on=[i for i in list(x) if i in list(y)], how='outer')
    return m

normal = sqlc.selectAll('綜合損益表-一般業', conn_lite)
hold = sqlc.selectAll('綜合損益表-金控業', conn_lite)
bank = sqlc.selectAll('綜合損益表-銀行業', conn_lite)
security = sqlc.selectAll('綜合損益表-證券業', conn_lite)
insurance = sqlc.selectAll('綜合損益表-保險業', conn_lite)
other = sqlc.selectAll('綜合損益表-其他業', conn_lite)

normal_beforeIfrs = sqlc.selectAll('ifrs前-合併損益表-一般業', conn_lite).rename(columns={'營業毛利(毛損)': '營業毛利（毛損）', '營業淨利(淨損)': '營業利益（損失）', '繼續營業單位稅前淨利(淨損)': '稅前淨利（淨損）', '所得稅費用(利益)': '所得稅費用（利益）', '繼續營業單位淨利(淨損)': '繼續營業單位本期淨利（淨損）', '非常損益': '其他綜合損益（淨額）', '合併總損益': '本期綜合損益總額', '合併淨損益': '綜合損益總額歸屬於母公司業主', '共同控制下前手權益損益': '綜合損益總額歸屬於共同控制下前手權益', '少數股權損益': '綜合損益總額歸屬於非控制權益', '基本每股盈餘': '基本每股盈餘（元）'})
normal_beforeIfrs['營業外收入及支出'] = normal_beforeIfrs['營業外收入及利益'] - normal_beforeIfrs['營業外費用及損失']
del normal_beforeIfrs['營業外收入及利益'], normal_beforeIfrs['營業外費用及損失'], normal_beforeIfrs['會計原則變動累積影響數']
normal = mymerge(normal_beforeIfrs, normal).sort_values(['年', '季', '公司代號']).reset_index(drop=True)
sqlc.createTable('ifrs前後-綜合損益表-一般業', list(normal), ['年', '季', '公司代號'], conn_lite)
sqlc.insertData('ifrs前後-綜合損益表-一般業', normal, conn_lite)

hold_beforeIfrs = sqlc.selectAll('ifrs前-合併損益表-金控業', conn_lite).rename(columns={'呆帳費用': '呆帳費用及保證責任準備提存','收回(提存)各項保險責任準備淨額': '保險負債準備淨變動' , '繼續營業單位稅前合併淨利(淨損)': '繼續營業單位稅前損益', '所得稅(費用)利益': '所得稅（費用）利益', '繼續營業單位稅後合併淨利(淨損)': '繼續營業單位本期淨利（淨損）','停業單位損益(稅後)': '停業單位損益', '非常損益(稅後)': '本期其他綜合損益（稅後淨額）', '合併總損益': '本期綜合損益總額', '合併總損益歸屬予_母公司股東': '綜合損益總額歸屬於母公司業主', '合併總損益歸屬予_少數股權': '綜合損益總額歸屬於非控制權益', '基本每股盈餘': '基本每股盈餘（元）'})
hold_beforeIfrs['利息淨收益'] = hold_beforeIfrs['利息收入'] - hold_beforeIfrs['減：利息費用']
del hold_beforeIfrs['利息收入'], hold_beforeIfrs['減：利息費用'], hold_beforeIfrs['會計原則變動之累積影響數(稅後)']
hold = mymerge(hold, hold_beforeIfrs).sort_values(['年', '季', '公司代號']).reset_index(drop=True)
sqlc.createTable('ifrs前後-綜合損益表-金控業', list(hold), ['年', '季', '公司代號'], conn_lite)
sqlc.insertData('ifrs前後-綜合損益表-金控業', hold, conn_lite)

bank_beforeIfrs = sqlc.selectAll('ifrs前-合併損益表-銀行業', conn_lite).rename(columns={'呆帳費用': '呆帳費用及保證責任準備提存（各項提存）', '繼續營業單位稅前合併淨利(淨損)': '繼續營業單位稅前淨利（淨損）', '所得稅(費用)利益': '所得稅（費用）利益', '繼續營業單位稅後合併淨利(淨損)': '繼續營業單位本期稅後淨利（淨損）','停業單位損益(稅後)': '停業單位損益', '非常損益(稅後)': '其他綜合損益（稅後）', '合併總損益': '本期綜合損益總額（稅後）', '合併總損益歸屬予_母公司股東': '綜合損益總額歸屬於母公司業主', '合併總損益歸屬予_少數股權': '綜合損益總額歸屬於非控制權益', '基本每股盈餘': '基本每股盈餘（元）'})
bank = mymerge(bank, bank_beforeIfrs).drop(['會計原則變動之累積影響數(稅後)'], axis=1).sort_values(['年', '季', '公司代號'])
sqlc.createTable('ifrs前後-綜合損益表-銀行業', list(bank), ['年', '季', '公司代號'], conn_lite)
sqlc.insertData('ifrs前後-綜合損益表-銀行業', bank, conn_lite)

security_beforeIfrs = sqlc.selectAll('ifrs前-合併損益表-證券業', conn_lite).rename(columns={'收入': '收益', '費用': '支出及費用', '繼續營業單位稅前淨利(淨損)': '稅前淨利（淨損）', '所得稅費用(利益)': '所得稅利益（費用）', '繼續營業單位稅後淨利(淨損)': '繼續營業單位本期淨利（淨損）','停業單位損益(稅後)': '停業單位損益', '列計非常損益及會計原則變動累積影響數前損益': '本期淨利（淨損）', '非常損益': '本期其他綜合損益（稅後淨額）', '合併總損益': '本期綜合損益總額', '合併淨損益': '綜合損益總額歸屬於母公司業主', '少數股權損益': '綜合損益總額歸屬於非控制權益', '每股盈餘': '基本每股盈餘（元）'})
security = mymerge(security, security_beforeIfrs).drop(['會計原則變動之累積影響數'], axis=1).sort_values(['年', '季', '公司代號']).reset_index(drop=True)
sqlc.createTable('ifrs前後-綜合損益表-證券業', list(security), ['年', '季', '公司代號'], conn_lite)
sqlc.insertData('ifrs前後-綜合損益表-證券業', security, conn_lite)

insurance_beforeIfrs = sqlc.selectAll('ifrs前-合併損益表-保險業', conn_lite).rename(columns={'營業利益(損失)': '營業利益（損失）', '繼續營業單位稅前純益(純損)': '繼續營業單位稅前純益（純損）', '所得稅費用(利益)': '所得稅費用（利益）', '繼續營業單位稅後純益(純損)': '繼續營業單位本期純益（純損）', '非常損益': '其他綜合損益（稅後淨額）', '合併總損益': '本期綜合損益總額', '合併淨損益': '綜合損益總額歸屬於母公司業主', '少數股權損益': '綜合損益總額歸屬於非控制權益', '基本每股盈餘': '基本每股盈餘（元）'})
insurance_beforeIfrs['營業外收入及支出'] = insurance_beforeIfrs['營業外收入及利益'] - insurance_beforeIfrs['營業外費用及損失']
del insurance_beforeIfrs['營業外收入及利益'], insurance_beforeIfrs['營業外費用及損失'], insurance_beforeIfrs['營業毛利(毛損)'], insurance_beforeIfrs['會計原則變動之累積影響數']
insurance = mymerge(insurance, insurance_beforeIfrs).sort_values(['年', '季', '公司代號']).reset_index(drop=True)
sqlc.createTable('ifrs前後-綜合損益表-保險業', list(insurance), ['年', '季', '公司代號'], conn_lite)
sqlc.insertData('ifrs前後-綜合損益表-保險業', insurance, conn_lite)

other_beforeIfrs = sqlc.selectAll('ifrs前-合併損益表-其他業', conn_lite).rename(columns={'營業收入': '收入', '營業支出': '支出', '繼續營業單位稅前淨利(淨損)': '繼續營業單位稅前淨利（淨損）', '所得稅費用(利益)': '所得稅費用（利益）', '非常損益': '其他綜合損益', '合併總損益': '本期綜合損益總額', '合併淨損益': '綜合損益總額歸屬於母公司業主', '少數股權損益': '綜合損益總額歸屬於非控制權益', '基本每股盈餘': '基本每股盈餘（元）'})
del other_beforeIfrs['會計原則變動之累積影響數']
other = mymerge(other, other_beforeIfrs).sort_values(['年', '季', '公司代號']).reset_index(drop=True)
unknown_beforeIfrs = sqlc.selectAll('ifrs前-合併損益表-未知業', conn_lite).rename(columns={'繼續營業單位稅前淨利': '繼續營業單位稅前淨利（淨損）', '所得稅費用(利益)': '所得稅費用（利益）', '繼續營業單位稅後淨利': '繼續營業單位本期淨利（淨損）', '非常損益': '其他綜合損益', '合併總損益': '本期綜合損益總額', '合併淨損益': '綜合損益總額歸屬於母公司業主', '少數股權損益': '綜合損益總額歸屬於非控制權益', '每股稅後盈餘': '基本每股盈餘（元）'})
unknown_beforeIfrs['收入'] = unknown_beforeIfrs['營業收入'] + unknown_beforeIfrs['營業外收入及利益']
unknown_beforeIfrs['支出'] = unknown_beforeIfrs['營業成本'] + unknown_beforeIfrs['營業費用'] + unknown_beforeIfrs['營業外費用及損失']
del unknown_beforeIfrs['營業收入'], unknown_beforeIfrs['營業外收入及利益'], unknown_beforeIfrs['營業成本'], unknown_beforeIfrs['營業費用'], unknown_beforeIfrs['營業外費用及損失'], unknown_beforeIfrs['會計原則變動之累積影響數'], unknown_beforeIfrs['營業毛利'], unknown_beforeIfrs['營業利益'], unknown_beforeIfrs['換算匯率'], unknown_beforeIfrs['換算匯率參考依據']
other = mymerge(other, unknown_beforeIfrs).sort_values(['年', '季', '公司代號']).reset_index(drop=True)
sqlc.createTable('ifrs前後-綜合損益表-其他業', list(other), ['年', '季', '公司代號'], conn_lite)
sqlc.insertData('ifrs前後-綜合損益表-其他業', other, conn_lite)

list(security)
def columnsDiff(df1, df2):
    return [col for col in list(df2) if col not in list(df1)]
columnsDiff(security, security_beforeIfrs)