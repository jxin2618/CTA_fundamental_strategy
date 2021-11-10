# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 13:42:25 2020

@author: DamonChan
"""

from pydbms import MysqlOrm, Py2Mysql
import pandas as pd

user = 'commodities'
password = 'Comm@QC777'
host = '192.168.7.92:3306'
db = 'commodities'

dbo = MysqlOrm(user, password, host, db, 'latin1', 'gb18030')

df = dbo.IndexConstituent.get_data('*', '')

df.object_id = [dbo._uuid(), dbo._uuid()]
df.constituent_weight = 1.5

'''
dbo.IndexConstituent.insert_or_update(df, ['date', 'sec_code'], dbo._update_time())


self = dbo.IndexConstituent

dim = ['date', 'sec_code']

up_add = dbo._update_time()
'''

self = dbo.SecuritiesInformation
df = pd.DataFrame(
        {'sec_code': ['A.CCRI'], 'short_name': ['tmp4'], 'sec_type': 'index'})
params = dict(record=df, dim=['sec_code'], date='20200701', left='valid_start', right='valid_end', quote=False, include='both')

self.check_slowly_change_record(**params)

record=df
dim=['sec_code']
date='20200731'
left='valid_start'
right='valid_end'
quote=False
include='both'

a, b = self._insert_or_update_slowly_change_record(record=record, dim=dim, date=date,
                left=left, right=right,
                quote=quote, include=include)

self.insert_or_update_slowly_change_df(
        df=record, dim=dim, date=date,
                left=left, right=right,
                quote=quote, include=include)


df = pd.DataFrame(
        {'date': ['20200701'], 'is_trading_date': [True], 'object_id': ['AAA']})
df = pd.DataFrame(
        {'date': ['20200701'], 'is_trading_date': [False], 'object_id': ['AAA']})

# EOF
