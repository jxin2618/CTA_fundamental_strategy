# -*- coding: utf-8 -*-
"""
Created on Tue Dec  8 14:40:15 2020

@author: J Xin
"""
#%% 连接数据库
from datapro import factorpro, datepro
import conf
import futuresapi
import pydbms
from pydbms import MysqlOrm, Py2Mysql
import pandas as pd
import numpy as np
import os
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

user = 'quantchina'
password = 'zMxq7VNYJljTFIQ8'
host = '192.168.7.93:3306'
db = 'my_steel'
dbo = MysqlOrm(user, password, host, db, 'latin1', 'gb18030')


#%% CU
os.chdir('D:/Xin/Program/metal/CU/data/raw_data')

#%% ID00188204 ID00188207 ID00259941 ID00302614

for ID in ['ID00188204', 'ID00188207', 'ID00259941', 'ID00302614']:
    df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}' and is_delete = 0".format(k=ID))  
    df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
    df = df.sort_values(by='data_date', ascending=True)
    df = df.dropna(subset=['data_value'], axis=0)
    df.drop_duplicates(subset='data_date', keep='last', inplace=True)
    df.loc[:, 'date'] = df.loc[:, 'publish_time']
    
    df.to_excel(ID + '.xlsx', index=False)

#%% ID00188318 & ID00188319 电解铜：现货库存
for ID in ['ID00188318', 'ID00188319']:
    df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}' and is_delete = 0".format(k=ID))  
    df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
    df = df.sort_values(by=['data_date'], ascending=True)
    df = df.dropna(subset=['data_value'], axis=0)
    df.drop_duplicates(subset='data_date', keep='last', inplace=True)
    df.loc[:, 'data_date'] = df.loc[:, 'data_date'].apply(lambda x: pd.Timestamp(x))
    df = df.fillna(method='ffill')
    df.loc[df.loc[:, 'data_date'] < pd.Timestamp('2020-09-21'), 'date'] = df.loc[df.loc[:, 'data_date'] < pd.Timestamp('2020-09-21'), 'data_date']
    df.loc[df.loc[:, 'data_date'] >= pd.Timestamp('2020-09-21'), 'date'] = df.loc[df.loc[:, 'data_date'] >= pd.Timestamp('2020-09-21'), 'publish_time']
    df.to_excel(ID + '.xlsx', index=False)

#%% ID00299226
ID = 'ID00299226'
os.chdir('D:/Xin/Program/metal/CU/data/raw_data')
df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}' and is_delete = 0".format(k=ID))  
df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
df = df.sort_values(by='data_date', ascending=True)
df.drop_duplicates(subset='data_date', keep='last', inplace=True)
df.loc[:, 'date'] = df.loc[:, 'publish_time']
df.to_excel(ID + '.xlsx', index=False)

#%% ID00408044
ID = 'ID00408044'
df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}' and is_delete = 0".format(k=ID))  
df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
df = df.sort_values(by=['data_date'], ascending=True)
df = df.dropna(subset=['data_value'], axis=0)
df.drop_duplicates(subset='data_date', keep='last', inplace=True)
df.loc[:, 'data_date'] = df.loc[:, 'data_date'].apply(lambda x: pd.Timestamp(x))
df.loc[df.loc[:, 'data_date'] < pd.Timestamp('2019-11-30'), 'date'] = df.loc[df.loc[:, 'data_date'] < pd.Timestamp('2019-11-30'), 'data_date'].apply(lambda x: x + relativedelta(days=30))
df.loc[df.loc[:, 'data_date'] >= pd.Timestamp('2019-11-30'), 'date'] = df.loc[df.loc[:, 'data_date'] >= pd.Timestamp('2019-11-30'), 'publish_time']
df.to_excel(ID + '.xlsx', index=False)

#%% ID01030223, ID01030227

for ID in ['ID01030223', 'ID01030227']:
    df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}' and is_delete = 0".format(k=ID))  
    df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
    df = df.sort_values(by=['data_date'], ascending=True)
    df.drop_duplicates(subset='data_date', keep='last', inplace=True)
    df.loc[:, 'data_date'] = df.loc[:, 'data_date'].apply(lambda x: pd.Timestamp(x))
    df = df.fillna(method='ffill')
    df.loc[df.loc[:, 'data_date'] <= pd.Timestamp('2020-09-04'), 'date'] = df.loc[df.loc[:, 'data_date'] <= pd.Timestamp('2020-09-04'), 'data_date'].apply(lambda x: x + relativedelta(days=3))
    df.loc[df.loc[:, 'data_date'] > pd.Timestamp('2020-09-04'), 'date'] = df.loc[df.loc[:, 'data_date'] > pd.Timestamp('2020-09-04'), 'publish_time']
    df.to_excel(ID + '.xlsx', index=False)

#%% ID01030232

ID = 'ID01030232'
df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}'".format(k=ID)) # 进口盈亏 

df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
df = df.sort_values(by='data_date', ascending=True) #原始数据按日期排序
df.drop_duplicates(subset='data_date', keep='last', inplace=True) #取每天的最新数据
df.loc[:, 'data_date'] = df.loc[:, 'data_date'].apply(lambda x: pd.Timestamp(x))

df_1 = df.loc[df.loc[:, 'data_date'] <= pd.Timestamp('2020-09-25'), :]
df_2 = df.loc[df.loc[:, 'data_date'] > pd.Timestamp('2020-09-25'), :]

begin_date = df_1.loc[:, 'data_date'].values[0] #原始数据的开始日
end_date = df_1.loc[:, 'data_date'].values[-1] #原始数据的结束日

futures_api = futuresapi.API()
calendar_1 = futures_api.futures_calendar(begin_date, pd.Timestamp(end_date) + relativedelta(days=7), columns=['date', 'is_trading_date', 'previous_trading_date', 'current_trading_date', 'yweek', 'week_day'] , is_trading_date=1)
calendar_0 = futures_api.futures_calendar(begin_date, pd.Timestamp(end_date) + relativedelta(days=7), columns=['date', 'is_trading_date', 'previous_trading_date', 'current_trading_date', 'yweek', 'week_day'] , is_trading_date=0)
calendar = pd.concat([calendar_1, calendar_0], axis=0, ignore_index=True)
calendar = calendar.sort_values(by='date', ascending=True)
calendar.loc[:, 'yweek'] = calendar.loc[:, 'yweek'].astype(int) # 全日历的周数

df_value_calendar = pd.merge(df_1, calendar, how='outer', left_on='data_date', right_on='date')
df_value_calendar.sort_values(by='date', ascending=True, inplace=True) # 数据标记日的周数，数据对齐全日历

roll_week = np.roll(df_value_calendar.loc[:, 'yweek'].unique(), -1)
roll_week[-1] = roll_week[-2] + 1
roll_week_df = pd.DataFrame(data={'yweek': df_value_calendar.loc[:, 'yweek'].unique(), 'simulated_yweek': roll_week}) #模拟数据实际发布日是标记日的下一个周

df_value_calendar = pd.merge(df_value_calendar, roll_week_df, on='yweek') # 模拟数据实际发布日是标记日的下一个周
df_value_calendar = df_value_calendar.loc[:, ['index_code', 'data_date', 'data_value', 'is_trading_date', 'simulated_yweek', 'date', 'publish_time', 'update_time']]

calendar_16_20 = pd.concat([futures_api.futures_calendar(pd.Timestamp('2016-01-01'), pd.Timestamp('2020-12-31'), columns=['date', 'yweek', 'week_day'] , is_trading_date=1), futures_api.futures_calendar(pd.Timestamp('2016-01-01'), pd.Timestamp('2020-12-31'), columns=['date', 'yweek', 'week_day'] , is_trading_date=0)], axis=0, ignore_index=True)
calendar_16_20.loc[:, 'yweek'] = calendar_16_20.loc[:, 'yweek'].astype(int) # 16年-20年的全日历
calendar_simulate = calendar_16_20.loc[(calendar_16_20.loc[:, 'yweek'] >= df_value_calendar.loc[:, 'simulated_yweek'].min()) & (calendar_16_20.loc[:, 'yweek'] <= df_value_calendar.loc[:, 'simulated_yweek'].max()), :].set_index(['date']).groupby('yweek').min().reset_index() # 获取模拟数据发布周的每周最早交易日

calendar_simulate = pd.merge(calendar_16_20, calendar_simulate, on=['yweek', 'week_day'], how='inner')
calendar_simulate.rename(columns={'date': 'simulate_publish_time'}, inplace=1)
df_simulate = pd.merge(df_value_calendar, calendar_simulate, left_on='simulated_yweek', right_on='yweek', how='outer') 
df_simulate = df_simulate.dropna(subset=['data_date'])

df_simulate = df_simulate.loc[:, ['index_code', 'data_date', 'data_value', 'simulate_publish_time', 'publish_time', 'update_time']]
df_simulate = df_simulate.rename(columns={'simulate_publish_time': 'date'})
d = df_simulate.pop('date')
df_simulate.loc[:, 'date'] = d

df_2.loc[:, 'date'] = df_2.loc[:, 'publish_time']
df_final = pd.concat([df_simulate, df_2], axis=0)

df_final.to_excel(ID + '.xlsx', index=False)

#%% AL
os.chdir('D:/Xin/Program/metal/AL/data/raw_data')

#%% CM0000464587

ID = 'CM0000464587'
df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}' and is_delete = 0".format(k=ID))  
df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
df = df.sort_values(by=['data_date'], ascending=True)
df.drop_duplicates(subset='data_date', keep='last', inplace=True)
df.loc[:, 'data_date'] = df.loc[:, 'data_date'].apply(lambda x: pd.Timestamp(x))
df.loc[df.loc[:, 'data_date'] <= pd.Timestamp('2018-01-31'), 'date'] = df.loc[df.loc[:, 'data_date'] <= pd.Timestamp('2018-01-31'), 'data_date'].apply(lambda x: x + relativedelta(days=30))
df.loc[df.loc[:, 'data_date'] > pd.Timestamp('2018-01-31'), 'date'] = df.loc[df.loc[:, 'data_date'] > pd.Timestamp('2018-01-31'), 'publish_time']
df.to_excel(ID + '.xlsx', index=False)

#%% ID00188138 ID00188160 ID00188307 ID00188313 ID00302661 ID00408863

for ID in ['ID00188138', 'ID00188160', 'ID00188307', 'ID00188313', 'ID00302661', 'ID00408863']:
    df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}' and is_delete = 0".format(k=ID))  
    df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
    df = df.dropna(subset=['data_value'], axis=0)
    df = df.sort_values(by='data_date', ascending=True)
    df = df.dropna(subset=['data_value'], axis=0)
    df.drop_duplicates(subset='data_date', keep='last', inplace=True)
    df.loc[:, 'date'] = df.loc[:, 'publish_time']
    df.to_excel(ID + '.xlsx', index=False)

#%% ID00275565 ID00275569
for ID in ['ID00275565', 'ID00275569']:
    df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}' and is_delete = 0".format(k=ID))  
    df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
    df = df.sort_values(by=['data_date'], ascending=True)
    df.drop_duplicates(subset='data_date', keep='last', inplace=True)
    df.loc[:, 'data_date'] = df.loc[:, 'data_date'].apply(lambda x: pd.Timestamp(x))
    df.loc[df.loc[:, 'data_date'] <= pd.Timestamp('2018-08-03'), 'date'] = df.loc[df.loc[:, 'data_date'] <= pd.Timestamp('2018-08-03'), 'data_date']
    df.loc[df.loc[:, 'data_date'] > pd.Timestamp('2018-08-03'), 'date'] = df.loc[df.loc[:, 'data_date'] > pd.Timestamp('2018-08-03'), 'publish_time']
    df.to_excel(ID + '.xlsx', index=False)

#%% ID01001760
ID = 'ID01001760'
df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}' and is_delete = 0".format(k=ID))  
df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
df = df.sort_values(by=['data_date'], ascending=True)
df.drop_duplicates(subset='data_date', keep='last', inplace=True)
df.loc[:, 'data_date'] = df.loc[:, 'data_date'].apply(lambda x: pd.Timestamp(x))
df.loc[df.loc[:, 'data_date'] <= pd.Timestamp('2020-08-07'), 'date'] = df.loc[df.loc[:, 'data_date'] <= pd.Timestamp('2020-08-07'), 'data_date'].apply(lambda x: x + relativedelta(days=3))
df.loc[df.loc[:, 'data_date'] > pd.Timestamp('2020-08-07'), 'date'] = df.loc[df.loc[:, 'data_date'] > pd.Timestamp('2020-08-07'), 'publish_time']
df.to_excel(ID + '.xlsx', index=False)

#%% ZN
os.chdir('D:/Xin/Program/metal/ZN/data/raw_data')

#%% ID00188144 ID00188145 ID00188330 ID00259381 ID00408213

for ID in ['ID00188144', 'ID00188145', 'ID00188330', 'ID00259381', 'ID00408213']:
    df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}' and is_delete = 0".format(k=ID))  
    df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
    df = df.dropna(subset=['data_value'], axis=0)
    df = df.sort_values(by='data_date', ascending=True)
    df = df.dropna(subset=['data_value'], axis=0)
    df.drop_duplicates(subset='data_date', keep='last', inplace=True)
    df.loc[:, 'date'] = df.loc[:, 'publish_time']
    df.to_excel(ID + '.xlsx', index=False)
    
#%% ID00188329

ID = 'ID00188329'
df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}' and is_delete = 0".format(k=ID))  
df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
df = df.sort_values(by=['data_date'], ascending=True)
df.drop_duplicates(subset='data_date', keep='last', inplace=True)
df.loc[:, 'data_date'] = df.loc[:, 'data_date'].apply(lambda x: pd.Timestamp(x))

df.loc[df.loc[:, 'data_date'] <= pd.Timestamp('2018-01-15'), 'date'] = df.loc[df.loc[:, 'data_date'] <= pd.Timestamp('2018-01-15'), 'data_date']
df.loc[df.loc[:, 'data_date'] > pd.Timestamp('2018-01-15'), 'date'] = df.loc[df.loc[:, 'data_date'] > pd.Timestamp('2018-01-15'), 'publish_time'] # 实际获取数据

df.to_excel(ID + '.xlsx', index=False)

#%% ID01001668 ID01001671
for ID in ['ID01001668', 'ID01001671']:
    df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}' and is_delete = 0".format(k=ID))  
    df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
    df = df.sort_values(by=['data_date'], ascending=True)
    df.drop_duplicates(subset='data_date', keep='last', inplace=True)
    df.loc[:, 'data_date'] = df.loc[:, 'data_date'].apply(lambda x: pd.Timestamp(x))
    df.loc[df.loc[:, 'data_date'] <= pd.Timestamp('2020-10-16'), 'date'] = df.loc[df.loc[:, 'data_date'] <= pd.Timestamp('2020-10-16'), 'data_date'].apply(lambda x: x + relativedelta(days=10))
    df.loc[df.loc[:, 'data_date'] > pd.Timestamp('2020-10-16'), 'date'] = df.loc[df.loc[:, 'data_date'] > pd.Timestamp('2020-10-16'), 'publish_time']
    df.to_excel(ID + '.xlsx', index=False)

#%% ID01030219

ID = 'ID01030219'
df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}' and is_delete = 0".format(k=ID))  
df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
df = df.sort_values(by=['data_date'], ascending=True)
df.drop_duplicates(subset='data_date', keep='last', inplace=True)
df.loc[:, 'data_date'] = df.loc[:, 'data_date'].apply(lambda x: pd.Timestamp(x))
df.loc[df.loc[:, 'data_date'] <= pd.Timestamp('2020-09-18'), 'date'] = df.loc[df.loc[:, 'data_date'] <= pd.Timestamp('2020-09-18'), 'data_date'].apply(lambda x: x + relativedelta(days=3))
df.loc[df.loc[:, 'data_date'] > pd.Timestamp('2020-09-18'), 'date'] = df.loc[df.loc[:, 'data_date'] > pd.Timestamp('2020-09-18'), 'publish_time']
df.to_excel(ID + '.xlsx', index=False)

#%% ID01030238

for ID in ['ID01030238', 'ID01030244', 'ID01030276']:
    df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}'".format(k=ID)) # 进口盈亏 
    
    df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
    df = df.sort_values(by='data_date', ascending=True) #原始数据按日期排序
    df.drop_duplicates(subset='data_date', keep='last', inplace=True) #取每天的最新数据
    df.loc[:, 'data_date'] = df.loc[:, 'data_date'].apply(lambda x: pd.Timestamp(x))
    
    df_1 = df.loc[df.loc[:, 'data_date'] <= pd.Timestamp('2020-09-25'), :]
    df_2 = df.loc[df.loc[:, 'data_date'] > pd.Timestamp('2020-09-25'), :]
    
    begin_date = df_1.loc[:, 'data_date'].values[0] #原始数据的开始日
    end_date = df_1.loc[:, 'data_date'].values[-1] #原始数据的结束日
    
    futures_api = futuresapi.API()
    calendar_1 = futures_api.futures_calendar(begin_date, pd.Timestamp(end_date) + relativedelta(days=7), columns=['date', 'is_trading_date', 'previous_trading_date', 'current_trading_date', 'yweek', 'week_day'] , is_trading_date=1)
    calendar_0 = futures_api.futures_calendar(begin_date, pd.Timestamp(end_date) + relativedelta(days=7), columns=['date', 'is_trading_date', 'previous_trading_date', 'current_trading_date', 'yweek', 'week_day'] , is_trading_date=0)
    calendar = pd.concat([calendar_1, calendar_0], axis=0, ignore_index=True)
    calendar = calendar.sort_values(by='date', ascending=True)
    calendar.loc[:, 'yweek'] = calendar.loc[:, 'yweek'].astype(int) # 全日历的周数
    
    df_value_calendar = pd.merge(df_1, calendar, how='outer', left_on='data_date', right_on='date')
    df_value_calendar.sort_values(by='date', ascending=True, inplace=True) # 数据标记日的周数，数据对齐全日历
    
    roll_week = np.roll(df_value_calendar.loc[:, 'yweek'].unique(), -1)
    roll_week[-1] = roll_week[-2] + 1
    roll_week_df = pd.DataFrame(data={'yweek': df_value_calendar.loc[:, 'yweek'].unique(), 'simulated_yweek': roll_week}) #模拟数据实际发布日是标记日的下一个周
    
    df_value_calendar = pd.merge(df_value_calendar, roll_week_df, on='yweek') # 模拟数据实际发布日是标记日的下一个周
    df_value_calendar = df_value_calendar.loc[:, ['index_code', 'data_date', 'data_value', 'is_trading_date', 'simulated_yweek', 'date', 'publish_time', 'update_time']]
    
    calendar_16_20 = pd.concat([futures_api.futures_calendar(pd.Timestamp('2016-01-01'), pd.Timestamp('2020-12-31'), columns=['date', 'yweek', 'week_day'] , is_trading_date=1), futures_api.futures_calendar(pd.Timestamp('2016-01-01'), pd.Timestamp('2020-12-31'), columns=['date', 'yweek', 'week_day'] , is_trading_date=0)], axis=0, ignore_index=True)
    calendar_16_20.loc[:, 'yweek'] = calendar_16_20.loc[:, 'yweek'].astype(int) # 16年-20年的全日历
    calendar_simulate = calendar_16_20.loc[(calendar_16_20.loc[:, 'yweek'] >= df_value_calendar.loc[:, 'simulated_yweek'].min()) & (calendar_16_20.loc[:, 'yweek'] <= df_value_calendar.loc[:, 'simulated_yweek'].max()), :].set_index(['date']).groupby('yweek').min().reset_index() # 获取模拟数据发布周的每周最早交易日
    
    calendar_simulate = pd.merge(calendar_16_20, calendar_simulate, on=['yweek', 'week_day'], how='inner')
    calendar_simulate.rename(columns={'date': 'simulate_publish_time'}, inplace=1)
    df_simulate = pd.merge(df_value_calendar, calendar_simulate, left_on='simulated_yweek', right_on='yweek', how='outer') 
    df_simulate = df_simulate.dropna(subset=['data_date'])
    
    df_simulate = df_simulate.loc[:, ['index_code', 'data_date', 'data_value', 'simulate_publish_time', 'publish_time', 'update_time']]
    df_simulate = df_simulate.rename(columns={'simulate_publish_time': 'date'})
    d = df_simulate.pop('date')
    df_simulate.loc[:, 'date'] = d
    
    df_2.loc[:, 'date'] = df_2.loc[:, 'publish_time']
    df_final = pd.concat([df_simulate, df_2], axis=0)
    
    df_final.to_excel(ID + '.xlsx', index=False)
    
#%% NI
os.chdir('D:/Xin/Program/metal/NI/data/raw_data')
#%% CM000046358 CM0000463606
for ID in ['CM0000463584', 'CM0000463606']:
    df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}' and is_delete = 0".format(k=ID))  
    df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
    df = df.sort_values(by=['data_date'], ascending=True)
    df = df.dropna(subset=['data_value'], axis=0)
    df.drop_duplicates(subset='data_date', keep='last', inplace=True)
    df.loc[:, 'data_date'] = df.loc[:, 'data_date'].apply(lambda x: pd.Timestamp(x))
    df.loc[df.loc[:, 'data_date'] < pd.Timestamp('2017-07-31'), 'date'] = df.loc[df.loc[:, 'data_date'] < pd.Timestamp('2017-07-31'), 'data_date'].apply(lambda x: x + relativedelta(days=30))
    df.loc[df.loc[:, 'data_date'] >= pd.Timestamp('2017-07-31'), 'date'] = df.loc[df.loc[:, 'data_date'] >= pd.Timestamp('2017-07-31'), 'publish_time']
    df.to_excel(ID + '.xlsx', index=False)

#%% CM0000463688
for ID in ['CM0000463688', 'CM0000463708']:
    df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}' and is_delete = 0".format(k=ID))  
    df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
    df = df.sort_values(by=['data_date'], ascending=True)
    df = df.dropna(subset=['data_value'], axis=0)
    df.drop_duplicates(subset='data_date', keep='last', inplace=True)
    df.loc[:, 'data_date'] = df.loc[:, 'data_date'].apply(lambda x: pd.Timestamp(x))
    df.loc[df.loc[:, 'data_date'] <= pd.Timestamp('2019-02-28'), 'date'] = df.loc[df.loc[:, 'data_date'] <= pd.Timestamp('2019-02-28'), 'data_date'].apply(lambda x: x + relativedelta(days=30))
    df.loc[df.loc[:, 'data_date'] > pd.Timestamp('2019-02-28'), 'date'] = df.loc[df.loc[:, 'data_date'] > pd.Timestamp('2019-02-28'), 'publish_time']
    df.to_excel(ID + '.xlsx', index=False)

#%% ID00185680 ID00188183 ID00188185

for ID in ['ID00185680', 'ID00188183', 'ID00188185', 'ID00188263','ID00265540', 'ID00302593', 'ID00302596']:
    df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}' and is_delete = 0".format(k=ID))  
    df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
    df = df.dropna(subset=['data_value'], axis=0)
    df = df.sort_values(by='data_date', ascending=True)
    df = df.dropna(subset=['data_value'], axis=0)
    df.drop_duplicates(subset='data_date', keep='last', inplace=True)
    df.loc[:, 'date'] = df.loc[:, 'publish_time']
    df.to_excel(ID + '.xlsx', index=False)

#%% ID00185743
ID = 'ID00185743'
df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}' and is_delete = 0".format(k=ID))  
df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
df = df.sort_values(by=['data_date'], ascending=True)
df.drop_duplicates(subset='data_date', keep='last', inplace=True)
df.loc[:, 'data_date'] = df.loc[:, 'data_date'].apply(lambda x: pd.Timestamp(x))

df.loc[df.loc[:, 'data_date'] <= pd.Timestamp('2020-07-31'), 'date'] = df.loc[df.loc[:, 'data_date'] <= pd.Timestamp('2020-07-31'), 'data_date'].apply(lambda x: x + relativedelta(days=15))
df.loc[df.loc[:, 'data_date'] > pd.Timestamp('2020-07-31'), 'date'] = df.loc[df.loc[:, 'data_date'] > pd.Timestamp('2020-07-31'), 'publish_time'] # 实际获取数据

df.to_excel(ID + '.xlsx', index=False)
#%% ID00188187 ID00188188 ID00408376 ID00408378
for ID in ['ID00188187', 'ID00188188', 'ID00408376', 'ID00408378']:
    df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}' and is_delete = 0".format(k=ID))  
    df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
    df = df.sort_values(by=['data_date'], ascending=True)
    df = df.dropna(subset=['data_value'], axis=0)
    df.drop_duplicates(subset='data_date', keep='last', inplace=True)
    df.loc[:, 'data_date'] = df.loc[:, 'data_date'].apply(lambda x: pd.Timestamp(x)) 
    df.loc[df.loc[:, 'data_date'] <= pd.Timestamp('2019-04-25'), 'date'] = df.loc[df.loc[:, 'data_date'] <= pd.Timestamp('2019-04-25'), 'data_date']
    df.loc[df.loc[:, 'data_date'] > pd.Timestamp('2019-04-25'), 'date'] = df.loc[df.loc[:, 'data_date'] > pd.Timestamp('2019-04-25'), 'publish_time'] # 实际获取数据
    df.to_excel(ID + '.xlsx', index=False)
#%% ID00188282
ID = 'ID00188282'
df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}' and is_delete = 0".format(k=ID))  
df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
df = df.sort_values(by=['data_date'], ascending=True)
df = df.dropna(subset=['data_value'], axis=0)
df.drop_duplicates(subset='data_date', keep='last', inplace=True)
df.loc[:, 'data_date'] = df.loc[:, 'data_date'].apply(lambda x: pd.Timestamp(x))

df.loc[df.loc[:, 'data_date'] <= pd.Timestamp('2018-08-03'), 'date'] = df.loc[df.loc[:, 'data_date'] <= pd.Timestamp('2018-08-03'), 'data_date'].apply(lambda x: x + relativedelta(days=3))
df.loc[df.loc[:, 'data_date'] > pd.Timestamp('2018-08-03'), 'date'] = df.loc[df.loc[:, 'data_date'] > pd.Timestamp('2018-08-03'), 'publish_time'] # 实际获取数据
df.to_excel(ID + '.xlsx', index=False)
#%% ID00265478 
#2019年4月12日至2020年5月12日都是2020年5月12日回补的

ID = 'ID00265478'
df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}' and is_delete = 0".format(k=ID))  
df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
df = df.sort_values(by=['data_date'], ascending=True)
df = df.dropna(subset=['data_value'], axis=0)
df.drop_duplicates(subset='data_date', keep='last', inplace=True)
df.loc[:, 'data_date'] = df.loc[:, 'data_date'].apply(lambda x: pd.Timestamp(x))

df.loc[df.loc[:, 'data_date'] < pd.Timestamp('2020-05-12'), 'date'] = df.loc[df.loc[:, 'data_date'] < pd.Timestamp('2020-05-12'), 'data_date'].apply(lambda x: x + relativedelta(days=0))
df.loc[df.loc[:, 'data_date'] >= pd.Timestamp('2020-05-12'), 'date'] = df.loc[df.loc[:, 'data_date'] >= pd.Timestamp('2020-05-12'), 'publish_time'] # 实际获取数据
df.to_excel(ID + '.xlsx', index=False)


#%% SN
os.chdir('D:/Xin/Program/metal/SN/data/raw_data')
#%% ID00188225
ID = 'ID00188225'
df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}' and is_delete = 0".format(k=ID))  
df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
df = df.sort_values(by=['data_date'], ascending=True)
df = df.dropna(subset=['data_value'], axis=0)
df.drop_duplicates(subset='data_date', keep='last', inplace=True)
df.loc[:, 'data_date'] = df.loc[:, 'data_date'].apply(lambda x: pd.Timestamp(x))

df.loc[:, 'date'] = df.loc[:, 'publish_time'] # 实际获取数据
df.to_excel(ID + '.xlsx', index=False)

#%% PB
os.chdir('D:/Xin/Program/metal/PB/data/raw_data')
#%% ID00259423
#2019年4月12日至2020年5月12日都是2020年5月12日回补的

ID = 'ID00259423'
df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}' and is_delete = 0".format(k=ID))  
df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
df = df.sort_values(by=['data_date'], ascending=True)
df = df.dropna(subset=['data_value'], axis=0)
df.drop_duplicates(subset='data_date', keep='last', inplace=True)
df.loc[:, 'data_date'] = df.loc[:, 'data_date'].apply(lambda x: pd.Timestamp(x))

df.loc[:, 'date'] = df.loc[:, 'publish_time'] # 实际获取数据
df.to_excel(ID + '.xlsx', index=False)

#%%
ID = 'ID00302799'
df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}' and is_delete = 0".format(k=ID))  
df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
df = df.sort_values(by=['data_date'], ascending=True)
df = df.dropna(subset=['data_value'], axis=0)
df.drop_duplicates(subset='data_date', keep='last', inplace=True)
df.loc[:, 'data_date'] = df.loc[:, 'data_date'].apply(lambda x: pd.Timestamp(x))

df.loc[df.loc[:, 'data_date'] < pd.Timestamp('2020-07-20'), 'date'] = df.loc[df.loc[:, 'data_date'] < pd.Timestamp('2020-07-20'), 'data_date'].apply(lambda x: x + relativedelta(days=0))
df.loc[df.loc[:, 'data_date'] >= pd.Timestamp('2020-07-20'), 'date'] = df.loc[df.loc[:, 'data_date'] >= pd.Timestamp('2020-07-20'), 'publish_time'] # 实际获取数据
df.to_excel(ID + '.xlsx', index=False)

#%% SS
os.chdir('D:/Xin/Program/metal/SS/data/raw_data')
#%% ID00187444

ID = 'ID00187444'
df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}' and is_delete = 0".format(k=ID))  
df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
df = df.sort_values(by=['data_date'], ascending=True)
df = df.dropna(subset=['data_value'], axis=0)
df.drop_duplicates(subset='data_date', keep='last', inplace=True)
df.loc[:, 'data_date'] = df.loc[:, 'data_date'].apply(lambda x: pd.Timestamp(x))

df.loc[df.loc[:, 'data_date'] < pd.Timestamp('2019-09-20'), 'date'] = df.loc[df.loc[:, 'data_date'] < pd.Timestamp('2019-09-20'), 'data_date'].apply(lambda x: x + relativedelta(days=0))
df.loc[df.loc[:, 'data_date'] >= pd.Timestamp('2019-09-20'), 'date'] = df.loc[df.loc[:, 'data_date'] >= pd.Timestamp('2019-09-20'), 'publish_time'] # 实际获取数据
df.to_excel(ID + '.xlsx', index=False)

#%%
for ID in ['ID00188852', 'ID00188853']:
    df = dbo.mysteel_data.get_data(column='index_code, data_date, data_value, publish_time, update_time', cond="where index_code = '{k}' and is_delete = 0".format(k=ID))  
    df.loc[:, 'data_value'] = df.loc[:, 'data_value'].astype(float)
    df = df.sort_values(by=['data_date'], ascending=True)
    df = df.dropna(subset=['data_value'], axis=0)
    df.drop_duplicates(subset='data_date', keep='last', inplace=True)
    df.loc[:, 'data_date'] = df.loc[:, 'data_date'].apply(lambda x: pd.Timestamp(x))
    
    df.loc[:, 'date'] = df.loc[:, 'publish_time'] 
    df.to_excel(ID + '.xlsx', index=False)




























