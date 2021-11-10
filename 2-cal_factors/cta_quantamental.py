# -*- coding: utf-8 -*-
"""
Created on Mon Dec 14 13:17:58 2020

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

#%% classes

class Cal_Factors(object):
    
    def __init__(self, option, condition):
        self.option = option
        self.factor_api = factorpro.FactorDesigner()
        self.futures_api = futuresapi.API()
        self.begin_date = pd.Timestamp('2015-01-01')
        self.end_date = pd.Timestamp('2020-11-16')
        self.dic = {'CU': '阴极铜', 'AL': '铝', 'ZN': '锌', 'NI': '镍', 
                    'PB': '铅', 'SN': '锡', 'SS': '不锈钢'}
        self.condition= condition
        
        
    def get_calendar(self):
        calendar_is_trading_date = self.futures_api.futures_calendar(self.begin_date, self.end_date, columns=['date', 'is_trading_date', 'previous_trading_date', 'current_trading_date'] , is_trading_date=1)
        calendar_0 = self.futures_api.futures_calendar(self.begin_date, self.end_date, columns=['date', 'is_trading_date', 'previous_trading_date', 'current_trading_date'] , is_trading_date=0)
        calendar = pd.concat([calendar_is_trading_date, calendar_0], axis=0, ignore_index=True)
        calendar = calendar.sort_values(by='date', ascending=True)
        
        return calendar, calendar_is_trading_date

    # 原始数据读取
    def read_zc_data(self, index_code, freq):
        os.chdir('D:/Xin/Program/Quantamental/metal/zc_new_data/AL/data/raw_data/')
        data = pd.read_excel(index_code + '.xlsx')
        data.loc[:, 'data_value'] = data.loc[:, 'data_value'].astype('float')
        data.loc[:, 'data_date'] = data.loc[:, 'data_date'].apply(lambda x: pd.Timestamp(x))
        print('got ' + index_code + '.xlsx')
        data = data.dropna(subset=['data_value'])
        data.sort_values(by='data_date', ascending=True, inplace=True)
        
        if self.condition == 'with_lag':
            if freq == 'daily':
                data.loc[:, 'data_date'] = data.loc[:, 'data_date'].apply(lambda x: x + relativedelta(days=1)) 
            if freq == 'week':
                data.loc[:, 'data_date'] = data.loc[:, 'data_date'].apply(lambda x: x + relativedelta(days=3)) 
            if freq == 'month':
                data.loc[:, 'data_date'] = data.loc[:, 'data_date'].apply(lambda x: x + relativedelta(days=15)) 
            
        data = data.loc[data.loc[:, 'data_date'] <= self.end_date, :]

        return data
    
    def original_pop(self, data, data_type):
        print('calculating pop...')
        if data_type == 0:
            data.loc[:, 'data_value_pct'] = data.loc[:, 'data_value'].pct_change(periods=1) # 原始数据的环比
        else:
            data.loc[:, 'data_value_pct'] = data.loc[:, 'data_value'].diff(periods=1) 
        data.loc[:, 'data_value_pct_diff'] = data.loc[:, 'data_value_pct'].diff(periods=1) # 原始数据的环比的变化
            
        return data

    def merge_calendar(self, df, calendar):
        df_value_calendar = pd.merge(df, calendar, how='outer',  left_on='data_date', right_on='date')
        df_value_calendar.sort_values(by='date', ascending=True, inplace=True)
        df_value_calendar = df_value_calendar.fillna(method='ffill')
        df_value_calendar = df_value_calendar.dropna(subset=['data_date'])
        
        os.chdir('D:/Xin/Program/Quantamental/metal/')
        data_price = pd.read_excel('商品指数20201214.xlsx')
        data_price = data_price.loc[(data_price.loc[:, 'underlying_asset'] == self.dic[self.option]) & (data_price.loc[:, 'date'] >= self.begin_date) & (data_price.loc[:, 'date'] <= self.end_date), :].sort_values(by='date', ascending=True).reset_index(drop=True)
        data_price = data_price.loc[:, ['date', 'open', 'sec_code']]
        data_price_begin_date = data_price.sort_values(by='date', ascending=True).reset_index(drop=True).loc[0, 'date']
        
        # data = pd.merge(df_value_calendar, data_price, on='date', how='inner')
        data = df_value_calendar.loc[df_value_calendar.loc[:, 'is_trading_date'] == 1, :]
        data = pd.merge(data, data_price, on='date', how='outer')
        data = data.sort_values(by='date', ascending=True)
        # data = data.dropna(subset=['data_date'])
        data.loc[:, 'price_change_pct'] = data.loc[:, 'open'].pct_change().fillna(0)
        data.loc[:, 'sec_code'] = data.loc[:, 'sec_code'].fillna(method='ffill').fillna(method='bfill')
        
        raw_data = data.loc[:, ['date', 'data_value', 'sec_code']]
        raw_data = raw_data.set_index(['date', 'sec_code'])   
        date_list = df_value_calendar.loc[:, 'date'].values
        # date_list = df_value_calendar.loc[df_value_calendar.loc[:, 'date'] >= data_price_begin_date, 'date'].values

        return data, raw_data, date_list
    
    def factor_date(self, date_list, calendar_is_trading_date):
        periods_1m = {'months': 1}
        periods_1y = {'years': 1}
        periods_1y1m = {'months': 1, 'years': 1}
        periods_2y = {'years': 2}
        first_date_1m_later = datepro.DatePro().date_shift(data=date_list[0], **periods_1m)
        first_date_1y_later = datepro.DatePro().date_shift(data=date_list[0], **periods_1y)
        first_date_1y1m_later = datepro.DatePro().date_shift(data=date_list[0], **periods_1y1m)
        first_date_2y_later = datepro.DatePro().date_shift(data=date_list[0], **periods_2y)
        later_1m = [d for d in date_list if (d >= first_date_1m_later and d in calendar_is_trading_date.date.tolist())]
        later_1y = [d for d in date_list if (d >= first_date_1y_later and d in calendar_is_trading_date.date.tolist())]
        later_1y1m = [d for d in date_list if (d >= first_date_1y1m_later and d in calendar_is_trading_date.date.tolist())]
        later_2y = [d for d in date_list if (d >= first_date_2y_later and d in calendar_is_trading_date.date.tolist())]
        
        return later_1m, later_1y, later_1y1m, later_2y
    
    def month_average_data(self, raw_data, calendar, later_1m):
        print('calculating month average value...')
        df_data_value_mean = pd.DataFrame() 
        for date in later_1m:
            try:
                sub_df_mean = self.factor_api.mean(date=date, data=raw_data, period={'months': -1}, calendar=calendar)    
                df_data_value_mean = df_data_value_mean.append(sub_df_mean)
            except IndexError:
                pass
        return df_data_value_mean
    
    def pop(self, factor_dictionary, data, df_data_value_mean, data_type, calendar, later_1y, later_1y1m):
        df_pct = data.loc[:, ['date', 'data_value_pct']].fillna(0) #4
        df_pct_diff = data.loc[:, ['date', 'data_value_pct_diff']].fillna(0) #5
        df_pct.loc[:, 'sec_code'] = self.option
        df_pct_diff.loc[:, 'sec_code'] = self.option
        df_pct = df_pct.set_index(['date', 'sec_code'])
        df_pct_diff = df_pct_diff.set_index(['date', 'sec_code'])
        factor_dictionary[df_pct.columns.tolist()[0]] = df_pct
        factor_dictionary[df_pct_diff.columns.tolist()[0]] = df_pct_diff       
        # 环比_quantile
        df_pct_quantile = pd.DataFrame() #6
        for date in later_1y:
            try:
                sub_df_pct_quantile = self.factor_api.quantile(date=date, data=df_pct, period={'years': -1}, calendar=calendar)
                df_pct_quantile = df_pct_quantile.append(sub_df_pct_quantile)
            except IndexError:
                pass
        factor_dictionary[df_pct_quantile.columns.tolist()[0]] = df_pct_quantile
        
        if data_type == 0:
            df_mean_pct = df_data_value_mean.pct_change(periods=1).rename(columns={'data_value_mean_1m': 'data_value_mean_1m_pct'}) #7
        else:            
            df_mean_pct = df_data_value_mean.diff(periods=1).rename(columns={'data_value_mean_1m': 'data_value_mean_1m_pct'}) #7
        df_mean_pct_diff = df_mean_pct.diff(periods=1).rename(columns={'data_value_mean_1m_pct': 'data_value_mean_1m_pct_diff'}) #8
        factor_dictionary[df_mean_pct.columns.tolist()[0]] = df_mean_pct
        factor_dictionary[df_mean_pct_diff.columns.tolist()[0]] = df_mean_pct_diff
        
        df_mean_pct_quantile = pd.DataFrame() #9
        for date in later_1y1m:
            sub_df_mean_pct_quantile = self.factor_api.quantile(date=date, data=df_mean_pct, period={'years': -1}, calendar=calendar)
            df_mean_pct_quantile = df_mean_pct_quantile.append(sub_df_mean_pct_quantile)
        factor_dictionary[df_mean_pct_quantile.columns.tolist()[0]] = df_mean_pct_quantile
                
        return factor_dictionary
       
    def yoy(self, factor_dictionary, data, raw_data, df_data_value_mean, data_type, calendar, later_1y, later_1y1m, later_2y): 
        print('calculating yoy...')
        # 同比
        df_yoy = pd.DataFrame() #10
        data_date = data.reset_index().loc[:, ['data_date']].drop_duplicates()
        for date in later_1y:
            try:
                if data_type == 0:
                    sub_df_yoy = self.factor_api.yoy(date=date, data=raw_data, period={'years': -1}, calendar=calendar)
                else:
                    pre_data = self.factor_api.pre(date=date, data=raw_data, period={'years': -1}, calendar=calendar, add_suffix=False)
                    sub_df_yoy = (raw_data.xs(date, level='date') - pre_data).rename(columns={'data_value': 'data_value_yoy_1y'}) 
                df_yoy = df_yoy.append(sub_df_yoy)
            except KeyError:
                pass
        factor_dictionary[df_yoy.columns.tolist()[0]] = df_yoy    
        # 同比的变化率
        df_yoy_data_date = pd.merge(df_yoy.reset_index(), data_date, left_on='date', right_on='data_date', how='inner')
        df_yoy_diff = df_yoy_data_date.loc[:, ['date', 'sec_code', 'data_value_yoy_1y']] 
        df_yoy_diff.loc[:, 'data_value_yoy_1y_diff'] = df_yoy_diff.loc[:, 'data_value_yoy_1y'].diff()
        df_yoy_diff = pd.merge(df_yoy_diff, calendar, how='outer', on='date').sort_values(by='date')
        df_yoy_diff.loc[:, 'data_value_yoy_1y_diff'] = df_yoy_diff.loc[:, 'data_value_yoy_1y_diff'].fillna(method='ffill').fillna(0)
        df_yoy_diff = df_yoy_diff.loc[df_yoy_diff.loc[:, 'date'] >= later_1y[0], :]
        df_yoy_diff.loc[:, 'sec_code'] = self.option
        df_yoy_diff = df_yoy_diff.loc[:, ['date', 'sec_code', 'data_value_yoy_1y_diff']].set_index(['date', 'sec_code'])  #11
        factor_dictionary[df_yoy_diff.columns.tolist()[0]] = df_yoy_diff
        # 同比的quantile
        df_yoy_quantile = pd.DataFrame() #12
        for date in later_2y:
            try:
                sub_df_yoy_quantile = self.factor_api.quantile(date=date, data=df_yoy, period={'years': -1}, calendar=calendar)           
                df_yoy_quantile = df_yoy_quantile.append(sub_df_yoy_quantile)
            except KeyError:
                pass
        if len(df_yoy_quantile.columns.tolist()) > 0:
            factor_dictionary[df_yoy_quantile.columns.tolist()[0]] = df_yoy_quantile
        # 平均同比
        df_mean_yoy = pd.DataFrame() #13
        for date in later_1y1m:
            try:
                if data_type == 0:
                    sub_df_mean_yoy = self.factor_api.yoy(date=date, data=df_data_value_mean, period={'years': -1}, calendar=calendar)
                else:
                    pre_data = self.factor_api.pre(date=date, data=df_data_value_mean, period={'years': -1}, calendar=calendar, add_suffix=False)
                    sub_df_mean_yoy = (df_data_value_mean.xs(date, level='date') - pre_data).rename(columns={'data_value_mean_1m': 'data_value_mean_1m_yoy_1y'}) 
                df_mean_yoy = df_mean_yoy.append(sub_df_mean_yoy)
            except KeyError:
                pass        
        factor_dictionary[df_mean_yoy.columns.tolist()[0]] = df_mean_yoy
        # 平均同比变化率
        df_mean_yoy_diff = df_mean_yoy.diff(periods=1).rename(columns={'data_value_mean_1m_yoy_1y': 'data_value_mean_1m_yoy_1y_diff'}) #14
        factor_dictionary[df_mean_yoy_diff.columns.tolist()[0]] = df_mean_yoy_diff     
        # 平均同比quantile
        df_mean_yoy_quantile = pd.DataFrame() #15
        for date in later_2y:
            try:    
                sub_df_mean_yoy_quantile = self.factor_api.quantile(date=date, data=df_mean_yoy, period={'years': -1}, calendar=calendar)
                df_mean_yoy_quantile = df_mean_yoy_quantile.append(sub_df_mean_yoy_quantile) 
            except KeyError:
                pass
        if len(df_mean_yoy_quantile.columns.tolist()) > 0:
            factor_dictionary[df_mean_yoy_quantile.columns.tolist()[0]] = df_mean_yoy_quantile

        return factor_dictionary

    def momentum(self, factor_dictionary, raw_data, data_type, later_1m, later_1y1m):
        print('calculating momentum...')
        df_momentum = pd.DataFrame() #1
        df_reversion = pd.DataFrame() #2
        for date in later_1m:
            try:
                if data_type == 0:  
                    sub_df_momentum = self.factor_api.momentum(date=date, data=raw_data, period={'months': -1}, calendar=calendar)
                    sub_df_reversion = self.factor_api.reversion(date=date, data=raw_data, period={'months': -1}, calendar=calendar)          
                else:
                    pre_data = self.factor_api.pre(date=date, data=raw_data, period={'months': -1}, calendar=calendar, add_suffix=False)
                    sub_df_momentum = (raw_data.xs(date, level='date') - pre_data).rename(columns={'data_value': 'data_value_momentum_1m'})
                    sub_df_reversion = -sub_df_momentum.rename(columns={'data_value_momentum_1m': 'data_value_reversion_1m'})            
                df_momentum = df_momentum.append(sub_df_momentum)
                df_reversion = df_reversion.append(sub_df_reversion)
            except KeyError:
                pass
        factor_dictionary[df_momentum.columns.tolist()[0]] = df_momentum
        factor_dictionary[df_reversion.columns.tolist()[0]] = df_reversion
        
        # momentum quantile 
        df_momentum_quantile = pd.DataFrame() #3
        for date in later_1y1m:
            try:
                sub_df_momentum_quantile = self.factor_api.quantile(date=date, data=df_momentum, period={'years': -1}, calendar=calendar)
                df_momentum_quantile = df_momentum_quantile.append(sub_df_momentum_quantile)
            except KeyError:
                pass
        factor_dictionary[df_momentum_quantile.columns.tolist()[0]] = df_momentum_quantile

        return factor_dictionary

    def quantile_zscore_ttm(self, factor_dictionary, raw_data, calendar, later_1y):
        print('calculating quantile, zscore, ttm...')
        df_zscore = pd.DataFrame()
        df_ttm = pd.DataFrame()
        df_quantile = pd.DataFrame()
        for date in later_1y:
            try:
                sub_df_zscore = self.factor_api.zscore(date=date, data=raw_data, period={'years': -1}, calendar=calendar)
                df_zscore = df_zscore.append(sub_df_zscore)
                sub_df_ttm = self.factor_api.mean(date=date, data=raw_data, period={'years': -1}, calendar=calendar)       
                df_ttm = df_ttm.append(sub_df_ttm)
                sub_df_quantile = self.factor_api.quantile(date=date, data=raw_data, period={'years': -1}, calendar=calendar)
                df_quantile = df_quantile.append(sub_df_quantile)
            except IndexError:
                pass
        
        temp = pd.merge(raw_data.reset_index(), df_ttm.reset_index(), how='inner', on='date')
        temp.loc[:, 'data_value_ttm'] = temp.loc[:, 'data_value'] / temp.loc[:, 'data_value_mean_1y']
        df_ttm = pd.DataFrame(data = temp.loc[:, 'data_value_ttm'].values, columns=['data_value_ttm'], index=df_ttm.index)
        factor_dictionary[df_zscore.columns.tolist()[0]] = df_zscore
        factor_dictionary[df_ttm.columns.tolist()[0]] = df_ttm
        factor_dictionary[df_quantile.columns.tolist()[0]] = df_quantile
            
        return factor_dictionary

    def slope(self, factor_dictionary, raw_data, calendar, later_1y):
        df_slope = pd.DataFrame()
        for date in later_1y:
            try:
                sub_df_slope = self.factor_api.slope(date=date, data=raw_data, period={'years': -1}, calendar=calendar)
                df_slope = df_slope.append(sub_df_slope)
            except IndexError:
                pass
        factor_dictionary[df_slope.columns.tolist()[0]] = df_slope 
        
        return factor_dictionary
    
    def get_futures_price(self):
        os.chdir('D:/Xin/Program/Quantamental/metal/future_prices/wind/')
        df_future = pd.read_excel(self.option + '.xlsx')
        df_future = df_future.sort_values(by='data_date', ascending=True)
        df_future.drop_duplicates(subset='data_date', keep='last', inplace=True)
        df_future.loc[:, 'data_date'] = df_future.loc[:, 'data_date'].apply(lambda x: pd.Timestamp(x))
        df_future = df_future.fillna(method='ffill')
        df_future = df_future.loc[(df_future.loc[:, 'data_date'] >= self.begin_date) & (df_future.loc[:, 'data_date'] <= self.end_date), :]
        
        return df_future
    

class Cal_Net_Value(object):
    
    def __init__(self, option, condition):
        self.option = option
        self.condition = condition
    
    def view_data(self, data, data_name, index_code):
        if os.path.exists('D:/Xin/Program/Quantamental/metal/zc_new_data/' + self.option + '/figure/factors/' + str(index_code)) == 0:
            os.mkdir('D:/Xin/Program/Quantamental/metal/zc_new_data/' + self.option + '/figure/factors/' + str(index_code))
        if os.path.exists('D:/Xin/Program/Quantamental/metal/zc_new_data/' + self.option + '/figure/factors/' + str(index_code) + '/' + self.condition) == 0:
            os.mkdir('D:/Xin/Program/Quantamental/metal/zc_new_data/' + self.option + '/figure/factors/' + str(index_code) + '/' + self.condition)
        os.chdir('D:/Xin/Program/Quantamental/metal/zc_new_data/' + self.option + '/figure/factors/' + str(index_code) + '/' + self.condition)
        fig1 = plt.figure()
        ax1 = fig1.add_subplot(111)
        x = data.loc[:, 'date'].values
        y = data.loc[:, 'data_value'].values
        z = data.loc[:, 'open']
        ax1.plot(x, y, color='blue', label='data_value')
        ax2 = ax1.twinx()
        ax2.plot(x, z, color='red', label='price')
        ax1.legend(loc='upper left')
        ax2.legend(loc='upper right')
        plt.xlabel('data_date')
        plt.ylabel(str(data_name))
        plt.title(data_name)
        plt.grid()
        plt.savefig('data.png')
        plt.show()
        if os.path.exists('D:/Xin/Program/Quantamental/metal/zc_new_data/' + self.option + '/data/intermediate_data/' + str(index_code)) == 0:
            os.mkdir('D:/Xin/Program/Quantamental/metal/zc_new_data/' + self.option + '/data/intermediate_data/' + str(index_code))
        if os.path.exists('D:/Xin/Program/Quantamental/metal/zc_new_data/' + self.option + '/data/intermediate_data/' + str(index_code) + '/' + self.condition) == 0:
            os.mkdir('D:/Xin/Program/Quantamental/metal/zc_new_data/' + self.option + '/data/intermediate_data/' + str(index_code) + '/' + self.condition)
        os.chdir('D:/Xin/Program/Quantamental/metal/zc_new_data/' + self.option + '/data/intermediate_data/' + str(index_code) + '/' + self.condition)
        data.to_excel('data_value.xlsx', index=0)

        return


    def get_pos(self, df, suffix, direction):  # 计算头寸
        if suffix == 'data_value_zscore_1y':
            df.loc[:, suffix + '_pos'] = ((df.loc[:, suffix].values > 1).astype(int) - (df.loc[:, suffix].values < -1).astype(int)) * direction   
        elif suffix in ['data_value_mean_1m_pct', 'data_value_mean_1m_pct_diff', 'data_value_mean_1m_yoy_1y', 'data_value_mean_1m_yoy_1y_diff', 'data_value_pct', 'data_value_pct_diff', 'data_value_yoy_1y', 'data_value_yoy_1y_diff', 'data_value_slope_1y', 'data_value_momentum_1m', 'data_value_reversion_1m']: 
            df.loc[:, suffix + '_pos'] = np.sign(df.loc[:, suffix].values) * direction
        elif suffix in ['data_value_mean_1m_pct_quantile_1y', 'data_value_mean_1m_yoy_1y_quantile_1y', 'data_value_momentum_1m_quantile_1y', 'data_value_pct_quantile_1y', 'data_value_quantile_1y', 'data_value_yoy_1y_quantile_1y']:
            df.loc[:, suffix + '_pos'] = ((df.loc[:, suffix].values > 0.75).astype(int) - (df.loc[:, suffix].values < 0.25).astype(int)) * direction
            df.loc[df.loc[:, suffix].isna()==True, suffix + '_pos'] = np.nan      
        elif suffix == 'data_value_ttm':
            df.loc[:, suffix + '_pos'] = np.sign(df.loc[:, suffix].values - 1) * direction  
        df.loc[df.loc[:, suffix].isna()==True, suffix + '_pos'] = np.nan  
        df.loc[:, suffix + '_pos'] = np.append(np.array([np.nan]), np.roll(df.loc[:, suffix + '_pos'], 1)[1:])
        return df

    def get_nav(self, df, suffix, index_code): # 计算净值
        df = df.loc[df.loc[:, 'open'].isna() == 0, :]
        df.loc[:, suffix + '_rtn'] = np.append(np.array([0]), np.roll(df.loc[:, suffix + '_pos'].fillna(0), 1)[1:]) * df.loc[:, 'price_change_pct']
        df.loc[:, suffix + '_turnover'] = np.append(np.array([0]), np.diff(df.loc[:, suffix + '_pos'].fillna(0)))
        df.loc[:, suffix + '_rtn_minus_cost'] = (df.loc[:, suffix + '_rtn'] - df.loc[:, suffix + '_turnover'].abs() * 0.0003).fillna(0)
        df.loc[:, suffix + '_nav'] = (1 + df.loc[:, suffix + '_rtn'].values).cumprod()
        df.loc[:, suffix + '_nav_minus_cost'] = (1 + df.loc[:, suffix + '_rtn_minus_cost'].values).cumprod()
        vmax = np.maximum.accumulate(df.loc[:, suffix + '_nav_minus_cost'])
        df.loc[:, suffix + '_mdd'] = df.loc[:, suffix + '_nav_minus_cost'] / vmax - 1
        
        os.chdir('D:/Xin/Program/Quantamental/metal/zc_new_data/' + self.option + '/data/intermediate_data/' + str(index_code) + '/' + self.condition)
        df.to_excel(suffix + '.xlsx', index=0)
       
        return df


    def view_factors(self, df, data_name, key, index_code):
        os.chdir('D:/Xin/Program/Quantamental/metal/zc_new_data/' + self.option + '/figure/factors/' + str(index_code) + '/' + self.condition)
        fig3 = plt.figure()
        ax3 = fig3.add_subplot(111)
        x = df.loc[:, 'date'].values
        y = df.loc[:, key]
        z = df.loc[:, key + '_nav']
        w = df.loc[:, key + '_nav_minus_cost']
        ax3.plot(x, y, color='blue', label=key)
        ax4 = ax3.twinx()
        ax4.plot(x, z, color='orange', label=key+ '_nav')
        ax4.plot(x, w, color='red', label=key + '_nav_minus_cost')
        plt.xlabel('date')
        # plt.ylabel(str(key))
    
        ax3.legend(loc='upper left')
        ax4.legend(loc='upper right')
        plt.title(data_name + '_' + key)
        plt.grid()
        plt.savefig(str(key) + '.png')
        plt.show()
    
        return
    
    
class Cal_Quantamental_Factors(object):
    
    def __init__(self, option):
        self.option = option
        
    def cal_basis(self, data_no_lag_dic, spot_index, df_future):
        df_spot = data_no_lag_dic[spot_index][0]
        df_spot = df_spot.rename(columns={'data_value': 'spot'})
        df_future_spot = pd.merge(df_future, df_spot, how='outer', on='data_date')
        df_future_spot = df_future_spot.sort_values(by='data_date', ascending=True).fillna(method='ffill')
        df_future_spot = df_future_spot.dropna(subset=['spot'])
        df_future_spot.loc[:, 'data_value'] = (df_future_spot.loc[:, 'spot'] - df_future_spot.loc[:, 'future']) / df_future_spot.loc[:, 'spot'] * 100 # 数据类型：-2, 比率*100
        df_future_spot = df_future_spot.loc[:, ['data_date', 'data_value']]
        data_no_lag_dic[spot_index] = [df_future_spot, -2]
        
        return  data_no_lag_dic  
    
    def cal_basis_rate(self, data_no_lag_dic, basis_index, df_future):
        df_spot = data_no_lag_dic[basis_index][0]
        df_spot = df_spot.rename(columns={'data_value': 'basis'})
        df_future_spot = pd.merge(df_future, df_spot, how='outer', on='data_date')
        df_future_spot = df_future_spot.sort_values(by='data_date', ascending=True).fillna(method='ffill')
        df_future_spot = df_future_spot.dropna(subset=['basis'])
        df_future_spot.loc[:, 'data_value'] = df_future_spot.loc[:, 'basis'] / df_future_spot.loc[:, 'future'] * 100 # 数据类型：-2, 比率*100
        df_future_spot = df_future_spot.loc[:, ['data_date', 'data_value']]
        data_no_lag_dic[basis_index] = [df_future_spot, -2]
        
        return  data_no_lag_dic          
        
    def cal_scrap_spot_rate(self, data_no_lag_dic, scrap_index, df_spot):
        df_scrap = data_no_lag_dic[scrap_index][0].rename(columns={'data_value': 'scrap'})
        df_scrap_spot = pd.merge(df_scrap, df_spot, how='outer', on='data_date')
        df_scrap_spot = df_scrap_spot.sort_values(by='data_date', ascending=True).fillna(method='ffill')
        df_scrap_spot = df_scrap_spot.dropna(subset=['spot', 'scrap'])
        df_scrap_spot.loc[:, 'data_value'] = (df_scrap_spot.loc[:, 'spot'] - df_scrap_spot.loc[:, 'scrap']) / df_scrap_spot.loc[:, 'spot'] * 100 # 数据类型：-2, 比率*100
        df_scrap_spot = df_scrap_spot.loc[:, ['data_date', 'data_value']]
        data_no_lag_dic[scrap_index] = [df_scrap_spot, -2]
        
        return data_no_lag_dic
    
    def cal_profit_rate(self, data_no_lag_dic, profit_index, df_spot):
        df_prof = data_no_lag_dic[profit_index][0].rename(columns={'data_value': 'profit'})
        df_profit_spot = pd.merge(df_prof, df_spot, how='outer', on='data_date')
        df_profit_spot = df_profit_spot.sort_values(by='data_date', ascending=True).fillna(method='ffill')
        df_profit_spot = df_profit_spot.dropna(subset=['spot', 'profit'])
        df_profit_spot.loc[:, 'data_value'] = df_profit_spot.loc[:, 'profit'] / df_profit_spot.loc[:, 'spot'] * 100 # 数据类型：-2, 比率*100
        df_profit_spot = df_profit_spot.loc[:, ['data_date', 'data_value']]
        data_no_lag_dic[profit_index] = [df_profit_spot, -2]
        
        return data_no_lag_dic
#%% main function

def main(cal_factors, cal_nv, data_no_lag_dic, data_name_list):
    for index in data_no_lag_dic:
        data = data_no_lag_dic[index][0]
        data_type = data_no_lag_dic[index][1]
        direction = data_name_list.loc[data_name_list.loc[:, 'index_code'] == index, 'direction'].values[0]
        data_name = data_name_list.loc[data_name_list.loc[:, 'index_code'] == index, 'index_name'].values[0]
        data = cal_factors.original_pop(data, data_type)
        data, raw_data, date_list = cal_factors.merge_calendar(data, calendar)
        later_1m, later_1y, later_1y1m, later_2y = cal_factors.factor_date(date_list, calendar_is_trading_date)
        df_data_value_mean = cal_factors.month_average_data(raw_data, calendar, later_1m)
        
        factor_dictionary = {}
        factor_dictionary = cal_factors.pop(factor_dictionary, data, df_data_value_mean, data_type, calendar, later_1y, later_1y1m)
        factor_dictionary = cal_factors.yoy(factor_dictionary, data, raw_data, df_data_value_mean, data_type, calendar, later_1y, later_1y1m, later_2y)
        factor_dictionary = cal_factors.momentum(factor_dictionary, raw_data, data_type, later_1m, later_1y1m)
        factor_dictionary = cal_factors.quantile_zscore_ttm(factor_dictionary, raw_data, calendar, later_1y)
        factor_dictionary = cal_factors.slope(factor_dictionary, raw_data, calendar, later_1y)
        data.pop('data_value_pct')
        data.pop('data_value_pct_diff')
        cal_nv.view_data(data, data_name, index)
        
        for key in factor_dictionary:
            value = factor_dictionary[key]
            df = pd.merge(data, value, on='date', how='outer')
            df = df.loc[df.loc[:, 'is_trading_date'] == 1, :]
            df = cal_nv.get_pos(df, key, direction)
            # df = df.fillna(method='ffill').fillna(0)
            df = cal_nv.get_nav(df, key, index)
            cal_nv.view_factors(df, data_name, key, index)

    return     
 
#%% NI
# option = 'NI'
# for condition in ['with_lag', 'without_lag']:
#     cal_factors = Cal_Factors(option, condition)
#     cal_quant_factors = Cal_Quantamental_Factors(option)
#     cal_nv = Cal_Net_Value(option, condition)
#     calendar, calendar_is_trading_date = cal_factors.get_calendar()
    
#     ## raw_data_list
#     file_data = 'D:/Xin/Program/Quantamental/metal/钢联有色数据1.xlsx'
#     data_name_list = pd.read_excel(file_data, sheet_name='卓创')
#     data_name_list = data_name_list.loc[data_name_list.loc[:, 'option'] == option, :]
    
#     # data_name_list = data_name_list.loc[data_name_list.loc[:, '是否使用'] == 1, :]
#     data_name_list = data_name_list.iloc[:, :-2]
#     index_list = data_name_list.loc[:, 'index_code'].values.tolist()
#     data_no_lag_dic = {}
#     for index in index_list:
#         data_type = data_name_list.loc[data_name_list.loc[:, 'index_code'] == index, 'data_type'].values[0]
#         freq = data_name_list.loc[data_name_list.loc[:, 'index_code'] == index, 'freq'].values[0]
#         data = cal_factors.read_zc_data(index, freq)
#         data_no_lag_dic[index] = [data, data_type]
    
    
#     ##
#     main(cal_factors, cal_nv, data_no_lag_dic, data_name_list)

#%% PB
# option = 'PB'
# for condition in ['with_lag', 'without_lag']:
#     cal_factors = Cal_Factors(option, condition)
#     cal_quant_factors = Cal_Quantamental_Factors(option)
#     cal_nv = Cal_Net_Value(option, condition)
#     calendar, calendar_is_trading_date = cal_factors.get_calendar()
    
#     ## raw_data_list
#     file_data = 'D:/Xin/Program/Quantamental/metal/钢联有色数据1.xlsx'
#     data_name_list = pd.read_excel(file_data, sheet_name='卓创')
#     data_name_list = data_name_list.loc[data_name_list.loc[:, 'option'] == option, :]
    
#     # data_name_list = data_name_list.loc[data_name_list.loc[:, '是否使用'] == 1, :]
#     data_name_list = data_name_list.iloc[:, :-2]
#     index_list = data_name_list.loc[:, 'index_code'].values.tolist()
#     data_no_lag_dic = {}
#     for index in index_list:
#         data_type = data_name_list.loc[data_name_list.loc[:, 'index_code'] == index, 'data_type'].values[0]
#         freq = data_name_list.loc[data_name_list.loc[:, 'index_code'] == index, 'freq'].values[0]
#         data = cal_factors.read_zc_data(index, freq)
#         data_no_lag_dic[index] = [data, data_type]
    
    
#     ##
#     main(cal_factors, cal_nv, data_no_lag_dic, data_name_list)
#%% SS
option = 'AL'
for condition in ['with_lag', 'without_lag']:
    cal_factors = Cal_Factors(option, condition)
    cal_quant_factors = Cal_Quantamental_Factors(option)
    cal_nv = Cal_Net_Value(option, condition)
    calendar, calendar_is_trading_date = cal_factors.get_calendar()
    
    ## raw_data_list
    file_data = 'D:/Xin/Program/Quantamental/metal/钢联有色数据1.xlsx'
    data_name_list = pd.read_excel(file_data, sheet_name='卓创红期新版')
    data_name_list = data_name_list.loc[data_name_list.loc[:, 'option'] == option, :]
    
    # data_name_list = data_name_list.loc[data_name_list.loc[:, '是否使用'] == 1, :]
    data_name_list = data_name_list.iloc[:, :-2]
    index_list = data_name_list.loc[:, 'index_code'].values.tolist()
    data_no_lag_dic = {}
    for index in index_list:
        data_type = data_name_list.loc[data_name_list.loc[:, 'index_code'] == index, 'data_type'].values[0]
        freq = data_name_list.loc[data_name_list.loc[:, 'index_code'] == index, 'freq'].values[0]
        data = cal_factors.read_zc_data(index, freq)
        data_no_lag_dic[index] = [data, data_type]
    
    
    ##
    main(cal_factors, cal_nv, data_no_lag_dic, data_name_list)



















