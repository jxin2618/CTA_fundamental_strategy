# -*- coding: utf-8 -*-
"""
Created on Thu Jan  7 11:10:57 2021

@author: J Xin
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
import os
import sys
import warnings

if not sys.warnoptions:
    warnings.simplefilter("ignore")

file_price = 'D:/Xin/Program/metal/商品指数20201214.xlsx'
data_price = pd.read_excel(file_price)

factor_list = pd.read_excel('D:/Xin/Program/metal/钢联有色数据1.xlsx', sheet_name='Sheet2')


#%%
class Option_Net_Value(object):

    def __init__(self, option):
        self.option = option
        self.dic = {'CU': '阴极铜', 'AL': '铝', 'ZN': '锌', 'NI': '镍'}
        self.option_price = data_price.loc[data_price.loc[:, 'underlying_asset'] == self.dic[self.option], :]

    def category_combination(self):
        option_price = self.option_price.loc[:, ['date', 'open', 'underlying_asset']]
        option_price = option_price.sort_values(by='date')
        option_factor = factor_list.loc[factor_list.loc[:, 'option'] == self.option, :]
        category_list = option_factor.loc[:, 'category'].unique().tolist()
        for category in category_list:
            category_df =  option_factor.loc[option_factor.loc[:, 'category'] == category, :]
            sub_category_list = category_df.loc[:, 'sub_category'].unique().tolist()
            pos_df = pd.DataFrame([], columns=['date'])
            for sub_category in sub_category_list:
                sub_category_df =  category_df.loc[category_df.loc[:, 'sub_category'] == sub_category, :]
                sub_category_df = sub_category_df.reset_index(drop=True)
                num = sub_category_df.shape[0] 
                sub_pos_df = pd.DataFrame([], columns=['date'])
                for x in range(0, num):
                    os.chdir('D:/Xin/Program/metal/' + self.option + '/data/output_data/factor_combination/')
                    data = pd.read_excel(sub_category_df.loc[x, 'index_code'] + '.xlsx')
                    data = data.loc[:, ['date', 'mean_pos']]
                    data = data.loc[data.loc[:, 'date'] >= pd.Timestamp('2015-09-01')]
                    data.rename(columns={'mean_pos': 'mean_pos_' + sub_category + str(x)}, inplace=True)
                    sub_pos_df = pd.merge(sub_pos_df, data, on='date', how='outer')
                sub_pos_df.sort_values(by='date', ascending=True, inplace=True)
                # sub_pos_df = sub_pos_df.fillna(0)
                sub_pos_df.loc[:, 'mean_pos_' + sub_category] = sub_pos_df.iloc[:, 1:].mean(axis=1)
                sub_pos_df = sub_pos_df.loc[:, ['date', 'mean_pos_' + sub_category]]
                pos_df = pd.merge(pos_df, sub_pos_df, on='date', how='outer')
            pos_df.loc[:, 'mean_pos'] = pos_df.iloc[:, 1:].mean(axis=1)
            pos_df.loc[:, 'mean_pos'] = pos_df.loc[:, 'mean_pos'].round(3)
            df = pd.merge(option_price, pos_df, on='date', how='inner')
            df.insert(loc=3, column='price_change_pct', value=df.loc[:, 'open'].pct_change(periods=1))
            
            df.loc[:, 'pos'] = np.sign(df.loc[:, 'mean_pos'].values)
            df.loc[:, 'rtn'] = (np.append(np.array([0]), df.loc[:, 'pos'].fillna(0).values[:-1]) * df.loc[:, 'price_change_pct']).fillna(0)
            df.loc[:, 'turnover'] = np.append(np.array([0]), np.diff(df.loc[:, 'pos'].fillna(0)))
            df.loc[:, 'rtn_minus_cost'] = (df.loc[:, 'rtn'] - df.loc[:, 'turnover'].abs() * 0.0003).fillna(0)
            df.loc[:, 'nav'] = (1 + df.loc[:, 'rtn'].values).cumprod()
            df.loc[:, 'nav_minus_cost'] = (1 + df.loc[:, 'rtn_minus_cost'].values).cumprod()
            vmax = np.maximum.accumulate(df.loc[:, 'nav_minus_cost'])
            df.loc[:, 'mdd'] = df.loc[:, 'nav_minus_cost'] / vmax - 1                
            
            data_path = 'D:/Xin/Program/metal/' + self.option + '/data/output_data/subcategory_combination/'
            if os.path.exists(data_path):
                os.chdir(data_path)
            else:
                os.mkdir(data_path)
                os.chdir(data_path)
            
            df.to_excel(category + '.xlsx', index=False)
            
            fig_path = 'D:/Xin/Program/metal/' + self.option + '/figure/subcategory_combination/'
            if os.path.exists(fig_path):
                os.chdir(fig_path)
            else:
                os.mkdir(fig_path)
                os.chdir(fig_path)
            self.view_results(df, category)
            
        
        return 
    
    def view_results(self, data, category):
        fig = plt.figure()
        ax1 = fig.add_subplot(111)
        x = data.loc[:, 'date'].values
        y = data.loc[:, 'nav']
        z = data.loc[:, 'nav_minus_cost']
        ax1.plot(x, y, 'orange', label='nav')
        ax1.plot(x, z, 'red', label='nav_minus_cost')
        plt.xlabel('date')
        plt.ylabel('net value')
        plt.legend()
        plt.title(category)
        plt.grid()

        plt.savefig(category + '.png')
        plt.show()

        print('got the figure')

        return


cl = Option_Net_Value('ZN')
cl.category_combination()

