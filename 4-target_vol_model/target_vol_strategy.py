# -*- coding: utf-8 -*-
"""
Created on Tue Jan 12 10:52:41 2021

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

    def option_combination(self):
        option_price = self.option_price.loc[:, ['date', 'open', 'underlying_asset']]
        option_price = option_price.sort_values(by='date')
        option_factor = factor_list.loc[factor_list.loc[:, 'option'] == self.option, :]
        category_list = option_factor.loc[:, 'category'].unique().tolist()
        df = pd.DataFrame([], columns=['date'])
        
        fig = plt.figure()
        ax1 = fig.add_subplot(111)
        for category in category_list:
            os.chdir('D:/Xin/Program/metal/' + self.option + '/data/output_data/subcategory_combination/')
            data = pd.read_excel(category + '.xlsx')  
            x = data.loc[:, 'date'].values
            y = data.loc[:, 'nav_minus_cost']
            ax1.plot(x, y, label=category)
            sub_df = data.loc[:, ['date', 'mean_pos']]
            sub_df = sub_df.rename(columns={'mean_pos': 'mean_pos_' + category})
            df = pd.merge(df, sub_df, on='date', how='outer')

        plt.legend()
        plt.title(self.option)
        plt.grid()
        
        fig_path = 'D:/Xin/Program/metal/' + self.option + '/figure/category_combination/'
        if os.path.exists(fig_path):
            os.chdir(fig_path)
        else:
            os.mkdir(fig_path)
            os.chdir(fig_path)   
        plt.savefig('category.png')
        plt.show()
       
        df.sort_values(by='date', ascending=True, inplace=True)
        df.loc[:, 'mean_pos'] = df.iloc[:, 1:].mean(axis=1)
        df.loc[:, 'mean_pos'] = df.loc[:, 'mean_pos'].round(3)
        df = pd.merge(option_price, df, on='date', how='inner')
        df.insert(loc=3, column='price_change_pct', value=df.loc[:, 'open'].pct_change(periods=1))
            
        df.loc[:, 'pos'] = np.sign(df.loc[:, 'mean_pos'].values)
        df.loc[:, 'rtn'] = (np.append(np.array([0]), df.loc[:, 'pos'].fillna(0).values[:-1]) * df.loc[:, 'price_change_pct']).fillna(0)
        df.loc[:, 'turnover'] = np.append(np.array([0]), np.diff(df.loc[:, 'pos'].fillna(0)))
        df.loc[:, 'rtn_minus_cost'] = (df.loc[:, 'rtn'] - df.loc[:, 'turnover'].abs() * 0.0003).fillna(0)
        df.loc[:, 'nav'] = (1 + df.loc[:, 'rtn'].values).cumprod()
        df.loc[:, 'nav_minus_cost'] = (1 + df.loc[:, 'rtn_minus_cost'].values).cumprod()
        vmax = np.maximum.accumulate(df.loc[:, 'nav_minus_cost'])
        df.loc[:, 'mdd'] = df.loc[:, 'nav_minus_cost'] / vmax - 1         

        data_path = 'D:/Xin/Program/metal/' + self.option + '/data/output_data/category_combination/'
        if os.path.exists(data_path):
            os.chdir(data_path)
        else:
            os.mkdir(data_path)
            os.chdir(data_path)
            
        df.to_excel(self.option + '.xlsx', index=False)   
        self.view_results(df, fig_path)
            
        return 
    
    def view_results(self, data, fig_path):
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
        plt.title(self.option)
        plt.grid()
        
        os.chdir(fig_path)
        plt.savefig(self.option + '.png')
        plt.show()

        print('got the figure')

        return
    
    def corr(self):
        os.chdir('D:/Xin/Program/metal/' + self.option + '/data/output_data/subcategory_combination/')
        option_factor = factor_list.loc[factor_list.loc[:, 'option'] == self.option, :]
        category_list = option_factor.loc[:, 'category'].unique().tolist()
        num_cat = len(category_list)
        init_corr = pd.DataFrame(np.zeros((num_cat, num_cat)) * np.nan , columns=category_list, index=category_list)
        for i in range(num_cat):
            init_corr.iloc[i, i] = 1
            rtn_i = pd.read_excel(category_list[i] + '.xlsx').loc[:, ['date', 'rtn_minus_cost']]
            rtn_i.rename(columns={'rtn_minus_cost': 'rtn_minus_cost_' + category_list[i]}, inplace=True)
            for j in range(i + 1, num_cat):
                rtn_j = pd.read_excel(category_list[j] + '.xlsx').loc[:, ['date', 'rtn_minus_cost']]
                rtn_j.rename(columns={'rtn_minus_cost': 'rtn_minus_cost_' + category_list[j]}, inplace=True)
                rtn_i_j = pd.merge(rtn_i, rtn_j, how='inner', on='date')
                corr_i_j = rtn_i_j.loc[:, 'rtn_minus_cost_' + category_list[i]].corr(rtn_i_j.loc[:, 'rtn_minus_cost_' + category_list[j]])
                init_corr.iloc[j, i] = corr_i_j
                
        init_corr.to_excel('D:/Xin/Program/metal/' + self.option + '/data/output_data/category_combination/' + self.option + '_corr.xlsx', index=1)

        return
    
    
cl = Option_Net_Value('ZN')
cl.option_combination()
cl.corr()





