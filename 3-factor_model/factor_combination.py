# -*- coding: utf-8 -*-
"""
Created on Wed Jan  6 09:57:04 2021

@author: J Xin
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
import os
import sys
import warnings

if not sys.warnoptions:
    warnings.simplefilter("ignore")
    
    
#%%
file_price = 'D:/Xin/Program/metal/商品指数20201214.xlsx'
data_price = pd.read_excel(file_price)

class factor_combination(object):
    def __init__(self, option, factor_dic):
        self.option = option
        self.factor_dic = factor_dic
        self.dic = {'CU': '阴极铜', 'AL': '铝', 'ZN': '锌', 'NI': '镍'}
        
    def single_factor_combination(self, id_num):
        id_list = pd.read_excel('D:/Xin/Program/metal/钢联有色数据1.xlsx', sheet_name='Sheet2 (2)')
        id_list = id_list.loc[id_list.loc[:, 'option'] == self.option, :]
        name = id_list.loc[id_list.loc[:, 'index_code'] == id_num, 'short_name'].values[0]
        
        option_close = data_price.loc[data_price.loc[:, 'underlying_asset'] == self.dic[self.option]] 
        option_close = option_close.loc[:, ['date', 'open', 'underlying_asset']]
        option_close = option_close.sort_values(by='date')
        
        if id_num[0: 4] == 'wind' or id_num[0:2] == 'zc':
            os.chdir('D:/Xin/Program/metal/' + self.option + '/data/intermediate_data/' + id_num + '/without_lag')        
        else:
            os.chdir('D:/Xin/Program/metal/' + self.option + '/data/intermediate_data/' + id_num + '/with_lag')          
        
        file_list = pd.DataFrame(self.factor_dic[id_num], columns=['filename', 'if_add'])
        file_list = file_list.loc[file_list.loc[:, 'if_add'] == 1, 'filename']
        df = pd.DataFrame(data=[], columns=['date'])
        for file in file_list:
            sub_df = pd.read_excel(file + '.xlsx')
            sub_df = sub_df.iloc[:,[2, -7]]
            df = pd.merge(df, sub_df, on='date', how='outer')
        
        df = pd.merge(option_close, df, on='date', how='inner')
        df.insert(loc=3, column='price_change_pct', value=df.loc[:, 'open'].pct_change(periods=1))
        num_row = df.shape[0]
        num_factor = df.shape[1] - 4
        for i in range(num_row):
            pos = df.iloc[i, 4:]
            df.loc[i, 'count_pos'] = len(pos[~pd.isna(pos)])
            df.loc[i, 'mean_pos'] = np.mean(pos[~pd.isna(pos)])
        df.loc[:, 'pos'] = np.sign(df.loc[:, 'mean_pos'].values)
        df.loc[:, 'rtn'] = (np.append(np.array([0]), df.loc[:, 'pos'].fillna(0).values[:-1]) * df.loc[:, 'price_change_pct']).fillna(0)
        df.loc[:, 'turnover'] = np.append(np.array([0]), np.diff(df.loc[:, 'pos'].fillna(0)))
        df.loc[:, 'rtn_minus_cost'] = (df.loc[:, 'rtn'] - df.loc[:, 'turnover'].abs() * 0.0003).fillna(0)
        df.loc[:, 'nav'] = (1 + df.loc[:, 'rtn'].values).cumprod()
        df.loc[:, 'nav_minus_cost'] = (1 + df.loc[:, 'rtn_minus_cost'].values).cumprod()
        vmax = np.maximum.accumulate(df.loc[:, 'nav_minus_cost'])
        df.loc[:, 'mdd'] = df.loc[:, 'nav_minus_cost'] / vmax - 1
        
        factor_nv_path = 'D:/Xin/Program/metal/' + self.option + '/data/output_data/factor_combination/'
        if os.path.exists(factor_nv_path):
            os.chdir(factor_nv_path)
        else:
            os.mkdir(factor_nv_path)
            os.chdir(factor_nv_path)
        
        df.to_excel(id_num + '.xlsx', index=False)
        
        return df, name
    
    def view_results(self, data, name):
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
           plt.title(name)
           plt.grid()
           fig_path = 'D:/Xin/Program/metal/' + self.option + '/figure/factor_combination/'
           if os.path.exists(fig_path):
               os.chdir(fig_path)
           else:
               os.mkdir(fig_path)
               os.chdir(fig_path)
           plt.savefig(str(name) + '_net_values.png')
           plt.show()
   
           print('got the figure')
   
           return

# CU
cu_dic = {
    'ID00188204': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 1),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 1),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 1),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 0),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 1),
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },
    
    'ID00188207': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 1),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 1),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 1),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 0),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 1),
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },
    
    'ID00188318': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 0),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 0),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 0),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 1),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 0),
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },        
    # 分析绝对低位
    'ID00188319': {  
        ('data_value_mean_1m_pct', 0),
        ('data_value_mean_1m_pct_diff', 0),
        ('data_value_mean_1m_pct_quantile_1y', 0),
        ('data_value_mean_1m_yoy_1y', 0),
        ('data_value_mean_1m_yoy_1y_diff', 0),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 0),
        ('data_value_momentum_1m', 0),
        ('data_value_momentum_1m_quantile_1y', 0),
        ('data_value_pct', 0),
        ('data_value_pct_diff', 0),
        ('data_value_pct_quantile_1y', 0),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 0),
        ('data_value_yoy_1y', 0),
        ('data_value_yoy_1y_diff', 0),
        ('data_value_yoy_1y_quantile_1y', 0),
        ('data_value_zscore_1y', 1)
    },        
    # 上海库存之和
    'ID00188318_ID00188319': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 1),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 1),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 1),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 1),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 1),
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },            
    'ID01030223': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 1),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 1),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 1),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 1),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 1),
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },        
    
    'ID01030232': {  
        ('data_value_mean_1m_pct', 0),
        ('data_value_mean_1m_pct_diff', 0),
        ('data_value_mean_1m_pct_quantile_1y', 0),
        ('data_value_mean_1m_yoy_1y', 0),
        ('data_value_mean_1m_yoy_1y_diff', 0),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 0),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 0),
        ('data_value_pct', 0),
        ('data_value_pct_diff', 0),
        ('data_value_pct_quantile_1y', 0),
        ('data_value_quantile_1y', 0),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 1),
        ('data_value_yoy_1y', 0),
        ('data_value_yoy_1y_diff', 0),
        ('data_value_yoy_1y_quantile_1y', 0),
        ('data_value_zscore_1y', 0)
    },        
    
    'wind001': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 1),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 1),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 1),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 1),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 1),
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },   

    'wind002': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 1),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 1),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 1),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 1),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 1) ,
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },   

    'zc001': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 1),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 1),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 1),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 1),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 1),
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },   
    
    'zc002': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 1),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 1),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 1),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 1),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 1) ,
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },   
    
    }


al_dic = {
    'ID00188138': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 1),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 1),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 1),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 0),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 1),
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },

    'ID00188160': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 1),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 1),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 1),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 0),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 1),
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },

    'ID00188307': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 1),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 1),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 1),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 1),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 1),
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },

    'ID00188313': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 1),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 1),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 1),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 1),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 1),
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },

    'ID00275569': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 1),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 1),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 0),
        ('data_value_momentum_1m_quantile_1y', 0),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 1),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 1),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 1),
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },
	
    'ID01001760': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 1),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 1),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 0),
        ('data_value_momentum_1m_quantile_1y', 0),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 1),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 1),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 1),
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },

    'zc005': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 0),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 0),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 0),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 0),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 0),
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },

    'zc006': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 1),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 1),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 1),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 0),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 1),
        ('data_value_yoy_1y_quantile_1y', 0),
        ('data_value_zscore_1y', 0)
    },
}



zn_dic = {
    'ID00188145': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 1),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 1),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 1),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 0),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 1),
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },

    'ID00188329': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 0),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 0),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 0),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 1),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 0),
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },

    'ID00188330': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 0),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 0),
        ('data_value_mean_1m_yoy_1y_diff', 0),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 0),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 0),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 1),
        ('data_value_yoy_1y', 0),
        ('data_value_yoy_1y_diff', 0),
        ('data_value_yoy_1y_quantile_1y', 0),
        ('data_value_zscore_1y', 1)
    },

    'ID00259381': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 1),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 1),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 1),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 0),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 1),
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },

    'ID00408213': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 0),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 0),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 0),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 1),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 0),
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },

    'ID01001668': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 0),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 0),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 0),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 0),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 0),
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },
    
    'ID01001671': {  
        ('data_value_mean_1m_pct', 0),
        ('data_value_mean_1m_pct_diff', 0),
        ('data_value_mean_1m_pct_quantile_1y', 0),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 1),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 0),
        ('data_value_pct_diff', 0),
        ('data_value_pct_quantile_1y', 0),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 1),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 1),
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },

    'ID01030219': {  
        ('data_value_mean_1m_pct', 0),
        ('data_value_mean_1m_pct_diff', 0),
        ('data_value_mean_1m_pct_quantile_1y', 0),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 1),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 0),
        ('data_value_pct_diff', 0),
        ('data_value_pct_quantile_1y', 0),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 1),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 1),
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },

    'zc008': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 1),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 1),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 1),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 1),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 1),
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },


    'zc009': {  
        ('data_value_mean_1m_pct', 0),
        ('data_value_mean_1m_pct_diff', 0),
        ('data_value_mean_1m_pct_quantile_1y', 0),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 0),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 0),
        ('data_value_momentum_1m_quantile_1y', 0),
        ('data_value_pct', 0),
        ('data_value_pct_diff', 0),
        ('data_value_pct_quantile_1y', 0),
        ('data_value_quantile_1y', 0),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 1),
        ('data_value_ttm', 0),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 0),
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 0)
    }

}

ni_dic = {
    'ID00185680': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 1),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 1),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 1),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 0),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 1),
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },
    
        'ID00185743': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 0),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 0),
        ('data_value_mean_1m_yoy_1y_diff', 0),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 0),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 0),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 1),
        ('data_value_yoy_1y', 0),
        ('data_value_yoy_1y_diff', 0),
        ('data_value_yoy_1y_quantile_1y', 0),
        ('data_value_zscore_1y', 1)
    },
    
    
    'ID00188183': {  
        ('data_value_mean_1m_pct', 1),
        ('data_value_mean_1m_pct_diff', 1),
        ('data_value_mean_1m_pct_quantile_1y', 1),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 1),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 1),
        ('data_value_momentum_1m_quantile_1y', 1),
        ('data_value_pct', 1),
        ('data_value_pct_diff', 1),
        ('data_value_pct_quantile_1y', 1),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 0),
        ('data_value_ttm', 0),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 1),
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },
    
        'ID00188263': {  
        ('data_value_mean_1m_pct', 0),
        ('data_value_mean_1m_pct_diff', 0),
        ('data_value_mean_1m_pct_quantile_1y', 0),
        ('data_value_mean_1m_yoy_1y', 0),
        ('data_value_mean_1m_yoy_1y_diff', 0),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 0),
        ('data_value_momentum_1m', 0),
        ('data_value_momentum_1m_quantile_1y', 0),
        ('data_value_pct', 0),
        ('data_value_pct_diff', 0),
        ('data_value_pct_quantile_1y', 0),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 1),
        ('data_value_ttm', 1),
        ('data_value_yoy_1y', 0),
        ('data_value_yoy_1y_diff', 0),
        ('data_value_yoy_1y_quantile_1y', 0),
        ('data_value_zscore_1y', 1)
    },

        'ID00265540': {  
        ('data_value_mean_1m_pct', 0),
        ('data_value_mean_1m_pct_diff', 0),
        ('data_value_mean_1m_pct_quantile_1y', 0),
        ('data_value_mean_1m_yoy_1y', 1),
        ('data_value_mean_1m_yoy_1y_diff', 0),
        ('data_value_mean_1m_yoy_1y_quantile_1y', 1),
        ('data_value_momentum_1m', 0),
        ('data_value_momentum_1m_quantile_1y', 0),
        ('data_value_pct', 0),
        ('data_value_pct_diff', 0),
        ('data_value_pct_quantile_1y', 0),
        ('data_value_quantile_1y', 1),
        ('data_value_reversion_1m', 0),
        ('data_value_slope_1y', 1),
        ('data_value_ttm', 0),
        ('data_value_yoy_1y', 1),
        ('data_value_yoy_1y_diff', 0),
        ('data_value_yoy_1y_quantile_1y', 1),
        ('data_value_zscore_1y', 1)
    },



}

def main(option, factor_dic):
    cl = factor_combination(option, factor_dic)
    for key in factor_dic.keys():
        df, name = cl.single_factor_combination(key)
        cl.view_results(df, name)

    return


# main('CU', cu_dic)
# main('AL', al_dic)
main('ZN', zn_dic)
# main('NI', ni_dic)



