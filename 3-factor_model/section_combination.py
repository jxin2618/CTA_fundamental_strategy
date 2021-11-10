# -*- coding: utf-8 -*-
"""
Created on Tue Jan 12 14:03:15 2021

@author: J Xin
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from scipy import stats
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
import os
import sys
import warnings

if not sys.warnoptions:
    warnings.simplefilter("ignore")



class Weight_by_Vol(object):

    def __init__(self, option_list):
        self.option_list = option_list

    def cal_vol(self, option, df_merge):
        file = 'D:/Xin/Program/metal/' + option + '/data/output_data/category_combination/' + option + '.xlsx'
        data = pd.read_excel(file).loc[:, ['date', 'mean_pos', 'rtn', 'rtn_minus_cost', 'nav_minus_cost']]
        data.loc[:, option + '_vol_21d'] = data.loc[:, 'rtn_minus_cost'].rolling(21).std() * np.sqrt(252)
        data = data.rename(columns={'pos': option + '_pos', 'mean_pos': option + '_mean_pos', 'rtn': option + '_rtn', 'rtn_minus_cost': option + '_rtn_minus_cost', 'nav_minus_cost': option + '_nav_minus_cost'})
        if df_merge.shape[0] == 0:
            df_merge = data
        else:
            df_merge = pd.merge(df_merge, data, how='outer', on='date')

        return df_merge

    def plot_option_nav(self):
        df_merge = pd.DataFrame()
        y_dic = {}
        for option in option_list:
            df_merge = self.cal_vol(option, df_merge)
            y_dic[option] = option + '_nav_minus_cost'

        df_merge = df_merge.sort_values(by='date', ascending=True)

        fig = plt.figure()
        ax1 = fig.add_subplot(111)
        x = df_merge.loc[:, 'date'].values
        for option in option_list:
            y = df_merge.loc[:, y_dic[option]].values
            ax1.plot(x, y, label=option)
            plt.ion()
        plt.xlabel('date')
        plt.ylabel('net value')
        plt.legend()
        plt.title('before_tvs')
        plt.grid()
        plt.savefig('D:/Xin/Program/metal/portfolio/figure/options_nav.png')
        plt.show()

        return df_merge
    
    def plot_option_nav_after_tvs(self, df, col, save_path):
        y_dic = {}
        for option in self.option_list:
            y_dic[option] = option + col
        fig = plt.figure()
        ax1 = fig.add_subplot(111)
        x = df.loc[:, 'date'].values
        for option in option_list:
            y = df.loc[:, y_dic[option]].values
            ax1.plot(x, y, label=option)
            plt.ion()
        plt.xlabel('date')
        plt.ylabel('net value')
        plt.legend()
        plt.title('after_tvs')
        plt.grid()
        plt.savefig(save_path)
        plt.show()

        return

    def cal_corr(self, df):
        num = len(self.option_list)
        init_corr = pd.DataFrame(np.zeros((num, num)) * np.nan, columns=option_list, index=option_list)
        for i in range(num):
            option_i = option_list[i]
            init_corr.iloc[i, i] = 1
            rtn_i = df.loc[:, option_i + '_rtn_minus_cost'].fillna(0)
            for j in range(i + 1, num):
                option_j = self.option_list[j]
                rtn_j = df.loc[:, option_j + '_rtn_minus_cost'].fillna(0)
                init_corr.iloc[j, i] = stats.pearsonr(rtn_i, rtn_j)[0]

        return init_corr

    def rho_mean_21d(self, df):
        df.reset_index(drop=True, inplace=True)
        num_row, num_col = df.shape
        num_option = len(option_list)
        rtn_option = [x + '_rtn_minus_cost' for x in self.option_list]
        # num of options in portfolio
        tmp = pd.Series(np.zeros(df.shape[0]))
        for i in range(0, num_option):
            tmp = (pd.Series(np.ones(df.shape[0])) - df.loc[:, rtn_option[i]].isna().astype(int)) + tmp
        df.loc[:, 'num_option'] = tmp

        for i in range(20, num_row):
            sub_df = df.iloc[i - 20: i + 1, :]
            N = sub_df.loc[:, 'num_option'].values[0]
            sub_df = sub_df.loc[:, rtn_option]
            sub_corr = self.cal_corr(sub_df)
            rho_mean = (2 * (sub_corr.fillna(0).sum().sum() - num_option)) / (N * (N - 1))
            df.loc[i, 'rho_mean'] = rho_mean
            df.loc[i, 'cf'] = np.sqrt(N / (1 + (N - 1) * rho_mean))

        return df

    def single_target_vol(self, initial_df):
        vol_target_list = [0.05, 0.1, 0.15, 0.2]

        for vol_target in vol_target_list:
            df = initial_df.copy()
            for option in self.option_list:
                df.loc[:, option + '_weight'] = vol_target / (df.loc[:, 'num_option'] * df.loc[:, option + '_vol_21d'])
                df.loc[:, option + '_weight'] = df.loc[:, option + '_weight'].shift(1).fillna(0)
            df = df.fillna(0)
            tmp = pd.Series(np.zeros(df.shape[0]))
            for option in self.option_list:
                df.loc[:, option + '_rtn_after_single_tvs_' + str(vol_target)] = df.loc[:, option + '_weight'] * df.loc[:, option + '_rtn_minus_cost']
                df.loc[:, option + '_nav_after_single_tvs_' + str(vol_target)] = (1 + df.loc[:, option + '_rtn_after_single_tvs_' + str(vol_target)]).cumprod()
                tmp = tmp + df.loc[:, option + '_weight'] * df.loc[:, option + '_rtn_minus_cost']

            df.loc[:, 'target_rtn'] = tmp
            df.loc[:, 'target_nav'] = (1 + df.loc[:, 'target_rtn']).cumprod()
            vmax = np.maximum.accumulate(df.loc[:, 'target_nav'])
            df.loc[:, 'target_mdd'] = df.loc[:, 'target_nav'] / vmax - 1

            fig2 = plt.figure()
            ax2 = fig2.add_subplot(111)
            x = df.loc[:, 'date'].values
            y = df.loc[:, 'target_nav'].values
            w = df.loc[:, 'target_mdd'].values
            ax2.plot(x, y, label='target_nav', color='red')
            ax3 = ax2.twinx()
            ax3.plot(x, w, label='target_mdd', color='blue')
            plt.xlabel('date')
            plt.ylabel('net value')

            ax2.legend(loc='lower left')
            ax3.legend(loc='lower right')
            ax3.set_ylim(-0.2, 0)
            plt.title('单品种目标波动率_' + str(vol_target))
            plt.grid()
            plt.savefig('D:/Xin/Program/metal/portfolio/figure/single_tvs/单品种目标波动率_' + str(vol_target) + '.png')
            plt.show()
            df.to_excel('D:/Xin/Program/metal/portfolio/data/output_data/port/单品种目标波动率_' + str(vol_target) + '.xlsx', index=0)
            path1 = 'D:/Xin/Program/metal/portfolio/data/output_data/sharp/单品种目标波动率_sharp_ratio_' + str(vol_target) + '.xlsx'
            path2 = 'D:/Xin/Program/metal/portfolio/data/output_data/seasonal_effect/单品种目标波动率_seasonal_effect_' + str(vol_target) + '.xlsx'
            path3 = 'D:/Xin/Program/metal/portfolio/figure/seasonal_effect/单品种目标波动率_seasonal_effect_' + str(vol_target) + '.png'
            path_after_single_tvs ='D:/Xin/Program/metal/portfolio/figure/single_tvs/options_nav_after_tvs_' +  str(vol_target) + '.png'
            self.sharp_ratio(df, path1)
            self.seasonal_effect(df, path2, path3)
            self.plot_option_nav_after_tvs(df, '_nav_after_single_tvs_' + str(vol_target), path_after_single_tvs)

        return

    def portfolio_target_vol(self, initial_df):
        vol_target_list = [0.05, 0.1, 0.15, 0.2]

        for vol_target in vol_target_list:
            df = initial_df.copy()
            for option in option_list:
                df.loc[:, option + '_weight'] = vol_target / (df.loc[:, 'num_option'] * df.loc[:, option + '_vol_21d']) * df.loc[:, 'cf']
                df.loc[:, option + '_weight'] = df.loc[:, option + '_weight'].shift(1).fillna(0)
            df = df.fillna(0)
            tmp = pd.Series(np.zeros(df.shape[0]))
            for option in option_list:
                df.loc[:, option + '_rtn_after_port_tvs_' + str(vol_target)] = df.loc[:, option + '_weight'] * df.loc[:, option + '_rtn_minus_cost']
                df.loc[:, option + '_nav_after_port_tvs_' + str(vol_target)] = (1 + df.loc[:, option + '_rtn_after_port_tvs_' + str(vol_target)]).cumprod()
                tmp = tmp + df.loc[:, option + '_weight'] * df.loc[:, option + '_rtn_minus_cost']

            df.loc[:, 'target_rtn'] = tmp
            df.loc[:, 'target_nav'] = (1 + df.loc[:, 'target_rtn']).cumprod()
            vmax = np.maximum.accumulate(df.loc[:, 'target_nav'])
            df.loc[:, 'target_mdd'] = df.loc[:, 'target_nav'] / vmax - 1

            fig2 = plt.figure()
            ax2 = fig2.add_subplot(111)
            x = df.loc[:, 'date'].values
            y = df.loc[:, 'target_nav'].values
            w = df.loc[:, 'target_mdd'].values
            ax2.plot(x, y, label='target_nav', color='red')
            ax3 = ax2.twinx()
            ax3.plot(x, w, label='target_mdd', color='blue')
            plt.xlabel('date')
            plt.ylabel('net value')
          
            ax2.legend(loc='lower left')
            ax3.legend(loc='lower right')
            ax3.set_ylim(-0.2, 0)
            plt.title('组合目标波动率_' + str(vol_target))
            plt.grid()
            plt.savefig('D:/Xin/Program/metal/portfolio/figure/portfolio_tvs/组合目标波动率_' + str(vol_target) + '.png')
            plt.show()
            df.to_excel('D:/Xin/Program/metal/portfolio/data/output_data/port/组合目标波动率_' + str(vol_target) + '.xlsx', index=0)
            path1 = 'D:/Xin/Program/metal/portfolio/data/output_data/sharp/组合目标波动率_sharp_ratio_' + str(vol_target) + '.xlsx'
            path2 = 'D:/Xin/Program/metal/portfolio/data/output_data/seasonal_effect/组合目标波动率_seasonal_effect_' + str(vol_target) + '.xlsx'
            path3 = 'D:/Xin/Program/metal/portfolio/figure/seasonal_effect/组合目标波动率_seasonal_effect_' + str(vol_target) + '.png'
            path_after_port_tvs ='D:/Xin/Program/metal/portfolio/figure/portfolio_tvs/options_nav_after_tvs_' +  str(vol_target) + '.png'
            self.sharp_ratio(df, path1)
            self.seasonal_effect(df, path2, path3)
            self.plot_option_nav_after_tvs(df, '_nav_after_port_tvs_' + str(vol_target), path_after_port_tvs)
        return 

    def sharp_ratio(self, data, path):
        year_dict = {}
        year_list = ['2015', '2016', '2017', '2018', '2019', '2020']
        df = pd.DataFrame()
        for year in year_list:
            sub_data = data.loc[(data.loc[:, 'date'] >= pd.Timestamp(str(year) + '-01-01') ) & (data.loc[:, 'date'] <= pd.Timestamp(str(year) + '-12-31')),:]
            year_dict[year] = sub_data
        year_dict['total'] = data
        for keys, values in year_dict.items(): 
            days = values.shape[0]
            realized_return = np.sum(values.loc[:, 'target_rtn'].values) 
            annual_return = np.sum(values.loc[:, 'target_rtn'].values) / days * 252
            annual_volatility = np.std(values.loc[:, 'target_rtn'].values) * np.sqrt(252)
            sharp_ratio = annual_return / annual_volatility
            MaxDrawdown = np.min(values.loc[:, 'target_mdd'].values)
            calmar = annual_return / np.max(np.abs(MaxDrawdown))

            year_df = pd.DataFrame(
                {'trading_days': [days], 'realized_return': [realized_return], 'annual_return': [annual_return], 'annual_volatility': [annual_volatility], 'max_drawdown': [MaxDrawdown], 'sharp_ratio': [sharp_ratio], 'calmar': [calmar]}, index=[keys])
            
            df = pd.concat([df, year_df], axis=0)

        df.to_excel(path, index=True)

        return

    def seasonal_effect(self, data, data_path, fig_path):
        data = data.loc[:, ['date', 'target_nav']]
        date =  pd.DataFrame(pd.date_range(start='2015-09-01', end='2020-11-30'), columns=['date'])
        data = pd.merge(date, data, on='date', how='outer')        
        data = data.sort_values(by='date', ascending=True)
        data = data.fillna(method='ffill').dropna()
        
        data = data.set_index(['date'])
        df = data.resample('1M').asfreq()
        df.loc[:, 'monthly_return'] = df.iloc[:, 0].pct_change()
        df = df.reset_index()
        df.loc[:, 'month'] = df.loc[:, 'date'].apply(lambda x: x.month)
        df_median = df.loc[:, ['monthly_return', 'month']].groupby('month').median()
        
        plt.bar(df_median.index, df_median.loc[:, 'monthly_return'], color=['r', 'y', 'blue', 'g', 'orange', 'pink', 'purple', 'olive', 'rosybrown', 'violet', 'lime', 'aqua'])
        plt.xlabel('month')
        plt.ylabel('monthly_return')
        plt.xticks(list(range(1, 13)))
        plt.title('seasonal effect')
        plt.grid()
        plt.savefig(fig_path)
        plt.show()
        
        df.to_excel(data_path)
        
        return


    def run(self):
        # cal 21 days' vol and plot the net value curves of options
        df_merge  = self.plot_option_nav()
        # cal the inter-correlation of the options
        corr = self.cal_corr(df_merge)
        corr.to_excel('D:/Xin/Program/metal/portfolio/data/output_data/corr.xlsx', index=True)
        # cal the CF coefficient in Weight Average Process
        df = self.rho_mean_21d(df_merge)
        print('calculated rho_mean_21d')
        self.single_target_vol(df)
        self.portfolio_target_vol(df)
        
        return
    


def main(option_list):
    the_class = Weight_by_Vol(option_list)
    the_class.run()

    return


option_list = ['CU', 'AL', 'ZN', 'NI']
if __name__ == '__main__':
    main(option_list)





