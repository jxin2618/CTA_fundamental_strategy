# -*- coding: utf-8 -*-
"""
Created on Mon Jan 25 16:59:56 2021

@author: DamonChan
"""

import numpy as np
from pandas import Series, DataFrame

class Performance(object):

    
    @classmethod
    def cal_seasonal_performance(cls, data):
        data = data.resample('1m').last().dropna().pct_change().to_frame('rm')
        data['year'] = data.index.map(lambda x: x.year)
        data['month'] = data.index.map(lambda x: x.month)
        res = data.pivot_table(index='year', columns='month', values='rm')
        res.loc['mean'] = res.mean()
        res.loc['median'] = res.median()
        return res
        
    @classmethod
    def cal_section_nav(cls, data, start=None, end=None):
        """计算区间净值
        
        Params:
        -------
        data: series
        start: str, datetime
        end: str, datetime
        
        Returns:
        --------
        res: series
        
        """
        data = data.sort_index()
        data = data.truncate(before=start, after=end)
        data = data / data.iloc[0]
        return data
        
    @classmethod
    def cal_max_draw_down(cls, data):
        """计算最大撤回

        Params:
        -------
        data: series
            index: date
            values: cumulative net value

        Returns:
        --------
        max_draw_down: series
            draw down time series
        """
        data = data.sort_index()
        return data / data.cummax() - 1
    
    @classmethod
    def cal_max_draw_down_duration(cls, data):
        """计算最大回撤久期
        
        Params:
        -------
        data: series
        
        Returns:
        --------
        res: series
        
        """
        data = data.sort_index()
        n = 0
        pre_high = data.iloc[0]
        mddd = Series(0, index=data.index)
        for k, v in data.iteritems():
            if v < pre_high:
                n += 1
            else:
                pre_high = v
                n = 0
            mddd.loc[k] = n
        return mddd
        

    @classmethod
    def performance_matrix(cls, data, ann_factor=252):
        """计算绩效矩阵

        Params:
        -------
        data: series
            index: date
            values: cumulative net value
        ann_factor: numeric
            annualized factor
            default is 252

        Returns:
        --------
        performance: dataframe

        """
        data = data.sort_index()
        g0 = data.index.map(lambda x: x.year)
        ret = data.pct_change().dropna()
        g = ret.index.map(lambda x: x.year)
        realized_return = ret.groupby(g).apply(
              lambda x: round(
                      ((1+x).cumprod().iloc[-1]-1)*100, 2)
              ).dropna()
        annual_return = ret.groupby(g).apply(
              lambda x: round(ann_factor * x.mean()*100, 2)).dropna()
        annual_volatility = ret.groupby(g).apply(
              lambda x: round(
                      np.sqrt(ann_factor) * x.std()*100, 2)).dropna()
        annual_sharpe_ratio = ret.groupby(g).apply(
              lambda x: round(
                      np.sqrt(ann_factor) * x.mean() /
                      x.std(), 4)).dropna()
        annual_draw_down = data.groupby(g0).apply(
              lambda x: round(
                      cls.cal_max_draw_down(x).min()*100,
                      2))
        annual_calmar_ratio = round(
                annual_return / annual_draw_down.abs(), 4)
        total_realized_return = round(
                (data.iloc[-1] / data.iloc[0] - 1)*100, 2)
        total_annualized_return = round(ret.mean() * ann_factor * 100, 2)
        total_annualized_vol = round(
                ret.std() * np.sqrt(ann_factor) * 100, 2)
        total_sharpe = round(
                total_annualized_return / total_annualized_vol, 4)
        total_max_draw_down = round(cls.cal_max_draw_down(data).min() * 100, 2)
        total_calmar = round(
                total_annualized_return / abs(total_max_draw_down), 4)
        annual_df = DataFrame(
                {'realized_return': realized_return,
                 'annualized_returns': annual_return,
                 'annualized_volatility': annual_volatility,
                 'max_draw_down': annual_draw_down,
                 'sharpe': annual_sharpe_ratio,
                 'calmar': annual_calmar_ratio})
        annual_df.loc['total', :] = [
                total_realized_return, total_annualized_return,
                total_annualized_vol, total_max_draw_down, total_sharpe,
                total_calmar]
        return annual_df