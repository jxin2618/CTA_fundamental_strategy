# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 09:47:38 2019

@author: DamonChan
"""

import pandas as pd
from datapro.datepro import DatePro
from datapro.statspro import StatisticModel

class FactorDesigner(object):


    @classmethod
    def _standardized_output(cls, date, data, key, suffix, **kwargs):
        """
        
        Params:
        -------
        date: str, datetime
        data: dataframe
        key: str
        suffix: str
        
        Returns:
        --------
        res: dataframe
        
        """
        res = cls._replace_inf(data=data)
        res = cls._add_suffix(
                data=res,
                key=key,
                suffix=suffix, **kwargs).reset_index()
        res['date'] = date
        res = cls._replace_inf(data=res.set_index(['date', 'sec_code']))
        return res
        
    @classmethod
    def _date_match(cls, date, **kwargs):
        """
        
        Params:
        -------
        date: str, datetime
        **kwargs : parameters in  dateutil.relativedelta 
        
        Returns:
        --------
        res: datetime
        
        """
        return DatePro.date_shift(data=date, **kwargs)
    
    @classmethod
    def _section_select(cls, left, right, data, calendar):
        """
        
        Params:
        -------
        left: str, datetime
        right: str, datetime
        data: dataframe
        calendar: dataframe
        
        Returns:
        --------
        res: dataframe
        
        """
        left_date = calendar[calendar.date==left
                            ].date.iloc[0]
        right_date = calendar[calendar.date==right
                            ].date.iloc[0]
        res = data.reset_index()
        res = res[(res.date>=left_date)&(res.date<=right_date)]
        return res.set_index(['date', 'sec_code'])
    
    @classmethod
    def _suffix(cls, **kwargs):
        res = []
        for i in ['years', 'months', 'weeks', 'days']:
            iv = kwargs.get(i)
            if iv:
                res.append(str(abs(iv))+i[0])
        return ''.join(res)
    
    @classmethod
    def _add_suffix(cls, data, key, suffix, **kwargs):
        rename = {i: "{i}_{k}_{s}".format(i=i, k=key, s=suffix)
            for i in data.columns}
        res = data.rename(columns=rename) if kwargs.get('add_suffix', True) \
            else data
        return res
    
    @classmethod
    def _replace_inf(cls, data):
        """
        
        Params:
        -------
        data: dataframe
        
        Returns:
        --------
        res: dataframe
        
        """
        res = data.replace(
                [pd.np.inf, -pd.np.inf],
                [pd.np.nan, pd.np.nan])
        return res

    @classmethod
    def pre(cls, *args, date, data, period, calendar, **kwargs):
        """Factor function pre.
        
        Params:
        -------
        date: str, datetime
        data: dataframe
            * index: ['date', 'sec_code']
            * columns: raw data names
        period: dict
            keys: years, months, weeks, days
            values: int
                {'years': -1, 'months': -1} means 1y1m before
        calendar: dataframe
        
        AlternativeParams:
        ------------------
        add_suffix: boolean
            default is True
        
        Returns:
        --------
        res: dataframe
        
        """
        key = 'pre'
        suffix = cls._suffix(**period)
        pre_date = cls._date_match(date=date, **period)
        pre_current_date = calendar[calendar.date==pre_date
                            ].current_trading_date.iloc[0]

        res = data.xs(
                pre_current_date,
                level='date')
        res = cls._standardized_output(
                date=date, data=res,
                key=key, suffix=suffix, **kwargs)
        return res

    @classmethod
    def stable_point(cls, *args, date, data, period, calendar, left_extend,
                     right_extend, **kwargs):
        """Factor function stablepoint.
        
        Params:
        -------
        date: str, datetime
        data: dataframe
            * index: ['date', 'sec_code']
            * columns: raw data names
        period: dict
            keys: years, months, weeks, days
            values: int
                {'years': -1, 'months': -1} means 1y1m before
        calendar: dataframe
        left_extend: dict
            keys: years, months, weeks, days
            values: int
        right_extend: dict
            keys: years, months, weeks, days
            values: int
        
        AlternativeParams:
        ------------------
        add_suffix: boolean
            default is True
        
        Returns:
        --------
        res: dataframe
        
        """
        key = 'stablepoint'
        suffix = cls._suffix(**period)
        mid_date = cls._date_match(date=date, **period)
        left_date = cls._date_match(date=mid_date, **left_extend)
        right_date = cls._date_match(date=mid_date, **right_extend)
        res = cls._section_select(
                left=left_date, right=right_date,
                data=data, calendar=calendar)
        res = res.groupby('sec_code').mean()
        res = cls._standardized_output(
                date=date, data=res,
                key=key, suffix=suffix, **kwargs)
        return res
    
    @classmethod
    def momentum(cls, *args, date, data, period, calendar, **kwargs):
        """Factor function momentum.
        
        Params:
        -------
        date: str, datetime
        data: dataframe
            * index: ['date', 'sec_code']
            * columns: raw data names
        period: dict
            keys: years, months, weeks, days
            values: int
                {'years': -1, 'months': -1} means 1y1m before
        calendar: dataframe
        
        AlternativeParams:
        ------------------
        add_suffix: boolean
            default is True
        
        Returns:
        --------
        res: dataframe
        
        """
        key = 'momentum'
        suffix = cls._suffix(**period)
        pre_data = cls.pre(
                date=date, data=data, period=period,
                calendar=calendar, add_suffix=False)
        res = data.xs(date, level='date').divide(pre_data.xs(date, level='date').fillna(pd.np.nan), fill_value=0) - 1
        res = cls._standardized_output(
                date=date, data=res,
                key=key, suffix=suffix, **kwargs)
        return res
    
    @classmethod
    def sign(cls, *args, date, data, period, calendar, **kwargs):
        """Factor function sign.
        
        Params:
        -------
        date: str, datetime
        data: dataframe
            * index: ['date', 'sec_code']
            * columns: raw data names
        period: dict
            keys: years, months, weeks, days
            values: int
                {'years': -1, 'months': -1} means 1y1m before
        calendar: dataframe
        
        AlternativeParams:
        ------------------
        add_suffix: boolean
            default is True
        
        Returns:
        --------
        res: dataframe
        
        """
        key = 'sign'
        suffix = cls._suffix(**period)
        pre_date = cls._date_match(date=date, **period)
        pre_current_date = calendar[calendar.date==pre_date
                            ].current_trading_date.iloc[0]
        res = cls._section_select(
                left=pre_current_date, right=date,
                data=data, calendar=calendar)
        res = res.where(res>=0, -1).where(res<=0, 1).groupby('sec_code').apply(
                lambda x: x.sum()/x.index.size)
        res = cls._standardized_output(
                date=date, data=res,
                key=key, suffix=suffix, **kwargs)
        return res
    
    @classmethod
    def slope(cls, *args, date, data, period, calendar, **kwargs):
        """Factor function slope.
        
        Params:
        -------
        date: str, datetime
        data: dataframe
            * index: ['date', 'sec_code']
            * columns: raw data names
        period: dict
            keys: years
            values: int
                {'years': -1} means 1y1m before
        calendar: dataframe
        
        AlternativeParams:
        ------------------
        add_suffix: boolean
            default is True
        
        Returns:
        --------
        res: dataframe
        
        """
        key = 'slope'
        suffix = cls._suffix(**period)
        pre_date = cls._date_match(date=date, **period)
        pre_current_date = calendar[calendar.date==pre_date
                            ].current_trading_date.iloc[0]
        section = cls._section_select(
                left=pre_current_date, right=date,
                data=data, calendar=calendar)
        res = section.apply(
                lambda x: x.groupby('sec_code').apply(
                        lambda y: StatisticModel.wls(
                                x=pd.np.arange(y.index.size)+1,
                                y=y.sort_index())),
                axis=0).applymap(
                        lambda x: x.params.x1
                        if 'x1' in x.params.index else pd.np.nan)
        res = cls._standardized_output(
                date=date, data=res,
                key=key, suffix=suffix, **kwargs)
        return res
        
    @classmethod
    def reversion(cls, *args, date, data, period, calendar, **kwargs):
        """Factor function reversion.
        
        Params:
        -------
        date: str, datetime
        data: dataframe
            * index: ['date', 'sec_code']
            * columns: raw data names
        period: dict
            keys: years, months, weeks, days
            values: int
                {'years': -1, 'months': -1} means 1y1m before
        calendar: dataframe
        
        AlternativeParams:
        ------------------
        add_suffix: boolean
            default is True
        
        Returns:
        --------
        res: dataframe
        
        """
        key = 'reversion'
        suffix = cls._suffix(**period)
        res = -cls.momentum(date=date, data=data, period=period,
                            calendar=calendar, add_suffix=False)
        res = cls._standardized_output(
                date=date, data=res,
                key=key, suffix=suffix, **kwargs)
        return res
    
    @classmethod
    def value(cls, *args, date, data, period, calendar,
              left_extend, right_extend,**kwargs):
        """Factor function value.
        
        Params:
        -------
        date: str, datetime
        data: dataframe
            * index: ['date', 'sec_code']
            * columns: raw data names
        period: dict
            keys: years, months, weeks, days
            values: int
                {'years': -1, 'months': -1} means 1y1m before
        calendar: dataframe
        left_extend: dict
            keys: years, months, weeks, days
            values: int
        right_extend: dict
            keys: years, months, weeks, days
            values: int
        
        AlternativeParams:
        ------------------
        add_suffix: boolean
            default is True
        
        Returns:
        --------
        res: dataframe
        
        """
        key = 'value'
        suffix = cls._suffix(**period)
        stable_point = cls.stable_point(
                date=date, data=data, period=period,
                calendar=calendar, left_extend=left_extend,
                right_extend=right_extend, add_suffix=False)
        res = stable_point.xs(
                date, level='date').divide(
                        data.xs(date, level='date').fillna(pd.np.nan),
                        fill_value=0) - 1
        res = cls._standardized_output(
                date=date, data=res,
                key=key, suffix=suffix, **kwargs)
        return res

    @classmethod
    def cum_sum(cls, *args, date, data, period, calendar, **kwargs):
        """Factor function cumsum.
        
        Params:
        -------
        date: str, datetime
        data: dataframe
            * index: ['date', 'sec_code']
            * columns: raw data names
        period: dict
            keys: years, months, weeks, days
            values: int
                {'years': -1, 'months': -1} means 1y1m before
        calendar: dataframe
        
        AlternativeParams:
        ------------------
        add_suffix: boolean
            default is True
        
        Returns:
        --------
        res: dataframe
        
        """
        key = 'cumsum'
        suffix = cls._suffix(**period)
        pre_date = cls._date_match(date=date, **period)
        pre_current_date = calendar[calendar.date==pre_date
                            ].current_trading_date.iloc[0]
        res = cls._section_select(
                left=pre_current_date, right=date,
                data=data, calendar=calendar)
        res = res.groupby('sec_code').sum()
        res = cls._standardized_output(
                date=date, data=res,
                key=key, suffix=suffix, **kwargs)
        return res

    @classmethod
    def annual_cum_sum(cls, *args, data, **kwargs):
        """Factor function annual-cumsum.
        
        Params:
        -------
        date: str, datetime
        data: dataframe
            * index: ['date', 'sec_code']
            * columns: raw data names
        period: dict
            keys: years, months, weeks, days
            values: int
                {'years': -1, 'months': -1} means 1y1m before
        calendar: dataframe
        
        AlternativeParams:
        ------------------
        add_suffix: boolean
            default is True
        
        Returns:
        --------
        res: dataframe
        
        """
        def _local_cumsum(x):
            return x.sort_index().cumsum()
        data = data.reset_index()
        data['anremark'] = data.date.apply(lambda x: x.year)
        res = data.set_index(['date', 'sec_code', 'anremark']).groupby('anremark').apply(_local_cumsum)
        res = res.reset_index('anremark', drop=True)
        return res

    @classmethod
    def cum_prod(cls, *args, date, data, period, calendar, **kwargs):
        """Factor function cumprod.
        
        Params:
        -------
        date: str, datetime
        data: dataframe
            * index: ['date', 'sec_code']
            * columns: raw data names
        period: dict
            keys: years, months, weeks, days
            values: int
                {'years': -1, 'months': -1} means 1y1m before
        calendar: dataframe
        
        AlternativeParams:
        ------------------
        add_suffix: boolean
            default is True
        
        Returns:
        --------
        res: dataframe
        
        """
        key = 'cumprod'
        suffix = cls._suffix(**period)
        pre_date = cls._date_match(date=date, **period)
        pre_current_date = calendar[calendar.date==pre_date
                            ].current_trading_date.iloc[0]
        res = cls._section_select(
                left=pre_current_date, right=date,
                data=data, calendar=calendar)
        res = res.groupby('sec_code').apply(
                lambda x: x.sort_index().cumprod()).xs(date, level='date')
        res = cls._standardized_output(
                date=date, data=res,
                key=key, suffix=suffix, **kwargs)
        return res
    
    @classmethod
    def mean(cls, *args, date, data, period, calendar, **kwargs):
        """Factor function mean.
        
        Params:
        -------
        date: str, datetime
        data: dataframe
            * index: ['date', 'sec_code']
            * columns: raw data names
        period: dict
            keys: years, months, weeks, days
            values: int
                {'years': -1, 'months': -1} means 1y1m before
        calendar: dataframe
        
        AlternativeParams:
        ------------------
        add_suffix: boolean
            default is True
        
        Returns:
        --------
        res: dataframe
        
        """
        key = 'mean'
        suffix = cls._suffix(**period)
        pre_date = cls._date_match(date=date, **period)
        pre_current_date = calendar[calendar.date==pre_date
                            ].current_trading_date.iloc[0]
        res = cls._section_select(
                left=pre_current_date, right=date,
                data=data, calendar=calendar)
        res = res.groupby('sec_code').mean()
        res = cls._standardized_output(
                date=date, data=res,
                key=key, suffix=suffix, **kwargs)
        return res
    
    @classmethod
    def volatility(cls, *args, date, data, period, calendar, **kwargs):
        """Factor function volatility.
        
        Params:
        -------
        date: str, datetime
        data: dataframe
            * index: ['date', 'sec_code']
            * columns: raw data names
        period: dict
            keys: years, months, weeks, days
            values: int
                {'years': -1, 'months': -1} means 1y1m before
        calendar: dataframe
        
        AlternativeParams:
        ------------------
        add_suffix: boolean
            default is True
        
        Returns:
        --------
        res: dataframe
        
        """
        key = 'volatility'
        suffix = cls._suffix(**period)
        pre_date = cls._date_match(date=date, **period)
        pre_current_date = calendar[calendar.date==pre_date
                            ].current_trading_date.iloc[0]
        res = cls._section_select(
                left=pre_current_date, right=date,
                data=data, calendar=calendar)
        res = res.groupby('sec_code').std()
        res = cls._standardized_output(
                date=date, data=res,
                key=key, suffix=suffix, **kwargs)
        return res
    
    @classmethod
    def skewness(cls, *args, date, data, period, calendar, **kwargs):
        """Factor function skewness.
        
        Params:
        -------
        date: str, datetime
        data: dataframe
            * index: ['date', 'sec_code']
            * columns: raw data names
        period: dict
            keys: years, months, weeks, days
            values: int
                {'years': -1, 'months': -1} means 1y1m before
        calendar: dataframe
        
        AlternativeParams:
        ------------------
        add_suffix: boolean
            default is True
        
        Returns:
        --------
        res: dataframe
        
        """
        key = 'skewness'
        suffix = cls._suffix(**period)
        pre_date = cls._date_match(date=date, **period)
        pre_current_date = calendar[calendar.date==pre_date
                            ].current_trading_date.iloc[0]
        res = cls._section_select(
                left=pre_current_date, right=date,
                data=data, calendar=calendar)
        res = res.groupby('sec_code').skew()
        res = cls._standardized_output(
                date=date, data=res,
                key=key, suffix=suffix, **kwargs)
        return res
    
    @classmethod
    def kurtosis(cls, *args, date, data, period, calendar, **kwargs):
        """Factor function kurtosis.
        
        Params:
        -------
        date: str, datetime
        data: dataframe
            * index: ['date', 'sec_code']
            * columns: raw data names
        period: dict
            keys: years, months, weeks, days
            values: int
                {'years': -1, 'months': -1} means 1y1m before
        calendar: dataframe
        
        AlternativeParams:
        ------------------
        add_suffix: boolean
            default is True
        
        Returns:
        --------
        res: dataframe
        
        """
        key = 'kurtosis'
        suffix = cls._suffix(**period)
        pre_date = cls._date_match(date=date, **period)
        pre_current_date = calendar[calendar.date==pre_date
                            ].current_trading_date.iloc[0]
        res = cls._section_select(
                left=pre_current_date, right=date,
                data=data, calendar=calendar)
        res = res.groupby('sec_code').apply(lambda x: x.kurtosis())
        res = cls._standardized_output(
                date=date, data=res,
                key=key, suffix=suffix, **kwargs)
        return res
    
    @classmethod
    def yoy(cls, *args, date, data, period, calendar, **kwargs):
        """Factor function yoy.
        
        Params:
        -------
        date: str, datetime
        data: dataframe
            * index: ['date', 'sec_code']
            * columns: raw data names
        period: dict
            keys: years
            values: int
                {'years': -1} means 1y1m before
        calendar: dataframe
        
        AlternativeParams:
        ------------------
        add_suffix: boolean
            default is True
        
        Returns:
        --------
        res: dataframe
        
        """
        key = 'yoy'
        period = {'years': period.get('years')}
        suffix = cls._suffix(**period)
        res = cls.momentum(date=date, data=data, period=period,
                           calendar=calendar, add_suffix=False)
        res = cls._standardized_output(
                date=date, data=res,
                key=key, suffix=suffix, **kwargs)
        return res
    
    @classmethod
    def zscore(cls, *args, date, data, period, calendar, **kwargs):
        """Factor function zscore.
        
        Params:
        -------
        date: str, datetime
        data: dataframe
            * index: ['date', 'sec_code']
            * columns: raw data names
        period: dict
            keys: years
            values: int
                {'years': -1} means 1y1m before
        calendar: dataframe
        
        AlternativeParams:
        ------------------
        add_suffix: boolean
            default is True
        
        Returns:
        --------
        res: dataframe
        
        """
        key = 'zscore'
        suffix = cls._suffix(**period)
        mean = cls.mean(date=date, data=data, period=period,
                        calendar=calendar, add_suffix=False).xs(
                                date, level='date')
        std = cls.volatility(date=date, data=data, period=period,
                        calendar=calendar, add_suffix=False).xs(
                                date, level='date')
        res = data.xs(date, level='date').sub(mean, fill_value=0).divide(
                std, fill_value=0)
        res = cls._standardized_output(
                date=date, data=res,
                key=key, suffix=suffix, **kwargs)
        return res
    
    @classmethod
    def rank(cls, *args, date, data, period, calendar, **kwargs):
        """Factor function rank.
        
        Params:
        -------
        date: str, datetime
        data: dataframe
            * index: ['date', 'sec_code']
            * columns: raw data names
        period: dict
            keys: years
            values: int
                {'years': -1} means 1y1m before
        calendar: dataframe
        
        AlternativeParams:
        ------------------
        add_suffix: boolean
            default is True
        
        Returns:
        --------
        res: dataframe
        
        """
        key = 'rank'
        suffix = cls._suffix(**period)
        pre_date = cls._date_match(date=date, **period)
        pre_current_date = calendar[calendar.date==pre_date
                            ].current_trading_date.iloc[0]
        section = cls._section_select(
                left=pre_current_date, right=date,
                data=data, calendar=calendar)
        res = section.groupby('sec_code').rank().xs(date, level='date')
        res = cls._standardized_output(
                date=date, data=res,
                key=key, suffix=suffix, **kwargs)
        return res
    
    @classmethod
    def size(cls, *args, date, data, period, calendar, **kwargs):
        """Factor function size.
        
        Params:
        -------
        date: str, datetime
        data: dataframe
            * index: ['date', 'sec_code']
            * columns: raw data names
        period: dict
            keys: years
            values: int
                {'years': -1} means 1y1m before
        calendar: dataframe
        ignore_na: boolean
        
        AlternativeParams:
        ------------------
        add_suffix: boolean
            default is True
        
        Returns:
        --------
        res: dataframe
        
        """
        key = 'size'
        suffix = cls._suffix(**period)
        pre_date = cls._date_match(date=date, **period)
        pre_current_date = calendar[calendar.date==pre_date
                            ].current_trading_date.iloc[0]
        section = cls._section_select(
                left=pre_current_date, right=date,
                data=data, calendar=calendar)
        res = section.groupby('sec_code').count()
        res = cls._standardized_output(
                date=date, data=res,
                key=key, suffix=suffix, **kwargs)
        return res
    
    @classmethod
    def quantile(cls, *args, date, data, period, calendar, **kwargs):
        """Factor function quantile.
        
        Params:
        -------
        date: str, datetime
        data: dataframe
            * index: ['date', 'sec_code']
            * columns: raw data names
        period: dict
            keys: years
            values: int
                {'years': -1} means 1y1m before
        calendar: dataframe
        
        AlternativeParams:
        ------------------
        add_suffix: boolean
            default is True
        
        Returns:
        --------
        res: dataframe
        
        """
        key = 'quantile'
        suffix = cls._suffix(**period)
        rank = cls.rank(date=date, data=data, period=period,
                        calendar=calendar, add_suffix=False).xs(
                                date, level='date')
        size = cls.size(date=date, data=data, period=period,
                        calendar=calendar, add_suffix=False).xs(
                                date, level='date')
        res = rank.divide(size, fill_value=0)
        res = cls._standardized_output(
                date=date, data=res,
                key=key, suffix=suffix, **kwargs)
        return res
    
    @classmethod
    def combination(cls, *args, date, data, period, calendar, **kwargs):
        pass
        

# EOF
