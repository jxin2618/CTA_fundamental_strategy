# -*- coding: utf-8 -*-
"""
Created on Sat Dec  9 18:44:03 2017

@author: ciaciaciu
"""

from .typepro import TypePro
from dateutil.relativedelta import relativedelta
import pandas as pd
import calendar
import datetime

class DatePro(object):
    
    _default_period = 2
    _default_basis = 0
    
    @classmethod
    def get_natural_date(
            cls, start=None, end=None, periods=None, freq='d', tz=None, 
            normalize=False, name=None, closed=None, lastrule=0):
        """ Get natural date .
        
        Params:
        ------
        start : string or datetime-like, default None
            Left bound for generating dates
        end : string or datetime-like, default None
            Right bound for generating dates
        periods : integer or None, default None
            If None, must specify start and end
        freq : string or DateOffset, default 'D' (calendar daily)
            Frequency strings can have multiples, e.g. '5H'
        tz : string or None
            Time zone name for returning localized DatetimeIndex, for example
        Asia/Hong_Kong
        normalize : bool, default False
            Normalize start/end dates to midnight before generating date range
        name : str, default None
            Name of the resulting index
        closed : string or None, default None
            Make the interval closed with respect to the given frequency to
            the 'left', 'right', or both sides (None)
        lastrule : int, valied values {0, 1}, default is 0
            * 0 : last day of last month is the actual last day of the month
            * 1 : last day of last month is the input end
        
        Returns:
        --------
        series
        
        """
        if ('m' in freq.lower())&(lastrule==0):
            end = (TypePro.to_date_string(end)
                   if TypePro.check_datetime(end) 
                   else end)
            end = pd.to_datetime(end.replace('-','')[:-2]+'01') + \
                        relativedelta(months=1)
        data = pd.Series(
            pd.date_range(
                start=start, end=end, period=periods, freq=freq, 
                tz=tz, normalize=normalize, name=name, closed=closed)
            )
        if lastrule==1:
            data = data.append(pd.Series(pd.to_datetime(end)),
                               ignore_index=True)
        return data.drop_duplicates()

    @classmethod
    def date_shift(cls, data, **kwargs):
        """Shift date .
        
        Params:
        ------
        data : str, datetime, list, series, dataframe
        **kwargs : parameters in  dateutil.relativedelta 
            
        Returns:
        --------
        datetime, list, series, dataframe
                
        """
        data = TypePro.to_datetime(data)
        data = TypePro.apply_map(
            data=data, func=lambda x:x+relativedelta(**kwargs))
        return data

    @classmethod
    def date_diff(cls, start, end, basis=_default_basis):
        """Diff of days between start and end.
        
        Params:
        -------
        start : str, datetime, list, series
        end : str, datetime, list, series
        basis : int, list, series, default is None
            the day-count basis
            * 0 - actual/actual (default)
            * 1 - 30/360 SIA
            * 2 - actual/360
            * 3 - actual/365
            * 4 - 30/360 PSA
            * 5 - 30/360 ISDA
            * 6 - 30/360 European
            * 7 - actual/365 Japanese
            * 8 - act/act ISMA
            * 9 - act/360 ISMA
            * 10 - act/365 ISMA
            * 11 - 30/360 ISMA
            * 12 - actual/365 ISDA
            Default is 0.
        
        Returns:
        --------
        single value or series
            if len(daysperiod) == 1 returns single value else series
            
        """
        ddict={
            'start': start,
            'end': end,
            'basis': basis}
        df = TypePro.to_frame_frome_dict(ddict)
        df.basis.fillna(cls._default_basis, inplace=True)
        start = df.start
        end = df.end
        basis = df.basis
        daysdiff = pd.Series(index=df.index)
        id0 = basis.isin([0, 2, 3, 8, 9, 10, 12])
        if id0.any():
            daysdiff[id0] = cls.date_diff_act(start[id0], end[id0])
        id1 = (basis==1)
        if id1.any():
            daysdiff[id1] = cls.date_diff_360_SIA(start[id1], end[id1])
        id2 = (basis==4)
        if id2.any():
            daysdiff[id2] = cls.date_diff_360_PSA(start[id2], end[id2])
        id3 = (basis==5)
        if id3.any():
            daysdiff[id3] = cls.date_diff_360_ISDA(start[id3], end[id3])
        id4 = basis.isin([6, 11])
        if id4.any():
            daysdiff[id4] = cls.date_diff_360_european(start[id4], end[id4])
        id5 = (basis==7)
        if id5.any():
            daysdiff[id5] = cls.date_diff_365(start[id5], end[id5])
        return daysdiff if daysdiff.index.size>1 else daysdiff.values[0]

    @classmethod
    def date_diff_act(cls, start, end):
        """Actual number of days between start and end .
        
        Params:
        -------
        start : str, datetime, list, series
        end : str, datetime, list, series
        
        Returns:
        --------
        single value or series
            if len(daysperiod) == 1 returns single value else series
        
        """
        ddict={
            'start': TypePro.to_datetime(start),
            'end': TypePro.to_datetime(end)}
        df = TypePro.to_frame_frome_dict(ddict)
        daysdiff = df.end - df.start
        daysdiff = daysdiff.astype('timedelta64[D]')
        return daysdiff if daysdiff.index.size>1 else daysdiff.values[0]

    @classmethod
    def date_diff_360_SIA(cls, start, end):
        """Number between start and end based on a 360 day year .
            (SIA compliant)
            
        Params:
        -------
        start : str, datetime, list, series
        end : str, datetime, list, series
        
        Returns:
        --------
        single value or series
                if len(daysperiod) == 1 returns single value else series
        
        """
        ddict={
            'start': TypePro.to_datetime(start),
            'end': TypePro.to_datetime(end)}
        df = TypePro.to_frame_frome_dict(ddict)
        start = df.start
        end = df.end
        sVector = start.apply(lambda x:(x.year, x.month, x.day))
        eVector = end.apply(lambda x:(x.year, x.month, x.day))
        sFeblast = start.apply(
            lambda x:(x.month==2)&((calendar.isleap(x.year)&
            (x.day==29)))|(((~calendar.isleap(x.year))&(x.day==28))))
        eFeblast = end.apply(
            lambda x:(x.month==2)&((calendar.isleap(x.year)&
            (x.day==29))|((~calendar.isleap(x.year))&(x.day==28))))
        eVector[(sFeblast&eFeblast)==1] = end[(sFeblast&eFeblast)==1].apply(
                            lambda x:(x.year, x.month, 30))
        sVector[sFeblast==1] = start[sFeblast==1].apply(
                            lambda x:(x.year, x.month, 30))
        sOtherlast = start.apply(lambda x:x.day)
        eOtherlast = end.apply(lambda x:x.day)
        sOtherlast = sOtherlast>=30
        eOtherlast = (eOtherlast==31)&sOtherlast
        eVector[eOtherlast==1] = end[eOtherlast==1].apply(
                            lambda x:(x.year, x.month, 30))
        sVector[sOtherlast==1] = start[sOtherlast==1].apply(
                            lambda x:(x.year, x.month, 30))
        sVector = pd.DataFrame(sVector.tolist(), index=sVector.index)
        eVector = pd.DataFrame(eVector.tolist(), index=eVector.index)
        daysdiff = ((eVector-sVector)*[360,30,1]).sum(axis=1)
        return daysdiff if daysdiff.index.size>1 else daysdiff.values[0]

    @classmethod
    def date_diff_360_PSA(cls, start, end):
        """Number between start and end based on a 360 day year .
            (PSA compliant)
            
        Params:
        -------
        start : str, datetime, list, series
        end : str, datetime, list, series
        
        Returns:
        --------
        single value or series
            if len(daysperiod) == 1 returns single value else series
        
        """
        ddict={
            'start': TypePro.to_datetime(start),
            'end': TypePro.to_datetime(end)}
        df = TypePro.to_frame_frome_dict(ddict)
        start = df.start
        end = df.end
        sVector = start.apply(lambda x:(x.year, x.month, x.day))
        eVector = end.apply(lambda x:(x.year, x.month, x.day))
        sFeblast = start.apply(
            lambda x:(x.month==2)&((calendar.isleap(x.year)&
            (x.day==29))|((~calendar.isleap(x.year))&(x.day==28))))
        sOtherlast = start.apply(lambda x:x.day)
        sOtherlast = sOtherlast>=30
        sVector[(sOtherlast==1)|(sFeblast==1)] = (
            start[(sOtherlast==1)|(sFeblast==1)].apply(
                lambda x:(x.year, x.month, 30)))
        eOtherlast = end.apply(lambda x:x.day)
        eOtherlast = (eOtherlast==31)&((sOtherlast==1)|(sFeblast==1))
        eVector[eOtherlast==1] = end[eOtherlast==1].apply(
                            lambda x:(x.year, x.month, 30))
        sVector = pd.DataFrame(sVector.tolist(), index=sVector.index)
        eVector = pd.DataFrame(eVector.tolist(), index=eVector.index)
        daysdiff = ((eVector-sVector)*[360,30,1]).sum(axis=1)
        daysdiff[start==end] = 0
        return daysdiff if daysdiff.index.size>1 else daysdiff.values[0]

    @classmethod
    def date_diff_360_ISDA(cls, start, end):
        """Number between start and end based on a 360 day year .
            (ISDA compliant)
            
        Params:
        -------
        sstart : str, datetime, list, series
        end : str, datetime, list, series
        
        Returns:
        --------
        single value or series
            if len(daysperiod) == 1 returns single value else series
        
        """
        ddict={
            'start': TypePro.to_datetime(start),
            'end': TypePro.to_datetime(end)}
        df = TypePro.to_frame_frome_dict(ddict)
        start = df.start
        end = df.end
        sVector = start.apply(lambda x:(x.year, x.month, x.day))
        eVector = end.apply(lambda x:(x.year, x.month, x.day))
        sOtherlast = start.apply(lambda x:x.day)
        sOtherlast = sOtherlast>=30
        sVector[sOtherlast==1] = start[sOtherlast==1].apply(
                            lambda x:(x.year, x.month, 30))
        eOtherlast = end.apply(lambda x:x.day)
        eOtherlast = (eOtherlast==31)&(sOtherlast==1)
        eVector[eOtherlast==1] = end[eOtherlast==1].apply(
                            lambda x:(x.year, x.month, 30))
        sVector = pd.DataFrame(sVector.tolist(), index=sVector.index)
        eVector = pd.DataFrame(eVector.tolist(), index=eVector.index)
        daysdiff = ((eVector-sVector)*[360,30,1]).sum(axis=1)
        return daysdiff if daysdiff.index.size>1 else daysdiff.values[0]

    @classmethod
    def date_diff_360_european(cls, start, end):
        """Number between start and end based on a 360 day year .
            (European)
            
        Params:
        -------
        start : str, datetime, list, series
        end : str, datetime, list, series
        
        Returns:
        --------
        single value or series
            if len(daysperiod) == 1 returns single value else series
        
        """
        ddict={
            'start': TypePro.to_datetime(start),
            'end': TypePro.to_datetime(end)}
        df = TypePro.to_frame_frome_dict(ddict)
        start = df.start
        end = df.end
        sVector = start.apply(lambda x:(x.year, x.month, x.day))
        eVector = end.apply(lambda x:(x.year, x.month, x.day))
        sOtherlast = start.apply(lambda x:x.day)
        sOtherlast = sOtherlast==31
        eOtherlast = end.apply(lambda x:x.day)
        eOtherlast = eOtherlast==31
        sVector[sOtherlast==1] = start[sOtherlast==1].apply(
                            lambda x:(x.year, x.month, 30))
        eVector[eOtherlast==1] = end[eOtherlast==1].apply(
                            lambda x:(x.year, x.month, 30))
        sVector = pd.DataFrame(sVector.tolist(), index=sVector.index)
        eVector = pd.DataFrame(eVector.tolist(), index=eVector.index)
        daysdiff = ((eVector-sVector)*[360,30,1]).sum(axis=1)
        return daysdiff if daysdiff.index.size>1 else daysdiff.values[0]

    @classmethod
    def date_diff_365(cls, start, end):
        """Number between start and end based on a 365 day year .
            
        Params:
        -------
        start : str, datetime, list, series
        end : str, datetime, list, series
        
        Returns:
        --------
        single value or series
            if len(daysperiod) == 1 returns single value else series
        
        """
        ddict={
            'start': TypePro.to_datetime(start),
            'end': TypePro.to_datetime(end)}
        df = TypePro.to_frame_frome_dict(ddict)
        start = df.start
        end = df.end
        daytotal = [0,31,59,90,120,151,181,212,243,273,304,334]
        sVector = start.apply(lambda x:(x.year, daytotal[x.month-1], x.day))
        eVector = end.apply(lambda x:(x.year, daytotal[x.month-1], x.day))
        sVector = pd.DataFrame(sVector.tolist(), index=sVector.index)
        eVector = pd.DataFrame(eVector.tolist(), index=eVector.index)
        daysdiff = ((eVector-sVector)*[365,1,1]).sum(axis=1)
        return daysdiff if daysdiff.index.size>1 else daysdiff.values[0]

    @classmethod
    def date_diff_period(cls, start, end,
                         period=_default_period, basis=_default_basis):
        """Number of days during coupon period .
        Params:
        -------
        start : str, datetime, list, series
        end : str, datetime, list, series
        period : int, list, series
            Default is 0.
            if negative, period=2
        basis : int, list, series, default is None
            the day-count basis
            * 0 - actual/actual (default)
            * 1 - 30/360 SIA
            * 2 - actual/360
            * 3 - actual/365
            * 4 - 30/360 PSA
            * 5 - 30/360 ISDA
            * 6 - 30/360 European
            * 7 - actual/365 Japanese
            * 8 - act/act ISMA
            * 9 - act/360 ISMA
            * 10 - act/365 ISMA
            * 11 - 30/360 ISMA
            * 12 - actual/365 ISDA
            Default is 0.
            warning: if basis in [0, 8, 12] returns date_diff_act
        
        Returns:
        --------
        single value or series
            if len(daysperiod) == 1 returns single value else series
        
        """
        ddict={
            'start': start,
            'end': end,
            'period': period, 'basis': basis}
        df = TypePro.to_frame_frome_dict(ddict)
        df.period.fillna(cls._default_period, inplace=True)
        df.loc[df.period<0, 'period'] = cls._default_period
        df.basis.fillna(cls._default_basis, inplace=True)
        daysperiod = pd.Series(index=df.index)
        id0 = df.basis.isin([0, 8, 12])
        if id0.any():
            daysperiod[id0] = cls.date_diff_act(df.start[id0], df.end[id0])
        id1 = df.basis.isin([1, 2, 4, 5, 6, 9, 11])
        if id1.any():
            daysperiod[id1] = 360/df.period[id1]
        id2 = df.basis.isin([3, 7, 10])
        if id2.any():
            daysperiod[id2] = 365/df.period[id2]
        return daysperiod if daysperiod.index.size>1 else daysperiod.values[0]

    @classmethod
    def date_num_year(cls, date, basis=_default_basis):
        """Number of days in year .
        
        Params:
        -------
        date : str, datetime, list, series
        basis : int, list, series, default is None
            the day-count basis
            * 0 - actual/actual (default)
            * 1 - 30/360 SIA
            * 2 - actual/360
            * 3 - actual/365
            * 4 - 30/360 PSA
            * 5 - 30/360 ISDA
            * 6 - 30/360 European
            * 7 - actual/365 Japanese
            * 8 - act/act ISMA
            * 9 - act/360 ISMA
            * 10 - act/365 ISMA
            * 11 - 30/360 ISMA
            * 12 - actual/365 ISDA
            Default is 0.
        
        Returns:
        --------
        single value or series
            if len(daysperiod) == 1 returns single value else series
            
        """
        ddict={
            'date': TypePro.to_datetime(date),
            'basis': basis}
        df = TypePro.to_frame_frome_dict(ddict)
        df.basis.fillna(cls._default_basis, inplace=True)
        date = df.date
        basis = df.basis
        dny = pd.Series(index=date.index)
        y = date.apply(lambda x:datetime.datetime(x.year, 1, 1))
        y_next = y.apply(lambda x:x+relativedelta(years=1))
        id0 = basis.isin([0, 8, 10, 12])
        if id0.any():
            dny[id0] = cls.date_diff_act(y[id0], y_next[id0])
        id1 = basis.isin([1, 2, 4, 5, 6, 9, 11])
        if id1.any():
            dny[id1] = cls.date_diff_360_SIA(y[id1], y_next[id1])
        id2 = basis.isin([3, 7])
        if id2.any():
            dny[id2] = cls.date_diff_365(y[id2], y_next[id2])
        return dny if dny.index.size>1 else dny.values[0]

    @classmethod
    def year_fraction(cls, start, end, basis=_default_basis):
        '''Fraction of year between start and end .
        
        Params:
        -------
        start : str, datetime, list, series
        end : str, datetime, list, series
        basis : int, list, series, default is None
            the day-count basis
            * 0 - actual/actual (default)
            * 1 - 30/360 SIA
            * 2 - actual/360
            * 3 - actual/365
            * 4 - 30/360 PSA
            * 5 - 30/360 ISDA
            * 6 - 30/360 European
            * 7 - actual/365 Japanese
            * 8 - act/act ISMA
            * 9 - act/360 ISMA
            * 10 - act/365 ISMA
            * 11 - 30/360 ISMA
            * 12 - actual/365 ISDA
            Default is 0.
        
        Returns:
        --------
        single value or series
            if len(daysperiod) == 1 returns single value else series
            
        '''
        ddict={
            'start': TypePro.to_datetime(start),
            'end': TypePro.to_datetime(end),
            'basis': basis}
        df = TypePro.to_frame_frome_dict(ddict)
        df.basis.fillna(cls._default_basis, inplace=True)
        start = df.start
        end = df.end
        basis = df.basis
        yf = pd.Series(index=start.index)
        # 0 - actual/actual
        id0 = basis.isin([0, 8])
        if id0.any():
            start0 = start.apply(lambda x:x+relativedelta(years=1))
            yf[id0] = cls.date_diff_act(start[id0], end[id0]
                        )/cls.date_diff_act(start[id0], start0[id0])
        # 1 - 30/360 SIA
        id1 = (basis==1)
        if id1.any():
            yf[id1] = cls.date_diff_360_SIA(start[id1], end[id1])/360
        # 2 - actual/360 ISMA
        id2 = basis.isin([2, 9])
        if id2.any():
            yf[id2] = cls.date_diff_act(start[id2], end[id2])/360
        # 3 - actual/365 ISMA
        id3 = basis.isin([3, 10])
        if id3.any():
            yf[id3] = cls.date_diff_act(start[id3], end[id3])/365
        # 4 - 30/360 PSA
        id4 = (basis==4)
        if id4.any():
            yf[id4] = cls.date_diff_360_PSA(start[id4], end[id4])/360
        # 5 - 30/360 ISDA
        id5 = (basis==5)
        if id5.any():
            yf[id5] = cls.date_diff_360_ISDA(start[id5], end[id5])/360
        # 6 - 30/360 European ISMA
        id6 = basis.isin([6, 11])
        if id6.any():
            yf[id6] = cls.date_diff_360_european(start[id6], end[id6])/360
        # 7 - actual/365 Japanese
        id7 = (basis==7)
        if id7.any():
            yf[id7] = cls.date_diff_365(start[id7], end[id7])/365
        # 8 - actual/365 ISDA
        id8 = (basis==12)
        if id8.any():
            sf = (1+cls.date_diff_act(start[id8], start[id8].apply(
                lambda x:datetime.datetime(x.year, 12, 31)))
                )/cls.date_num_year(start[id8])
            ef = cls.date_diff_act(
                    end[id8].apply(lambda x:datetime.datetime(x.year, 1, 1)), 
                       end[id8])/cls.date_num_year(end[id8])
            yf[id8] = sf+ef-start[id8].apply(lambda x:x.year) + \
                   end[id8].apply(lambda x:x.year)-1
        return yf if yf.index.size>1 else yf.values[0]

# EOF