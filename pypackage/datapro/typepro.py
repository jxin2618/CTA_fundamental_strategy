# -*- coding: utf-8 -*-
"""
Created on Sat Dec  9 19:36:33 2017

@author: ciaciaciu
"""

from collections import Iterable
from datetime import datetime
from pandas import Series, DataFrame, np, to_datetime as pd_to_datetime, isnull

class TypePro(object):
    
    @classmethod
    def check_type(cls, data, dtype):
        """Check data, whether type of data is given type
        
        Params:
        -------
        data : any type of data
        dtype : type
        
        Returns:
        --------
        boolean
        
        """
        return isinstance(data, dtype)
    
    @classmethod
    def check_series(cls, data):
        """Check data, whether type of data is pandas.core.series.Series
        
        Params:
        -------
        data : any type of data
        
        Returns:
        --------
        boolean
        
        """
        return cls.check_type(data, Series)
    
    @classmethod
    def check_dataframe(cls, data):
        """Check data, whether type of data is pandas.core.frame.DataFrame
        
        Params:
        -------
        data : any type of data
        
        Returns:
        --------
        boolean
        
        """
        return cls.check_type(data, DataFrame)
    
    @classmethod
    def check_ndarry(cls, data):
        """Check data, whether type of data is pd.np.ndarry
        
        Params:
        -------
        data : any type of data
        
        Returns:
        --------
        boolean
        
        """
        return cls.check_type(data, np.ndarray)
    
    @classmethod
    def check_datetime(cls, data):
        """Check data, whether type of data is datetime.datetime
        """
        return (cls.check_type(data, datetime) or
                cls.check_type(data, datetime.now().date().__class__))
    
    @classmethod
    def check_iterable(cls, data, except_types=type(None)):
        """Check data, whether type of data is iterable except <except_types>
        
        Params:
        -------
        data : any type of data
        except_types: list
        
        Returns:
        --------
        boolean
        
        """
        check = cls.check_type(data, except_types)
        if not check:
            res = cls.check_type(data, Iterable)
        else:
            res = False
        return res
    
    @classmethod
    def check_null(cls, data):
        """Check data, whether type of data is null
        
        Params:
        -------
        data : any type of data
        
        Returns:
        --------
        boolean
        
        """
        return isnull(data)
        
    @classmethod
    def check_bool(cls, data):
        """Check data, whether type of data is bool
        
        Params:
        -------
        data : any type of data
        
        Returns:
        --------
        boolean
        
        """
        res = any(
            [cls.check_type(data, bool), cls.check_type(data, np.bool),
             cls.check_type(data, np.bool8), cls.check_type(data, np.bool_)]
            )
        return res
        
    @classmethod
    def check_real(cls, data):
        """Check data, whether type of data is bool
        
        Params:
        -------
        data : any type of data
        
        Returns:
        --------
        boolean
        
        """
        return np.isreal(data)
        
    @classmethod
    def to_list(cls, data):
        """Transform 1-dimensional data to list
        
        Params:
        -------
        data : 1-dimensional data
        
        Returns:
        --------
        list
        
        """
        if not isinstance(data, list):
            if cls.check_series(data):
                data = data.tolist()
            elif cls.check_dataframe(data):
                if data.columns.size==1:
                    data = data.iloc[:,0].tolist()
                elif data.index.size==1:
                    data = data.iloc[0,:].tolist()
                else:
                    raise ValueError("type of data must be in "
                                 "[list, tuple, series] and 1-dimensional data")
            elif cls.check_iterable(data, except_types=str):
                data = list(data)
            else:
                data = [data]
        return data
    
    @classmethod
    def to_series(cls, data, **kwargs):
        """Transform 1-dimensional data to list
        
        Params:
        -------
        data : 1-dimensional data
        **kwargs: params
            Series(data, **kwargs)
        
        Returns:
        --------
        series
        
        """
        if cls.check_dataframe(data):
            if data.columns.size==1:
                    data = data.iloc[:,0].tolist()
            elif data.index.size==1:
                    data = data.iloc[0,:].tolist()
            else:
                raise ValueError("type of data must be in "
                                 "[list, tuple, series] and 1-dimensional data")
        else:
            data = Series(data, **kwargs)
        return data
        
    @classmethod
    def to_frame_frome_dict(cls, ddict, fillna=np.NaN):
        """take two dataset into series having common index
        
        Params:
        -------
        ddict: dict
        fillna: single value
        
        Returns:
        --------
        left: series
        right: series
        
        """
        check_single = [
            TypePro.check_iterable(v, except_types=str)
            for k, v in ddict.items()]
        if not any(check_single):
            df = DataFrame(ddict, index=[0])
        else:
            df = DataFrame(ddict)
        return df
    
    @classmethod
    def to_list_str(cls, data, separator=',', drop_blank=True):
        """Transform str to list.
        
        Params:
        -------
        data : str
        separator : str, default is ','
        drop_blank : boolean, default is True
            * True : remove all blank
            * False : do not remove blank
        
        Returns:
        --------
        list
        
        """
        data = data.split(separator)
        if drop_blank:
            data = [i.replace(' ','') for i in data]
        return data
    
    @classmethod
    def to_datetime(cls, data):
        """Transform str to pd.to_datetime.
        
        Params:
        ------
        data : str, list, tuple, series, dataframe
        
        Returns:
        series, dataframe, list, Timestamp
        
        """
        res = cls.apply_map(data=data, func=pd_to_datetime)
        return res
    
    @classmethod
    def to_date_string(cls, data, dformat='%Y%m%d'):
        """Transform datetime to str.
        
        Params:
        -------
        data: datetime or iterable class of datetime
        format: str
            date string
        
        Returns:
        --------
        dataframe, serise, list, str
        
        """
        res = cls.apply_map(data, func=lambda x: x.strftime(dformat))
        return res
        
    @classmethod
    def apply_map(cls, data, func, iter_except=str, **kwargs):
        """apply map.
        
        Params:
        -------
        data: any
        func: function
            Python function, returns a single value from a single value
        iter_except: types
            if data is iterable except iter_except
        **kwargs: params
            dataframe.applymap(data, func, **kwargs)
            series.apply(data, func, **kwargs)
            [func(i, **kwargs) for i in data]
            func(data, **kwargs)
        
        Returns:
        --------
        series, dataframe, list, ...
        
        """
        if cls.check_dataframe(data):
            res = data.applymap(func, **kwargs)
        elif cls.check_series(data):
            res = data.apply(func, **kwargs)
        elif cls.check_iterable(data, except_types=iter_except):
            res = [func(i, **kwargs) for i in data]
        else:
            res = func(data, **kwargs)
        return res





    

    

    



    

    

    
# EOF