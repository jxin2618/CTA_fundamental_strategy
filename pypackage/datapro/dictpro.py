# -*- coding: utf-8 -*-
"""
Created on Wed Apr  3 09:44:02 2019

@author: DamonChan
"""

from collections import ChainMap


class DictPro(object):

    @classmethod
    def add_default_kwargs(cls, dkwargs, **kwargs):
        """向kwargs添加存在于dkwargs中且不存在于kwargs的(key, value)

        Params:
        -------
        dkwargs : dict
            默认的dict
        kwargs : dict
            传入的参数

        Returns:
        --------
        skwargs : dict
            经过dkwargs初始化后的kwargs

        """

        skwargs = ChainMap(kwargs, dkwargs)
        return dict((k, v) for k, v in skwargs.items())

    @classmethod
    def fit_default_kwargs(cls, dkwargs, **kwargs):
        """把kwargs的keys调整到dkwargs的keys

        Params:
        -------
        dkwargs : dict
            默认的dict
        kwargs : dict
            传入的参数

        Returns:
        --------
        skwargs : dict
            经过dkwargs初始化后的kwargs

        """

        skwargs = ChainMap(kwargs, dkwargs)
        return dict((k, skwargs[k]) for k in dkwargs.keys())

# EOF
