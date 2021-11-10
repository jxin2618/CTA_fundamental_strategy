# -*- coding: utf-8 -*-
"""
Created on Thu Jan 10 16:09:30 2019

@author: chendm
"""

import pandas as pd
import statsmodels.api as sm
from .typepro import TypePro


class OutlierPro(object):

    @classmethod
    def z_score(cls, data, **kwargs):
        """Z-Score.

        (x-x.mean(**kwargs))/x.std(**kwargs)

        Params:
        -------
        data: series, dataframe
        **kwargs: params

        Returns:
        --------
        series, dataframe

        """
        return (data-data.mean(**kwargs))/data.std(**kwargs)

    @classmethod
    def mad(cls, data, **kwargs):
        """MAD.

        Params:
        -------
        data: series, dataframe
        **kwargs: params
            data.median(**kwargs)

        Returns:
        --------
        series, dataframe

        """
        return abs(data-data.median(**kwargs)).median(**kwargs)

    @classmethod
    def extreme_value(cls, data, lower_bound=-pd.np.inf,
                      upper_bound=pd.np.inf):
        """Extreme value recognition.

        Paramas:
        --------
        data: series, dataframe
        lower_bound: numeric
        upper_bound: numeric

        Returns:
        --------
        res: dict

        """
        normal_idx = (data <= upper_bound) & (data >= lower_bound)
        # 不能直接用~normal_idx，会收到nan的影响
        extreme_idx = (data > upper_bound) | (data < lower_bound)
        upper_extreme_idx = data > upper_bound
        lower_extreme_idx = data < lower_bound
        res = {'normal_idx': normal_idx, 'extreme_idx': extreme_idx,
               'upper_extreme_idx': upper_extreme_idx,
               'lower_extreme_idx': lower_extreme_idx,
               'upper_bound': upper_bound, 'lower_bound': lower_bound}
        return res

    @classmethod
    def extreme_value_mad(cls, data, n=5, **kwargs):
        """Extreme value recognized by +-n*mad.

        Paramas:
        --------
        data: series or dataframe
        n: numeric
            N*MAD
        axis : {index (0), columns (1)}
        skipna : boolean, default True
            Exclude NA/null values when computing the result.
        level : int or level name, default None
            If the axis is a MultiIndex (hierarchical), count along a
            particular level, collapsing into a Series
        numeric_only : boolean, default None
            Include only float, int, boolean columns. If None, will attempt to use
            everything, then use only numeric data. Not implemented for Series.

        Returns:
        --------
        res: dict

        """
        u = data.median(**kwargs)
        m = cls.mad(data, **kwargs)
        upper_bound = u+n*m
        lower_bound = u-n*m
        res = cls.extreme_value(
            data=data, upper_bound=upper_bound,
            lower_bound=lower_bound)
        return res

    @classmethod
    def extreme_value_std(cls, data, n=3, **kwargs):
        """Extreme value recognized by +-n*std.

        Paramas:
        --------
        data: series, dataframe
        n: numeric
            N*std
        axis : {index (0), columns (1)}
        skipna : boolean, default True
            Exclude NA/null values when computing the result.
        level : int or level name, default None
            If the axis is a MultiIndex (hierarchical), count along a
            particular level, collapsing into a Series
        numeric_only : boolean, default None
            Include only float, int, boolean columns. If None, will attempt to use
            everything, then use only numeric data. Not implemented for Series.

        Returns:
        --------
        res: dict

        """
        u = data.mean(**kwargs)
        std = data.std(**kwargs)
        upper_bound = u+n*std
        lower_bound = u-n*std
        res = cls.extreme_value(
            data=data, upper_bound=upper_bound,
            lower_bound=lower_bound)
        return res

    @classmethod
    def extreme_value_q(cls, data, q=0.05, **kwargs):
        """Extreme value recognized by quantile(q and 1-q).

        Paramas:
        --------
        data: series or dataframe
        q: 0 ~ 1
            q quantile
            out of [q, 1-q]
        axis : {index (0), columns (1)}
        skipna : boolean, default True
            Exclude NA/null values when computing the result.
        level : int or level name, default None
            If the axis is a MultiIndex (hierarchical), count along a
            particular level, collapsing into a Series
        numeric_only : boolean, default None
            Include only float, int, boolean columns. If None, will attempt to use
            everything, then use only numeric data. Not implemented for Series.

        Returns:
        --------
        res: dict
        """
        if isinstance(data, pd.DataFrame):
            params = {'axis': kwargs.get('axis', 0)}
        else:
            params = {}
        upper_bound = data.quantile(1-q, **params)
        lower_bound = data.quantile(q, **kwargs)
        res = cls.extreme_value(
            data=data, upper_bound=upper_bound,
            lower_bound=lower_bound)
        return res

    @classmethod
    def extreme_value_maq(cls, data, q=0.05, **kwargs):
        """Extreme value recognized by asymmetric quantile(q and 1-q).

        Paramas:
        --------
        data: series or dataframe
        q: 0 ~ 1
            q quantile
            out of [w1*q, 1-w2*q]
        axis : {index (0), columns (1)}
        skipna : boolean, default True
            Exclude NA/null values when computing the result.
        level : int or level name, default None
            If the axis is a MultiIndex (hierarchical), count along a
            particular level, collapsing into a Series
        numeric_only : boolean, default None
            Include only float, int, boolean columns. If None, will attempt to use
            everything, then use only numeric data. Not implemented for Series.

        Returns:
        --------
        res: dict
        """
        if isinstance(data, pd.DataFrame):
            params = {'axis': kwargs.get('axis', 0)}
        else:
            params = {}
        md = (data-data.median())
        total_unit = md[md >= 0].mean()+abs(md[md < 0].mean())
        w1 = abs(md[md <= 0].mean())/total_unit
        w2 = 1 - w1
        upper_bound = data.quantile(1-w2*q, **params)
        lower_bound = data.quantile(w1*q, **kwargs)
        res = cls.extreme_value(
            data=data, upper_bound=upper_bound,
            lower_bound=lower_bound)
        return res

    @classmethod
    def extreme_value_mdq(cls, data, q=0.05, **kwargs):
        """中位数偏差（绝对值）分位数法识别极值

        Paramas:
        --------
        data: series or dataframe
        q: 0 ~ 1
            q quantile
        axis : {index (0), columns (1)}
        skipna : boolean, default True
            Exclude NA/null values when computing the result.
        level : int or level name, default None
            If the axis is a MultiIndex (hierarchical), count along a
            particular level, collapsing into a Series
        numeric_only : boolean, default None
            Include only float, int, boolean columns. If None, will attempt to use
            everything, then use only numeric data. Not implemented for Series.

        Returns:
        --------
        res: dict
        """
        if isinstance(data, pd.DataFrame):
            params = {'axis': kwargs.get('axis', 0)}
        else:
            params = {}
        md = (data-data.median())
        upper_bound = md.abs().quantile(1-q, **params)
        lower_bound = -upper_bound
        ub = (
            data[md > upper_bound].min()
            if (md > upper_bound).any()
            else data.max())
        lb = (
            data[md < lower_bound].max()
            if (md < lower_bound).any()
            else data.min())
        res = cls.extreme_value(
            data=data, upper_bound=ub,
            lower_bound=lb)
        return res

    @classmethod
    def _winsorized(cls, x, outlier, **kwargs):
        res = outlier(x, **kwargs)
        x[res.get('upper_extreme_idx')] = res.get('upper_bound')
        x[res.get('lower_extreme_idx')] = res.get('lower_bound')
        return x

    @classmethod
    def winsorized(cls, data, outlier, **kwargs):
        """Winsorized.

        Params:
        -------
        data: series, dataframe
        outlier: func
            extreme value function
        **kwargs: params
            outlier(**kwargs)

        Returns:
        --------
        res: series, dataframe

        """
        data = data.copy()
        res = data.apply(
            cls._winsorized, axis=kwargs.get('axis', 0),
            outlier=outlier, **kwargs) \
            if isinstance(data, pd.DataFrame) else cls._winsorized(
                data, outlier=outlier, **kwargs)
        return res

    @classmethod
    def _winsorized_linear_interpolation(cls, x, outlier, **kwargs):
        res = outlier(x, **kwargs)
        nidx = res.get('normal_idx')
        nidx_size = nidx[nidx].index.size/2
        up_step = (res.get('upper_bound')-x.median())/nidx_size
        low_step = (res.get('lower_bound')-x.median())/nidx_size

        x[res.get('upper_extreme_idx')] = \
            x[res.get('upper_extreme_idx')].rank() * up_step +\
            res.get('upper_bound')
        x[res.get('lower_extreme_idx')] = \
            x[res.get('lower_extreme_idx')].rank(ascending=False) * \
            low_step+res.get('lower_bound')
        return x

    @classmethod
    def winsorized_linear_interpolation(cls, data, outlier, **kwargs):
        """Winsorized with linear interpolation.

        Params:
        -------
        data: series, dataframe
        outlier: func
            extreme value function
        **kwargs: params
            outlier(**kwargs)

        Returns:
        --------
        res: series, dataframe

        """
        data = data.copy()
        res = (data.apply(
            cls._winsorized_linear_interpolation, axis=kwargs.get('axis', 0),
                outlier=outlier, **kwargs)\
            if isinstance(data, pd.DataFrame)
            else cls._winsorized_linear_interpolation(
                data, outlier=outlier, **kwargs))
        return res


class StatisticModel(object):

    @classmethod
    def wls(cls, y, x, constant=True, w=1):
        """

        Params:
        -------
        y: series
        x: dataframe
        constant: boolean
        w: numeric or series

        Returns:
        --------
        res: obj

        """
        X = sm.add_constant(x) if constant else x
        wls_model = sm.WLS(y, X, w)
        return wls_model.fit()


class NeutralPro(object):

    @classmethod
    def neutral_regression(cls, data, neutral_columns, weight_column=None,
                           constant=True):
        """Factor neutralization with regression.

        Params:
        -------
        data: dataframe
        neutral_columns: list
            columns in data used to be X
        weight_column: str

        Returns:
        --------
        res: dataframe
            resid of wls

        """
        neutral_columns = TypePro.to_list(neutral_columns)
        res = pd.DataFrame(
            index=data.index,
            columns=data.columns.drop(neutral_columns))
        w = data.loc[:, weight_column] if weight_column else 1
        neutral_data = data.loc[:, neutral_columns]
        data = data.drop(neutral_columns, axis=1)
        if weight_column:
            data = data.drop(weight_column, axis=1)
        res = data.apply(
            lambda d: StatisticModel.wls(
                y=d.dropna() , x=neutral_data.reindex(d.dropna().index),
                w=w, constant=constant).resid)
        return res

    @classmethod
    def neutral_grouping(cls, data, neutral_columns, g, gtype=0,
                         duplicates='raise', catelike=None,
                         neutral_func=OutlierPro.z_score, neutral_params={}):
        """Factor neutralization with grouping.

        Params:
        -------
        data: dataframe
        neutral_columns: list
            columns in data used to generate groups
            sequential dependencies
        g: int, list, series
            nums of groups for corresponding columns in neutral_columns
        gtype: 0 or 1
            only used when g=3
            * 0: 0-0.333, 0.333-0.666, 0.666-1
            * 1: 0-0.3, 0.3-0.7, 0.7-1
        duplicates : {default 'raise', 'drop'}, optional
            If bin edges are not unique, raise ValueError or drop non-uniques.
        catelike: str, list
            category-like columns in date.columns used themselves as groups insdead of qcut
        neutral_func: function
            default is z-score
        neutral_params: dict
            neutral_func(**neutral_params)

        Returns:
        --------
        res: dataframe
            z-score

        """
        neutral_columns = TypePro.to_list(neutral_columns)
        valid_columns = data.columns.drop(neutral_columns)
        res = pd.DataFrame(index=data.index, columns=valid_columns)
        neutral_df = data.loc[:, neutral_columns]
        gp = GroupPro.quantile_grouping_multiple(
            data=neutral_df, g=g, gtype=gtype,
            duplicates=duplicates, catelike=catelike)
        res = data.loc[:, valid_columns].groupby(gp).apply(
            neutral_func, **neutral_params)
        return res


class GroupPro(object):

    @classmethod
    def quantile_grouping(cls, data, g=3, gtype=0, label='g',
                          duplicates='raise', **kwargs):
        """Grouping by quantile.

        params:
        -------
        data: series
        g: int
            nums of groups
        gtype: 0 or 1
            only used when g=3
            * 0: 0-0.333, 0.333-0.666, 0.666-1
            * 1: 0-0.3, 0.3-0.7, 0.7-1
        duplicates : {default 'raise', 'drop'}, optional
            If bin edges are not unique, raise ValueError or drop non-uniques.

        Returns:
        --------
        res: series(Categories)

        """
        glist = list(pd.np.arange(0, 1, 1/(g))) + [1]
        if gtype != 0 and g == 3:
            glist = [0, 0.3, 0.7, 1]
        glabel = ['{l}{i}'.format(l=label, i=i+1)
                  for i in pd.np.arange(len(glist)-1)]
        return pd.qcut(x=data, q=glist, labels=glabel, duplicates=duplicates)

    @classmethod
    def quantile_grouping_multiple(cls, data, g, gtype=0, order=None,
                                   duplicates='raise', catelike=None,
                                   **kwargs):
        """Multiple grouping by quantile.

        g = [2, 2] means 2x2 grouping

        params:
        -------
        data: dataframe
        g: int, list, series
            nums of groups for each columns
            order: g1 < g2...< gn
        gtype: 0 or 1
            only used when g=3
            * 0: 0-0.333, 0.333-0.666, 0.666-1
            * 1: 0-0.3, 0.3-0.7, 0.7-1
        order: list
            grouping order
            if order is not None, data.reindex(order)
        duplicates : {default 'raise', 'drop'}, optional
            If bin edges are not unique, raise ValueError or drop non-uniques.
        catelike: str, list
            category-like columns in date.columns used themselves as groups insdead of qcut
            if c in order(or data.columns) is category-like, corresponding value in g is not used

        Returns:
        --------
        res: series

        """
        def _order_grouping(x, d, p):
            p['label'] = x.iloc[:, 0].unique()[0] + '-' + d.name
            d = d.reindex(x.index)
            group = cls.quantile_grouping(data=d, duplicates=duplicates, **p)
            return group
        grouping_columns = (
            data.columns if not order else TypePro.to_list(order))
        label = (
            TypePro.to_series(grouping_columns, index=grouping_columns)
            if TypePro.check_series(g) else grouping_columns)
        params = TypePro.to_frame_frome_dict(
            {'g': g, 'gtype': gtype, 'label': label})
        params.set_index('label', drop=False, inplace=True)
        catelike = TypePro.to_list(catelike) if catelike else []
        for i in pd.np.arange(params.index.size):
            idx = params.index[i]
            p = params.loc[idx].to_dict()
            if idx not in catelike:
                if i == 0:
                    groups = cls.quantile_grouping(
                        data=data.loc[:, idx], duplicates=duplicates, **p)
                else:
                    groups = groups.to_frame().groupby(
                        params.index[i-1], as_index=False, group_keys=False
                        ).apply(_order_grouping, d=data.loc[:, idx], p=p)
            else:
                if i == 0:
                    groups = data.loc[:, idx].copy()
                else:
                    groups = groups + '-' + data.loc[:, idx]
        groups.sort_index(inplace=True)
        groups._group_names = params.index
        return groups

    @classmethod
    def multiple_groups_to_frame(
            cls, data, seperator='-', columns=None, **kwargs):
        """Take multiple groups into frame.

        data: series
        seperator: str
        columns: list

        Returns:
        --------
        res: dataframe

        """
        columns = columns if columns else data._group_names
        res = pd.DataFrame(
            data.apply(lambda x: x.split(seperator)).to_dict(),
            index=columns).T
        return res


# EOF
