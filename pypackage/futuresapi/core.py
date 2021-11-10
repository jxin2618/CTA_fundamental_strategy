# -*- coding: utf-8 -*-
"""
Created on Fri Jul 31 09:13:40 2020

@author: DamonChan
"""

import os
import pandas as pd
from conf import Conf
from pandas.io.sql import read_sql
from pydbms import MysqlOrm, Py2Mysql

config = Conf()
config.read(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini'))

class API(object):
    
    
    def __init__(self):
        self._dbo = MysqlOrm(**dict(config['commodity']))

    def _optional_params_to_conditions(self, **kwargs):
        return [
            (Py2Mysql.list2colin(
                column=i, data=j)
                if j else "") for i, j in kwargs.items()]

    def _standardized_columns(self, columns, default_columns):
        columns = pd.Series(columns).drop_duplicates()
        columns = columns[
                ~columns.isin([i[1] for i in default_columns])].apply(
                    Py2Mysql.deal_field)
        for i in default_columns:
            k, v = i
            tmp_col = '{k}.{v}'.format(
                k=k, v=Py2Mysql.deal_field(v))
            if v in columns.tolist():
                columns.replace(v, tmp_col, inplace=True)
            else:
                columns = columns.append(
                    pd.Series(tmp_col), ignore_index=True)
        columns = Py2Mysql.list2str(data=columns, quote=False, output='n')
        return columns

    def futures_calendar(self, start, end, columns=['date', 'is_trading_date',
                                              'previous_trading_date',
                                              'current_trading_date'],
                         is_trading_date=True):
        """Query futures calendar.
        
        Params:
        -------
        start: str
        end: str
        columns: list
        is_trading_date: boolean
        
        Returns:
        --------
        res: dataframe
        
        """
        cbetween = Py2Mysql.between(
                'date', left=start, right=end, quote=True, include='both')
        cis_trading_date = "is_trading_date={x}".format(x=is_trading_date)
        conds = Py2Mysql.conditions_joint([cbetween, cis_trading_date])
        col = Py2Mysql.list2str(columns, quote=False, output='n')
        res = self._dbo.FuturesCalendar.get_data(column=col, cond=conds)
        return res

    def futures_list(self, date=None, valid_type=0, **kwargs):
        """Query futures list.

        Params:
        -------
        date: str, datetime
        valid_type: int, valid in {0, 1}
            * 0 -> use list_date and delist_date
            * 1 -> use valid_start and valid_end
        **kwargs: optional params, str, list
            used to be query conditions


        Returns:
        --------
        dataframe

        """
        start = 'list_date' if valid_type == 0 else 'valid_start'
        end = 'delist_date' if valid_type == 0 else 'valid_end'
        tables = "FuturesInformation"
        columns = (
            "sec_code, standard_contract_code, cash_name_full, sec_type, "
            "list_date, delist_date, valid_start, valid_end")
        cond_date = [(
            "{start}<={d} and {end}>={d}".format(
                d=Py2Mysql.deal_value(date), start=start, end=end)
            if date else "")]
        cond_optional = self._optional_params_to_conditions(**kwargs)
        cond_list = cond_optional + cond_date
        conds = Py2Mysql.conditions_joint(conds=cond_list, operator='AND')
        res = self._dbo.__dict__[tables].get_data(
            column=columns, cond=conds)
        if res.index.size > 0:
            res.loc[:, 'date'] = pd.to_datetime(date)
        return res

    def last_n_date(self, date, n, is_trading_date=True, include=False):
        """Query last N date.

        Params:
        -------
        date: str, datetime
        n: int
        is_trading_date: True
        include: boolean
            include date or not

        Returns:
        --------
        dataframe

        """
        prefix = ""
        suffix = " limit {n} ;".format(n=n)
        columns = "date, is_trading_date"
        tables = 'FuturesCalendar'
        include = 'both' if include else 'neither'
        cond_trading_date = [(
                Py2Mysql.list2colin(
                    column='is_trading_date', data=is_trading_date,
                    table="FuturesCalendar")
                if is_trading_date else "")]
        cond_date = [Py2Mysql.between(
            column='date', left=None,
            right=date, quote=True, include=include)]
        cond_list = cond_date + cond_trading_date
        conds = Py2Mysql.conditions_joint(conds=cond_list, operator='AND')
        sql = (
            "SELECT {prefix} {columns} from {tables} {conds} order by date "
            "desc {suffix}").format(
                    columns=columns, tables=tables, conds=conds,
                    prefix=prefix, suffix=suffix)
        return pd.io.sql.read_sql(sql=sql, con=self._dbo._engine).sort_values(
                'date').reset_index(drop=True)

    def last_valid_constituent(self, date, sec_code):
        """Query last valid constituent.

        Params:
        -------
        date: str
            date之前（不含）的最近一天有效的成分
        sec_code: str

        Returns:
        --------
        dataframe

        """
        tables = (
            " (select max(date) as dt from FuturesIndexConstituent "
            " where sec_code='{sc}' and date<'{dt}') as tmp, "
            "FuturesIndexConstituent").format(
            sc=sec_code, dt=date)
        columns = "date, sec_code, constituent, constituent_weight"
        cond_sec_code = [(
            Py2Mysql.list2colin(
                column='sec_code', data=sec_code,
                table='FuturesIndexConstituent')
            if sec_code else "")]
        cond_join = ["tmp.dt=FuturesIndexConstituent.date"]
        cond_list = cond_sec_code + cond_join
        conds = Py2Mysql.conditions_joint(
                conds=cond_list, operator='AND')
        sql = "SELECT {columns} from {tables} {conds};".format(
            columns=columns, tables=tables, conds=conds)
        return read_sql(sql=sql, con=self._dbo._engine)


    def query_common(self, start, end, sec_code=None, standard_contract_code=None,
                     columns=['close'], how='inner', is_trading_date=True,
                     with_liquid=False, **kwargs):
        """通用查询

        Params:
        -------
        start: str, datetime
        end: str, datetime
        sec_code: str, list
        standard_contract_code: str, list
        columns: str, list
        how: str
            valid in {'inner', 'left', 'right'}
            default is 'inner'
        is_traidng_date: boolean
        **kwargs: optional params
            used to be query conditions

        Returns:
        --------
        dataframe

        """
        liquid_cond = (
            " inner join FuturesLiquidUniverse "
            " on FuturesLiquidUniverse.standard_contract_codee=FuturesInformation.standard_contract_code "
            " and FuturesCalendar.date>= FuturesLiquidUniverse.valid_start "
            " and FuturesCalendar.date<= FuturesLiquidUniverse.valid_end "
            if with_liquid else "")
        tables = (
            " FuturesInformation "
            " {how} join FuturesCalendar on "
            " FuturesInformation.valid_start<=FuturesCalendar.date and "
            " FuturesInformation.valid_end>=FuturesCalendar.date and "
            " FuturesInformation.list_date<=FuturesCalendar.date and "
            " FuturesInformation.delist_date>=FuturesCalendar.date "
            " {how} join FuturesQuotations on "
            " FuturesCalendar.date=FuturesQuotations.date and "
            " FuturesInformation.sec_code=FuturesQuotations.sec_code "
            " left join FuturesPartition on "
            " FuturesInformation.standard_contract_code=FuturesPartition.standard_contract_code"
            " {lc}"
            ).format(how=how, lc=liquid_cond)
        default_columns = [
            ('FuturesCalendar', 'date'),
            ('FuturesInformation', 'sec_code'),
            ('FuturesInformation', 'standard_contract_code'),
            ('FuturesInformation', 'valid_start'),
            ('FuturesInformation', 'valid_end')]
        columns = self._standardized_columns(
            columns=columns, default_columns=default_columns)
        cond_date = [Py2Mysql.between(
            column='FuturesCalendar.date', left=start,
            right=end, quote=True, include='both')]
        cond_sec_code = [(
            Py2Mysql.list2colin(
                column='sec_code', data=sec_code,
                table='FuturesInformation')
            if sec_code else "")]
        cond_standard_contract_code = [(
            Py2Mysql.list2colin(
                column='standard_contract_code', data=standard_contract_code,
                table='Futuresformation')
            if standard_contract_code else "")]
        cond_trading_date = [(
            Py2Mysql.list2colin(
                column='is_trading_date', data=is_trading_date,
                table="FuturesCalendar")
            if is_trading_date else "")]
        cond_optional = [
            (Py2Mysql.list2colin(
                column=i, data=j,)
                if j else "") for i, j in kwargs.items()]
        cond_list = cond_date + cond_sec_code + cond_standard_contract_code +\
            cond_trading_date + cond_optional
        conds = Py2Mysql.conditions_joint(conds=cond_list, operator='AND')
        sql = "SELECT {columns} from {tables} {conds};".format(
            columns=columns, tables=tables, conds=conds)
        return read_sql(sql=sql, con=self._dbo._engine)


    def query_constituent(
            self, start, end, sec_code=None, sec_type=None,
            columns=None, how='inner', is_trading_date=True,
            **kwargs):
        """成分查询

        Params:
        -------
        start: str, datetime
        end: str, datetime
        sec_code: str, list
            sec_code of index
        sec_type: str
            sec_type of index
        columns: str, list
            properties of constituent
        how: str
            valid in {'inner', 'left', 'right'}
            default is 'inner'
        is_traidng_date: boolean
        **kwargs: optional params
            used to be query conditions

        Returns:
        --------
        dataframe

        """
        quote_date = 'date'
        tables = (
            " ( select valid_start, valid_end, sec_code, "
            " sec_type as idx_sec_type"
            " from FuturesInformation ) as idx "
            " {how} join FuturesCalendar on "
            " idx.valid_start<=FuturesCalendar.date and "
            " idx.valid_end>=FuturesCalendar.date "
            " {how} join FuturesIndexConstituent on "
            " FuturesCalendar.{d}=FuturesIndexConstituent.date and "
            " idx.sec_code=FuturesIndexConstituent.sec_code "
            " {how} join FuturesQuotations on "
            " FuturesQuotations.sec_code=FuturesIndexConstituent.constituent "
            " and FuturesQuotations.date=FuturesCalendar.date "
            " {how} join FuturesInformation on "
            " FuturesInformation.sec_code=FuturesIndexConstituent.constituent"
            ).format(how=how, d=quote_date)
        default_columns = [
            ('FuturesCalendar', 'date'), ('idx', 'sec_code'),
            ('FuturesInformation', 'standard_contract_code'),
            ('FuturesIndexConstituent', 'constituent'),
            ('FuturesIndexConstituent', 'constituent_weight')]
        columns = self._standardized_columns(
            columns=columns, default_columns=default_columns)
        cond_date = [Py2Mysql.between(
            column='FuturesCalendar.date', left=start,
            right=end, quote=True, include='both')]
        cond_sec_code = [(
            Py2Mysql.list2colin(
                column='sec_code', data=sec_code,
                table='idx')
            if sec_code else "")]
        cond_sec_type = [(
            Py2Mysql.list2colin(
                column='idx_sec_type', data=sec_type,
                table='idx')
            if sec_type else "")]
        cond_trading_date = [(
            Py2Mysql.list2colin(
                column='is_trading_date', data=is_trading_date,
                table="FuturesCalendar")
            if is_trading_date else "")]
        cond_optional = [
            (Py2Mysql.list2colin(
                column=i, data=j,)
                if j else "") for i, j in kwargs.items()]
        cond_list = cond_date + cond_sec_code + cond_sec_type + \
           cond_trading_date + cond_optional
        conds = Py2Mysql.conditions_joint(conds=cond_list, operator='AND')
        sql = "SELECT {columns} from {tables} {conds};".format(
            columns=columns, tables=tables, conds=conds,
            )
        return read_sql(sql=sql, con=self._dbo._engine)


# EOF
