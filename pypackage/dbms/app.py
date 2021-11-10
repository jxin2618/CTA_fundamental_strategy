# -*- coding: utf-8 -*-
"""
Created on Tue Mar  6 10:12:17 2018

@author: Chandeman
"""

from datetime import datetime
import pandas as pd
from . import py2sql
from sqlalchemy import create_engine
from pandas.io.sql import to_sql, read_sql

class DatabaseManagement(object):
    """Database Management
    """
    
    
    def __init__(self, user ,password, server, database,
                 dbtype='sqlserver', encoding='latin1', decoding='gbk18030'):
        """Initialization
        
        Params:
        -------
        user : str
            SQL user name
        password : str
            SQL user password
        server : str
            SQL server name
        database : str
            SQL database name
        dbtype : str
            default is 'pymssql'
            valid value {'sqlserver', ''postgre', 'mysql'}
        
        Returns:
        --------
        object handle
        
        """
        
        dbtype = dbtype.lower()
        if dbtype in ['sqlserver', 'postgre']:
            enginestr = (
                'mssql+pymssql://'+user+':'+password+'@'+
                server+'/'+database)
        elif dbtype=='mysql':
            enginestr = (
                'mysql://'+user+':'+password+'@'+
                server+'/'+database)
        else:
            raise ValueError("dbtype: {dbtype} error".format(dbtype=dbtype))
        self._encoding = encoding
        self._decoding = decoding
        self._dbtype = dbtype
        self._database = database
        self._engine = create_engine(enginestr)
        self._config_user_table_property()
    
    def _add_user_table_property(self, tb):
        db = self._database
        con = self._engine
        self.user_table.__setattr__(tb, DatabaseObjectDefinition(
                    table=tb, database=db, engine=con, dbtype=self._dbtype))
    
    def _config_user_table_property(self):
        self.user_table = EmptyClass()
        self._table_name = self.get_table_name_from_database()
        for i in self._table_name:
            self._add_user_table_property(i)

    def get_table_name_from_database(self):
        """Get DB Table's Names
        
        Params:
        -------
            
        Returns:
        --------
        pd.Series
        
        """
        
        dbtype = self._dbtype
        if dbtype in ['sqlserver', 'postgre']:
            sql_str = (
                    "select name as table_name from sysobjects where "
                    "OBJECTPROPERTY(id, N'IsUserTable') = 1")
        elif dbtype=='mysql':
            sql_str = ("select table_name from information_schema.tables where "
            	"table_schema='%(database_name)s' and table_type='base table'"
            	%{'database_name':self._database})
        return read_sql(sql=sql_str, con=self._engine).table_name

class DatabaseObjectDefinition(object):
    """Objects Definition
    """
    
    
    def __init__(self, table, database, engine,
                 dbtype='sqlserver', encoding='latin1', decoding='gb18030'):
        """Initialization
        """
        
        dbtype = dbtype.lower()
        self._dbtype = dbtype
        if dbtype=='sqlserver':
            self._sql_suffix = ' from '+database+'.[dbo].'+table
        elif dbtype=='postgre':
            self._sql_suffix = ' from '+database+'.public.'+table
        elif dbtype=='mysql':
            self._sql_suffix = ' from '+database+'.'+table
        self._table = table
        self._database = database
        self._engine = engine
        self._dbtype = dbtype
        self._encoding = encoding
        self._decoding = decoding
    
    def _decode(self, data):
        if isinstance(data, str):
            data = data.encode(self._encoding).decode(self._decoding)
        else:
            data = data
        return data
    
    def get_column(self):
        """Fetch Columns Names
        
        Params:
        -------
        None
            
        Returns:
        --------
        list of column names
            
        """
        
        dbtype = self._dbtype
        if dbtype in ['sqlserver', 'postgre']:
            return self.get_data(column='top 0 *').columns.tolist()
        elif dbtype=='mysql':
            return self.get_data(column='*', cond=' limit 0').columns.tolist()
        
    def get_data(self, column="*", cond="", convert=False):
        """Read SQL query or database table into a DataFrame.
        
        Params:
        -------
        column : str
            selected columns
        cond : str
            selected condition
            
        Returns:
        --------
        data : dataframe
            selected data
            
        """
        
        suffix = self._sql_suffix
        sqlstr = "select "+column+suffix+cond
        res = read_sql(sqlstr, con=self._engine)
        if convert:
            res = res.applymap(self._decode)
        return res
    
    
    def set_data(self, data):
        """Write records stored in a DataFrame to a SQL database.
        
        Params:
        -------
        column : str
            selected columns
        cond : str
            selected condition
        check_column : list, default is None
            list of columns' names
            
        Returns:
        --------
        None
            
        """
        
        to_sql(data, name=self._table, con=self._engine, 
               if_exists="append", index = False)
    
    def update_data(self, update, cond):
        """Read SQL query to Update a Record.
        
        Params:
        -------
        update : str
            SQL update string
        cond : str
            SQL conditions
            
        Returns:
        --------
        None
            
        """
        
        sqlstr = " UPDATE "+self._table+' SET '+update+' '+cond+' select 1 '
        read_sql(sqlstr, con=self._engine)
            
    def drop_data(self, cond=""):
        """Read SQL query or database table to Drop a DataFrame.
        
        Params:
        -------
        cond : str
            delete condition
            
        Returns:
        --------
        None
            
        """
        
        suffix = self._sql_suffix
        sqlstr = "delete "+suffix+cond+' select 1 '
        if 'where' in cond.lower():
            read_sql(sqlstr, con=self._engine)
        else:
            print("There's no data to delete . (Illegal to Delete All Data)")
    
    def check_record(self, record, cond_add=None):
        """Check a record.
        
        Params:
        -------
        record: dataframe or series
            if dataframe, just update the first raw data
        cond_add: str
        
        Returns:
        --------
        boolean
        
        """
        if isinstance(record, pd.DataFrame):
            record = record.iloc[[0], :]
        if isinstance(record, pd.Series):
            record = record.to_frame().T
        if not isinstance(record, pd.DataFrame):
            raise ValueError('Type of record error.')
        cond = py2sql.df2conds(df=record)
        cond_list = [cond, cond_add]
        cond = py2sql.conditions_joint(conds=cond_list, operator='AND')
        df = self.get_data(column="TOP 1 *", cond=cond)
        return df.index.size > 0
        
    def check_df(self, df, cond_add=None):
        """Check data by a dataframe.
        
        Params:
        -------
        df: dataframe
        cond_add: str
        
        Returns:
        --------
        data: series
        
        """
        res = pd.Series(index=df.index)
        for i in df.index:
            record = df.loc[[i]]
            res.loc[i] = self.check_record(record=record, cond_add=cond_add)
        return res
        
    def check_df_change(self, df, dim, cond_add=None):
        """Check data change or not by a dataframe.
        
        Params:
        -------
        df: dataframe
        dim: list
            dimensions to locate records
        cond_add: str
        
        Returns:
        --------
        data: dataframe
        
        """
        check_df = df.loc[:, dim]
        check_exist = self.check_df(df=check_df, cond_add=cond_add)
        update_df = df[check_exist]
        check_replace = ~self.check_df(df=update_df, cond_add=cond_add)
        change_df = update_df[check_replace].reindex(
            columns=df.columns)
        res = pd.DataFrame(
            index=False, columns=['change', 'not_change', 'not_exist'])
        res[res.index.isin(change_df.index), 'change'] = True
        res[res.index.isin(check_exist[~check_exist].index), 'not_exist'] = True
        res[(~res.not_exist)and(~res.change), 'not_change'] = True
        return res
    
    def update_record(self, record, cond_column, cond_add=None, up_add=None):
        """Update a record. 
        
        Params:
        -------
        record: dataframe or series
            if dataframe, just update the first raw data
        cond_column: str or list
           used to create the conditions of WHERE in db
        cond_add: str
        up_add: str
        
        Returns:
        --------
        None
        
        """
        if isinstance(record, pd.DataFrame):
            record = record.iloc[[0], :]
        if isinstance(record, pd.Series):
            record = record.to_frame().T
        if not isinstance(record, pd.DataFrame):
            raise ValueError('Type of record error.')
        cond = py2sql.df2conds(df=record.loc[:, cond_column])
        cond_list = [cond, cond_add]
        cond = py2sql.conditions_joint(conds=cond_list, operator='AND')
        updata = ','.join(
                ["{f}={v}".format(f=py2sql.deal_field(i),
                 v=py2sql.deal_value(record.iloc[0].loc[i]))
                for i in record.iloc[0].index])
        if up_add:
            updata += ','+up_add
        self.update_data(update=updata, cond=cond)
    
    def update_df(self, df, cond_column, cond_add=None, up_add=None):
        """Update data by a of dataframe with condition columns.
        
        Params:
        -------
        df: dataframe
        cond_column: str or list
            used to create the conditions of WHERE in db
        cond_add: str
        up_add: str
        
        Returns:
        --------
        sn: int
            successful
        fn: int
            failed
        failed: dataframe
        
        """
        sn = 0
        fn = 0
        failed = pd.DataFrame()
        for i in df.index:
            record = df.loc[[i]]
            try:    
                self.update_record(
                    record=record, cond_column=cond_column,
                    cond_add=cond_add, up_add=up_add)
                sn += 1
            except Exception as e:
                fn += 1
                failed = failed.append(record)
        return sn, fn, failed
        
    def fetch_df(self, column, df, cond_add=None, convert=False):
        """Fetch data by a condition of dataframe
        
        * columns of df are query-dimensions
        
        Params:
        -------
        columns: str or list
        df: dataframe
        cond_add: str
        convert: boolean
            decoding or not
        
        Returns:
        --------
        dataframe
        
        """
        cond = ""
        if isinstance(column, list):
            column = py2sql.list2str(data=column, quote=False, output='n')
        elif isinstance(column, str):
            pass
        else:
            raise ValueError("Type of columns must be str or list.")
        cond = py2sql.df2conds(df=df)
        cond_list = [cond, cond_add]
        cond = py2sql.conditions_joint(conds=cond_list, operator='AND')
        return self.get_data(column=column, cond=cond, convert=convert)
    
    def update_cover(self, df, dim, up_add=None, cond_add=None):
        '''update -- cover
        
        Params:
        -------
        df: dataframe
        dim: list
            dimensions to locate the record
        up_add: str
            used to update
            like "column=value"
        cond_add: str
            additional conditions used to find a record to update
        
        Returns:
        --------
        res: series
            index: df.index of records failed to update
            values: error
            
        '''
        failed = pd.Series(index=df.index)
        if isinstance(dim, str):
            dim = [dim]
        if df.index.size > 0:
            for i in df.index:
                try:
                    iv_record = df.loc[[i]]
                    self.update_record(
                            record=iv_record, cond_column=dim,
                            cond_add=cond_add, up_add=up_add)
                except Exception as e:
                    failed.loc[i] = str(e)
        return failed.dropna()
        
    def update_insert(self, df):
        """update -- insert
        
        equals to set_data
        
        Params:
        -------
        df: dataframe
        
        Returns:
        --------
        res: series
            index: df.index of records failed to update
            values: error
        
        """
        failed = pd.Series(index=df.index)
        if df.index.size > 0:
            for i in df.index:
                try:
                    iv_record = df.loc[[i]]
                    self.set_data(data=iv_record)
                except Exception as e:
                    failed.loc[i] = str(e)
        return failed.dropna()
        
    def insert_or_cover(self, df, dim, up_add=None, cond_add=None):
        """update -- insert or cover
        
        if nth record exist:
            cover the record
        else:
            insert record
        
        Params:
        -------
        df: dataframe
        dim: list
            dimensions to locate the record
        up_add: str
            used to update
            like "column=value"
        cond_add: str
            additional conditions used to find a record to update
        
        Returns:
        --------
        insert_record_error: serise
        cover_record_error:series
            
        """
        if isinstance(dim, str):
            dim = [dim]
        check_df = df.loc[:, dim]
        check_exist = self.check_df(df=check_df, cond_add=cond_add)
        insert_df = df[~check_exist]
        update_df = df[check_exist]
        insert_record_error = self.update_insert(df=insert_df)
        cover_record_error = self.update_cover(
            df=update_df, dim=dim, up_add=up_add, cond_add=cond_add)
        return insert_record_error, cover_record_error
        
    def insert_and_replace(self, df, dim, up_add=None, cond_add=None):
        """update -- insert and replace
        
        if nth record exist:
            replace the old record:
                if record change:
                    update old record
                    insert new record
                else:
                    do nothing
        else:
            insert record
        
        Params:
        -------
        df: dataframe
        dim: list
            dimensions to locate the record
        up_add: str
            used to update
            like "column=value"
        cond_add: str
            additional conditions used to find a record to update
        
        Returns:
        --------
        insert_record_error: serise
        cover_record_error:series
            
        """
        if isinstance(dim, str):
            dim = [dim]
        df_stats = self.check_df_change(
            df=df, dim=dim, up_add=up_add, cond_add=cond_add)
        replace_df = df[df_stats.change]
        insert_df = df[df_stats.not_exist|df_stats.change]
        insert_record_error = self.update_insert(df=insert_df)
        cover_record_error = self.update_cover(
            df=replace_df, dim=dim, up_add=up_add, cond_add=cond_add)
        return insert_record_error, cover_record_error
        
        
class EmptyClass(object):
    """User Tables
    
    An empty class.
    
    It's cool.
    
    """


# EOF