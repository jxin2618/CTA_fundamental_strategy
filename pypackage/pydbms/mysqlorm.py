# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 09:47:59 2020

@author: DamonChan
"""

import warnings
from uuid import uuid1
import pandas as pd
from sqlalchemy import create_engine
from pandas.io.sql import to_sql, read_sql, execute
from datapro.typepro import TypePro
from datapro.datepro import DatePro
from itertools import compress

warnings.filterwarnings('ignore')

class MysqlOrm(object):
    
    
    def __init__(self, user, password, host, db,
                 encoding='latin1', decoding='gb18030'):
        """Database connection.
        
        Params:
        -------
        user : str
            user name
        password : str
            user password
        host : str
            host name
        db : str
            database name
            
        
        Returns:
        --------
        object handle
        
        """
        enginestr = (
                'mysql+pymysql://'+user+':'+password+'@'+
                host+'/'+db)
        self._encoding = encoding
        self._decoding = decoding
        self._db = db
        self._engine = create_engine(enginestr)
        self._init_table()

    def _add_table(self, table):
        self.__setattr__(table, MysqlStable(
                    name=table, db=self._db, engine=self._engine,
                    encoding=self._encoding, decoding=self._decoding))
    
    def _init_table(self):
        for i in self._table_name():
            self._add_table(i)
    
    def _table_name(self):
        sql_str = ("select table_name from information_schema.tables where "
            	"table_schema='%(database_name)s' and table_type='BASE TABLE';"
            	%{'database_name':self._db})
        return read_sql(sql=sql_str, con=self._engine).iloc[:, 0]

    def _uuid(self):
        return str(uuid1()).upper()


class MysqlStable(object):
    
    
    def __init__(self, name, db, engine, encoding, decoding):
        self._table = name
        self._db = db
        self._engine = engine
        self._encoding = encoding
        self._decoding = decoding

    def _decode(self, data):
        if isinstance(data, str):
            data = data.encode(self._encoding).decode(self._decoding)
        else:
            data = data
        return data

    def _update_time(self):
        return "update_time={udt}".format(udt=self._current_time())
    
    def _create_time(self):
        return "create_time={udt}".format(udt=self._current_time())

    def _current_time(self):
        return "CURRENT_TIMESTAMP"
    
    def _uuid(self):
        return str(uuid1()).upper()

    def _get_suffix(self):
        return " from {table} ".format(db=self._db, table=self._table)

    def _create_select_sqlstr(self, column, cond):
        sqlstr = "SELECT {column} {suffix} {cond}".format(
                column=column, suffix=self._get_suffix(), cond=cond)
        return sqlstr
    
    def _create_update_sqlstr(self, update, cond):
        sqlstr = "UPDATE {table} SET {update} {cond}; ".format(
                table=self._table, update=update, cond=cond)
        return sqlstr
    
    def _create_delete_sqlstr(self, cond):
        sqlstr = "DELETE {subffix} {cond} ;".format(
                subffix=self._get_suffix(), cond=cond)
        return sqlstr
    
    def execute_sql(self, sql):
        res = execute(sql=sql, con=self._engine)
        res.close()

    def get_column(self):
        return self.get_data(column='*', cond=' limit 0 ').columns.tolist()

    def get_data(self, column, cond, convert=False):
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
        
        sqlstr = self._create_select_sqlstr(column=column, cond=cond)
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
        return to_sql(data, name=self._table, con=self._engine, 
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
        
        sqlstr = self._create_update_sqlstr(update=update, cond=cond)
        return self.execute_sql(sql=sqlstr)
    
    def delete_data(self, cond):
        """Read SQL query or database table to Drop a DataFrame.
        
        Params:
        -------
        cond : str
            delete condition
            
        Returns:
        --------
        None
        
        """
        
        sqlstr = self._create_delete_sqlstr(cond=cond)
        if 'where' in cond.lower():
            return self.execute_sql(sql=sqlstr)
        else:
            raise ValueError("There's no data to delete . (Illegal to Delete All Data)")

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
        
        record = Py2Mysql.standardize_record(record=record)
        cond = Py2Mysql.df2conds(df=record)
        cond_list = [cond, cond_add]
        cond = Py2Mysql.conditions_joint(conds=cond_list, operator='AND')
        cond += ' LIMIT 1 ;'
        df = self.get_data(column=" * ", cond=cond)
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
        record = Py2Mysql.standardize_record(record=record)
        cond = Py2Mysql.df2conds(df=record.loc[:, cond_column])
        cond_list = [cond, cond_add]
        cond = Py2Mysql.conditions_joint(conds=cond_list, operator='AND')
        updata = ','.join(
                ["{f}={v}".format(f=Py2Mysql.deal_field(i),
                 v=Py2Mysql.deal_value(record.iloc[0].loc[i]))
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
        for i in df.index:
            record = df.loc[[i]]
            self.update_record(
                record=record, cond_column=cond_column,
                cond_add=cond_add, up_add=up_add)

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
            column = Py2Mysql.list2str(data=column, quote=False, output='n')
        elif isinstance(column, str):
            pass
        else:
            raise ValueError("Type of columns must be str or list.")
        cond = Py2Mysql.df2conds(df=df)
        cond_list = [cond, cond_add]
        cond = Py2Mysql.conditions_joint(conds=cond_list, operator='AND')
        return self.get_data(column=column, cond=cond, convert=convert)

    def insert_or_update(self, df, dim, up_add=None):
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
        
        Returns:
        --------
        insert_record_error: serise
        cover_record_error:series
            
        """
        sqlstr = ("insert into {table} {column} values {value} "
                  "on duplicate key update {update_column} {up_add};")
        if up_add:
            up_add = ", " + up_add
        else:
            up_add = ""
        if df.index.size>0:
            column, value = Py2Mysql.df2tuple(df=df)
            update_column = ', '.join(df.columns.drop(dim).map(
                    Py2Mysql.deal_field).map(
                            lambda x: "{x}=values({x})".format(x=x)).tolist())
            sqlstr = sqlstr.format(table=self._table, column=column,
                          value=value, update_column=update_column, up_add=up_add)
            self.execute_sql(sql=sqlstr)


    def check_slowly_change_record(
            self, record, dim, date, left, right,
            quote=False, include='both'):
        """Check a slowly changed record.
        
        Params:
        -------
        record: dataframe or series
            if dataframe, just update the first raw data
        dim: str or list
           used to create the conditions of WHERE in db
          date: str, datetime
          left: str
          right: str
          quote: boolean
          include: str
              * left: <= column <
              * right: < column <=
              * both: <= column <=
              * neither < column <
        
        Returns:
        --------
        res: int
            * 0: not exists
            * 1: exist bot not change
            * 2: exist and change
        
        """
        record = Py2Mysql.standardize_record(record=record)
        check_df = record.loc[:, dim]
        cond_add = Py2Mysql.slowly_change_between(
                value=date, left=left, right=right,
                quote=quote, include=include)
        exist = self.check_record(record=check_df, cond_add=cond_add)
        if not exist:
            res = 0
        else:
            not_change = self.check_record(record=record, cond_add=cond_add)
            if not_change:
                res = 1
            else:
                res = 2
        return res

    def _get_slowly_change_record(self, record, dim, date, left, right,
                                  quote=False, include='both', shift=0):
        record = Py2Mysql.standardize_record(record=record)
        date = DatePro.date_shift(date, days=shift)
        cond_add = Py2Mysql.slowly_change_between(
                value=date, left=left, right=right,
                quote=quote, include=include)
        res = self.fetch_df(
                column=[left, right]+dim, df=record.loc[:, dim],
                cond_add=cond_add, convert=True)
        return res

    def _insert_or_update_slowly_change_record(
            self, record, dim, date, left, right,
            quote=False, include='both'):
        """Insert or update a slowly changed record.
        
        Params:
        -------
        record: dataframe or series
            if dataframe, just update the first raw data
        dim: str or list
           used to create the conditions of WHERE in db
          date: str, datetime
          left: str
          right: str
          quote: boolean
          include: str
              * left: <= column <
              * right: < column <=
              * both: <= column <=
              * neither < column <
        
        Returns:
        --------
        insert_record: dataframe
        update_record: dict
        
        """
        record = Py2Mysql.standardize_record(record=record)
        check_type = self.check_slowly_change_record(
                record=record, dim=dim, date=date,
                left=left, right=right,
                quote=quote, include=include)
        check_type_m1 = self.check_slowly_change_record(
                record=record, dim=dim, date=DatePro.date_shift(date, days=-1),
                left=left, right=right,
                quote=quote, include=include)
        empty_record = pd.DataFrame()
        if check_type==0:
            # 当天记录不存在
            if check_type_m1==1:
                # * 上一天存在相同记录
                #   修改上一天记录的end=date，当前不新增
                insert_record = empty_record()
                update_record_old = self._get_slowly_change_record(
                    record=record, dim=dim, date=date,
                    left=left, right=right,
                    quote=quote, include=include, shift=-1)
                update_record_new = update_record_old.assign(**{right: date})
            elif check_type_m1 in [0, 2]:
                # * 上天不存在或存在不同记录
                #   新增
                insert_record = record.assign(**{
                    left: date, right: '9999-12-31'})
                update_record_old = empty_record
                update_record_new = empty_record
            else:
                raise ValueError("Unkonw.")
            update_record = [(update_record_old, update_record_new)]
        elif check_type==1:
            # 当天记录存在且相同
            insert_record = empty_record
            update_record_old = empty_record
            update_record_new = empty_record
            update_record = [(update_record_old, update_record_new)]
        elif check_type==2:
            # 当天记录存在，且不同
            if check_type_m1==1:
                # * 上一天存在相同记录
                #   修改上一天记录的end=date，当前记录start=date+1
                insert_record = empty_record
                update_record_old0 = self._get_slowly_change_record(
                    record=record, dim=dim, date=date,
                    left=left, right=right,
                    quote=quote, include=include, shift=-1)
                update_record_new0 = update_record_old0.assign(**{right: date})
                update_record_old1 = self._get_slowly_change_record(
                    record=record, dim=dim, date=date,
                    left=left, right=right,
                    quote=quote, include=include, shift=0)
                update_record_new1 = update_record_old1.assign(**{
                        left: DatePro.date_shift(date, days=1)})
            elif check_type_m1 in [0, 2]:
                # * 上天不存在或存在不同记录
                #   新增,当天记录end=end-1
                insert_record = record.assign(**{
                    left: date, right: '9999-12-31'})
                update_record_old0 = empty_record
                update_record_new0 = empty_record
                update_record_old1 = self._get_slowly_change_record(
                    record=record, dim=dim, date=date,
                    left=left, right=right,
                    quote=quote, include=include, shift=0)
                update_record_new1 = update_record_old1.assign(**{
                    right: DatePro.date_shift(date, days=-1)})
            else:
                raise ValueError("Unkonw.")
            update_record = [(update_record_old0, update_record_new0),
                             (update_record_old1, update_record_new1)]
        return insert_record, update_record
    
    def insert_or_update_slowly_change_record(
            self, record, dim, date, left, right,
            quote=False, include='both', uuid_column='object'):
        """Insert or update a slowly changed record.
        
        Params:
        -------
        record: dataframe or series
            if dataframe, just update the first raw data
        dim: str or list
           used to create the conditions of WHERE in db
        date: str, datetime
        left: str
        right: str
        quote: boolean
        include: str
          * left: <= column <
          * right: < column <=
          * both: <= column <=
          * neither < column <
           
        """
        
        insert_record, update_record = \
            self._insert_or_update_slowly_change_record(
                record=record, dim=dim, date=date,
                left=left, right=right,
                quote=quote, include=include)
    
    def insert_or_update_slowly_change_df(
            self, df, dim, date, left, right,
            quote=False, include='both'):
        """Insert or update  slowly changed records in df.
        
        Params:
        -------
        record: dataframe or series
            if dataframe, just update the first raw data
        dim: str or list
           used to create the conditions of WHERE in db
          date: str, datetime
          left: str
          right: str
          quote: boolean
          include: str
              * left: <= column <
              * right: < column <=
              * both: <= column <=
              * neither < column <
        """
        insert_records = pd.DataFrame()
        update_records = []
        for k, v in df.iterrows():
            insert_record, update_record = \
                self._insert_or_update_slowly_change_record(
                    record=v, dim=dim, date=date,
                    left=left, right=right,
                    quote=quote, include=include)
            insert_records = insert_records.append(
                    insert_record, ignore_index=True, sort=False)
            update_records.extend(update_record)
        for i in update_records:
            self.replace_record(i[0], i[1], up_add=self._update_time())
        self.insert_or_update(df=insert_records, dim=dim,
                              up_add=self._update_time())

    def replace_record(self, old, new, cond_add=None, up_add=None):
        if old.index.size>0 and new.index.size>0:
            old = Py2Mysql.standardize_record(record=old)
            new = Py2Mysql.standardize_record(record=new)
            cond_list = [Py2Mysql.df2conds(old), cond_add]
            cond = Py2Mysql.conditions_joint(conds=cond_list, operator='AND')
            updata = ','.join(
                    ["{f}={v}".format(f=Py2Mysql.deal_field(i),
                     v=Py2Mysql.deal_value(new.iloc[0].loc[i]))
                    for i in new.iloc[0].index])
            if up_add:
                updata += ','+up_add
            self.update_data(update=updata, cond=cond)
        


class Py2Mysql(object):
    
    
    @classmethod
    def deal_field(cls, x):
        left = "`"
        right = "`"
        if not (x[0] == left and x[-1] == right):
            x = "{l}{x}{r}".format(x=x, l=left, r=right)
        return x

    @classmethod
    def deal_value(cls, x):
        if TypePro.check_type(x, str):
            x = "'{x}'".format(x=x.replace("'", "''"))
        elif TypePro.check_datetime(x):
            if TypePro.check_null(x):
                x = 'NULL'
            else:
                x = "'{x}'".format(x=x)
        elif TypePro.check_bool(x):
            x = "{x}".format(x=int(x))
        elif TypePro.check_real(x):
            if TypePro.check_null(x):
                x = 'NULL'
            else:
                x = str(x)
        else:
            x = str(x)
        return x
    
    @classmethod
    def list2str(cls, data, quote=True, output='l'):
        """Python list to sql str
    
        Params:
        -------
        data: str, list, tuple, series, dataframe(1-d)
        quote: boolean
            * True: "x" -> "'x'"
            * False: "x" -> "x"
        output: str, valid in {'l', 't', 'n'}
            * l: "[{data}]"
            * t: "({data})"
            * n: "{data}"
    
        Returns:
        --------
        str
    
        """
        data = TypePro.to_list(data)
        output = output.upper()
        data = str(data)
        data = data.replace(',', '') if len(data) == 1 else data
        data = data.replace("'", '') if not quote else data
        if output == 'T':
            data = data.replace('[', '(').replace(']', ')')
        elif output == 'N':
            data = data[1:-1]
        return data

    @classmethod
    def list2colin(cls, column, data, table=None):
        """Python list to sql str: <column> in (data)
    
        Params:
        -------
        data: str, list, tuple, series, dataframe(1-d)
    
        Returns:
        --------
        str
    
        """
        data = TypePro.to_list(data)
        l = len(data)
        column = cls.deal_field(column)
        column = table + '.' + column \
            if table and TypePro.check_type(table, str) \
            else column
        data = ','.join([cls.deal_value(i) for i in data])
        if l > 1:
            sql = "{k} in ({v})".format(k=column, v=data)
        elif l == 1:
            if data.upper() != 'NULL':
                sql = "{k}={v}".format(k=column, v=data)
            else:
                sql = "{k} is {v}".format(k=column, v=data)
        else:
            sql = ''
        return sql

    @classmethod
    def conditions_joint(cls, conds, operator='AND', add_where=True):
        """Sql query conditions joint.
    
        Params:
        -------
        conds: list, series, dataframe(1-d)
        operator: str, valid in {'and', 'or'}
        add_where: boolean
             * True: " WHERE cond1 operator cond2 ..."
             * False: " cond1 operator cond2 ..."
    
        Returns:
        --------
        str
    
        """
        conds = TypePro.to_list(conds)
        operator = ' '+operator+' '
        selector = [i.replace(' ', '') != '' if i else False for i in conds]
        conds = list(compress(conds, selector))
        res = operator.join(conds) if conds else ''
        if res.replace(' ', '') and add_where:
            res = ' WHERE ' + res
        return res

    @classmethod
    def df2conds(cls, df):
        """dataframe to sql query conditions
    
        Params:
        -------
        df: dataframe
    
        Returns:
        --------
        str
    
        """
        cond = ""
        if isinstance(df, pd.DataFrame):
            df = df.fillna('NULL')
            if df.index.size > 0:
                if df.columns.size > 1:
                    sqlstr = df.apply(
                            lambda x: cls.deal_field(x.name)
                            + '=' + x.apply(
                                cls.deal_value)).apply(
                                lambda x: x + ' AND ', axis=1).sum(axis=1).apply(
                                lambda x: '( ' + x[:-4]+')'+' OR ').sum()[:-3]
                    cond = ' ( '+sqlstr+') '
                elif df.columns.size == 1:
                    cond = cls.list2colin(
                        column=df.columns[0],
                        data=df.iloc[:, 0].tolist())
        else:
            raise ValueError("Type of df is not a DataFrame.")
        return cond.replace("='NULL'", ' IS NULL')

    @classmethod
    def between(cls, column, left=None, right=None, quote=True, include='both'):
        """sql between condition
    
        Params:
        -------
        column: str
        left: any value
        right: any value
        quote: boolean
            * True: "x" -> "'x'"
            * False: "x" -> "x"
        include: str, valid in {'left', 'right', 'both', 'neither'}
            * left: <= column <
            * right: < column <=
            * both: <= column <=
            * neither < column <
    
        Returns:
        --------
        str
    
        """
        nleft = ('>=' if include.lower() in ['both', 'left'] else '>')
        nright = ('<=' if include.lower() in ['both', 'right'] else '<')
        quote_mark = "'" if quote else ""
        sql_left = "{col}{nl}{q}{l}{q}".format(
            col=column, nl=nleft, q=quote_mark, l=left)
        sql_right = "{col}{nr}{q}{r}{q}".format(
            col=column, nr=nright, q=quote_mark, r=right)
        if left and right:
            sql = "{sl} AND {sr}".format(sl=sql_left, sr=sql_right)
        elif left:
            sql = sql_left
        elif right:
            sql = sql_right
        return sql

    @classmethod
    def replace_special_symbol(cls, x, replace=' '):
        if TypePro.check_type(x, str):
            x = replace.join(x.split())
        return x
    
    @classmethod
    def standardize_record(cls, record):
        if isinstance(record, pd.DataFrame):
            record = record.iloc[[0], :]
        elif isinstance(record, pd.Series):
            record = record.to_frame().T
        else:
            raise ValueError('Type of record error.')
        return record
    
    @classmethod
    def df2tuple(cls, df):
        """
        
        Params:
        -------
        df: dataframe
        
        Returns:
        --------
        column: str
        value: str
        
        """
        value = ', '.join(df.apply(lambda x: '('+', '.join(
                x.apply(cls.deal_value).tolist())+')', axis=1).tolist())
        column = '('+', '.join(df.columns.map(cls.deal_field).tolist())+')'
        return column, value

    @classmethod
    def slowly_change_between(cls, value, left=None, right=None,
                              quote=False, include='both'):
        """
        """
        left = cls.deal_field(left)
        right = cls.deal_field(right)
        value = cls.deal_value(value)
        res = cls.between(column=value, left=left, right=right,
                          quote=quote, include=include)
        return res


# EOF
