"""
Created on Mon Oct 29 08:31:30 2018

@author: Chandeman
"""

import pandas as pd
from datetime import datetime
from pandas import DataFrame, np
from itertools import compress
    
def deal_value(x):
    if isinstance(x, str):
        x = "'{x}'".format(x=x.replace("'", "''"))
    elif isinstance(x, datetime):
        x = "'{x}'".format(x=x)
    elif (isinstance(x, bool) or isinstance(x, pd.np.bool)
          or isinstance(x, pd.np.bool8) or isinstance(x, pd.np.bool_)):
        x = "{x}".format(x=int(x))
    elif np.isreal(x):
        if isinstance(x, pd._libs.tslib.NaTType):
            x = 'NULL'
        elif np.isnan(x):
            x = 'NULL'
        else:
            x = x
    else:
        x = str(x)
    return x
    
def deal_field(x):
    if not (x[0]=='[' and x[-1]==']'):
        x = "[{x}]".format(x=x)
    return x
    
def list2str(
        data:list, quote: bool=True,
        output: ['l', 't', 'n']='l'
        )->str:
    output = output.upper()
    data = str(data)
    data = data.replace(',','') if len(data)==1 else data
    data = data.replace("'",'') if not quote else data
    if output=='T':
        data = data.replace('[', '(').replace(']', ')')
    elif output=='N':
        data = data[1:-1]
    return data

def list2colin(column: str, data: list, quote: bool=True)->str:
    l = len(data)
    column = deal_field(column)
    data = ','.join([deal_value(i) for i in data])
    if l > 1:
        sql = "{k} in ({v})".format(k=column, v=data)
    elif l == 1:
        sql = "{k}={v}".format(k=column, v=data)
    else:
        sql = ''
    return sql

def conditions_joint(
        conds: list,
        operator: ['AND', 'OR']='AND',
        add_where=True)->str:
    operator = ' '+operator+' '
    selector = [i.replace(' ','')!='' if i else False for i in conds]
    conds = list(compress(conds, selector))
    res = operator.join(conds) if conds else ''
    if res.replace(' ', '') and add_where:
        res = ' WHERE ' + res
    return res

def df2conds(df: DataFrame)->str:
    cond = ""
    if isinstance(df, DataFrame):
        df = df.fillna('NULL')
        if df.index.size > 0:
            if df.columns.size > 1:
                sqlstr = df.apply(
                    lambda x:deal_field(x.name)+'='+x.apply(deal_value)).apply(
                    lambda x:x+' AND ',axis=1).sum(axis=1).apply(
                    lambda x:'( '+x[:-4]+')'+' OR ').sum()[:-3]
                cond = ' ( '+sqlstr+') '
            elif df.columns.size == 1:
                cond = list2colin(
                    column=df.columns[0],
                    data=df.iloc[:, 0].tolist(),
                    quote=False)
    else:
        raise ValueError("Type of df is not a DataFrame.")
    return cond.replace("='NULL'", ' IS NULL')
    
def between(column: str, left=None, right=None, quote: bool=True,
            include: ['both', 'left', 'right', 'none']='both')->str:
    nleft = ('>=' if include.lower() in ['both', 'left'] else '>')
    nright = ('<=' if include.lower() in ['both', 'right'] else '<')
    sql_left = "{col}{nl}{q}{l}{q}".format(
        col=column, nl=nleft, q=quote)
    sql_right = "{col}{nr}{q}{r}{q}".format(
        col=column, nr=nright, q=quote)
    if left and right:
        sql = "{sl} AND {sr}".format(sl=sql_left, sr=sql_right)
    elif left:
        sql = sql_left
    elif right:
        sql = sql_right
    return sql

def type_transform(x, type_func):
    try:
        x = type_func(x)
    except:
        x = x
    return x

def replace_special_symbol(x, replace=' '):
    if isinstance(x, str):
        x = replace.join(x.split())
    return x


# EOF