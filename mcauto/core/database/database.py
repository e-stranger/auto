import json
import os
import pyodbc
import sqlalchemy
import pandas as pd
import urllib
import re

adi_default_creds_path = "C:\\Users\\john.atherton\\PycharmProjects\\mcauto\\mcauto\\config\\adi_database_creds.json"
with open(adi_default_creds_path) as creds_file:
    adi_creds = json.load(creds_file)

def create_database(account: str, creds_path: str = adi_default_creds_path, do_connect=False, use_sqlalchemy: bool=False):
    with open(creds_path) as creds_file:
        creds = json.load(creds_file)

    if account == 'Adidas':
        if use_sqlalchemy:
            return SQLAlchemyUtils.get_sqlalchemy_engine('Adidas')
        else:
            return AdidasPyodbcDatabase(**creds, do_connect=do_connect)

    else:
        raise ValueError('Database class is not implemented for %s' % account)


"""
Base class for SQL/relational database with stored procedure functionality. 
The below class methods MUST be defined in any subclass.
"""

class BaseDatabase:
    def connect(self):
        raise NotImplementedError('Implement method in subclass')

    def cursor(self):
        raise NotImplementedError('Implement method in subclass')

    def execute(self, command):
        raise NotImplementedError('Implement method in subclass')

    def execute_procedure(self, proc_name, **kwargs):
        raise NotImplementedError('Implement method in subclass')


class AdidasPyodbcDatabase(BaseDatabase):
    signin_string = "Driver={SQL Server Native Client 11.0};Server=%s;Database=%s;uid=%s;pwd=%s"
    def __init__(self, server, database, uid, pwd, do_connect=False, use_sqlalchemy=False):
        self.server = server
        self.database = database
        self.uid = uid
        self.pwd = pwd

        if do_connect:
            self.connect()

    def __del__(self):

        if hasattr(self, 'curs'):
            try:
                self.curs.close()
            except:
                pass
        if hasattr(self, 'conn'):
            try:
                self.conn.close()
            except:
                pass

    def connect(self):
        signin_string = self.signin_string % (
            self.server, self.database, self.uid, self.pwd)
        self.conn = pyodbc.connect(signin_string)

    def cursor(self):
        if not hasattr(self, 'conn'):
            self = self.connect()
        self.curs = self.conn.cursor()
        return self.curs

    def execute(self, command):
        if not hasattr(self, 'conn'):
            print('Connection not yet mad. Connecting..')
            self.connect()
        try:
            df = pd.read_sql(command, self.conn)
            return df
        except pd.io.sql.DatabaseError as e:
            raise pd.io.sql.DatabaseError('failed on execute') from e

    def execute_procedure(self, proc_name, **kwargs):
        command = format_procedure(proc_name, **kwargs)
        return self.execute(command)


def format_procedure(proc_name, since_date=None, **kwargs):
    command = 'EXEC ' + proc_name
    sql_kwargs = []
    if kwargs:

        for k, v in kwargs.items():
            if isinstance(v, str):
                sql_kwargs.append(f"@{k} = '{v[1]}'")
            elif isinstance(v, int):
                sql_kwargs.append(f"@{k} = {v}")
            elif isinstance(v, float):
                ...
        sql_kwargs = ", ".join(sql_kwargs)
        command = command + ' ' + sql_kwargs
    elif since_date:
        command = command + f" @sinceDate = '{since_date}'"
    print(command)
    return command

class SQLAlchemyUtils():
    illegal_chars = ['.', ':', '/']

    def __init__(self):
        self.engine = SQLAlchemyUtils.get_sqlalchemy_engine()

    def reconnect(self):

        self.engine = SQLAlchemyUtils.get_sqlalchemy_engine()

    @staticmethod
    def get_sqlalchemy_engine(who='Adidas'):
        signin_string = "Driver={SQL Server Native Client 11.0};Server=%s;Database=%s;uid=%s;pwd=%s"
        if who.lower() == 'adidas':
            fmt_signin_str = signin_string % (adi_creds['server'],
                                              adi_creds['database'],
                                              adi_creds['uid'],
                                              adi_creds['pwd'])
            params = urllib.parse.quote_plus(fmt_signin_str)
            engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany=True, echo=True)
            return engine

    def insert_clean_df(self, df: pd.DataFrame, name: str, if_exists: str = 'append', nocount: bool = False, do_truncate: bool = False, drop_columns: list = None):

        df = SQLAlchemyUtils.clean_col_names(df)

        if drop_columns:
            df = df.drop(columns=drop_columns, errors='ignore')

        SQLAlchemyUtils.check_columns_compatible(columns=df.columns, table_name=name)

        if nocount:
            self.engine.execute('SET NOCOUNT ON;')
        else:
            self.engine.execute('SET NOCOUNT OFF;')

        if do_truncate:
            conn = self.engine.connect()
            trans = conn.begin()
            r1 = conn.execute('TRUNCATE TABLE %s' % (name))
            trans.commit()
            # do_truncate = input(f'Do you want to truncate {name}? y/n')
            #
            # if do_truncate.lower() == 'y':
            #     conn = self.engine.connect()
            #     trans = conn.begin()
            #     r1 = conn.execute('TRUNCATE TABLE %s' % (name))
            #     trans.commit()
            #
            # else:
            #     print('Not truncating. Continuing...')




        return df.to_sql(name=name, con=self.engine, if_exists=if_exists, index=False)

    @staticmethod
    def check_columns_compatible(columns, table_name, who='Adidas'):
        if not isinstance(columns, list):
            columns = list(columns)
        engine = SQLAlchemyUtils.get_sqlalchemy_engine(who)

        try:
            db_name = who.upper()
            with engine.connect() as conn:
                res = conn.execute(f"SELECT DISTINCT COLUMN_NAME FROM {db_name}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
                columns_from_sql = [i[0].upper() for i in res.fetchall()]
                for column in columns:
                    if column.upper() not in columns_from_sql:
                        raise ValueError(f"{column} not in {columns_from_sql}")
        except:
            raise ValueError

    @staticmethod
    def clean_col_names(df: pd.DataFrame):
        return df.rename(SQLAlchemyUtils.sql_col_name_format, axis=1)

    @staticmethod
    def sql_col_name_format(col_name: str):
        for char in SQLAlchemyUtils.illegal_chars:
            col_name = col_name.replace(char, ' ')
        return col_name

class DBClassMixin(SQLAlchemyUtils):
    def __init__(self, table_name, do_truncate, drop_columns):
        super().__init__()
        self.table_name = table_name
        self.do_truncate = do_truncate
        self.drop_columns = drop_columns


    def insert(self):
        if not hasattr(self, 'data'):
            print('Ya need data!')
            return
        elif not hasattr(self, 'do_truncate'):
            print('Ya need do_truncate!')
            return
        elif not hasattr(self, 'table_name'):
            print('Ya need table_name!')
            return
        if not hasattr(self, 'drop_columns'):
            self.drop_cols = []

        self.insert_clean_df(self.data, self.table_name, do_truncate=self.do_truncate, drop_columns=self.drop_columns)

