import json
import os
import pyodbc

import pandas as pd

default_creds_path = os.getcwd() + "\\mcauto\\config\\adi_database_creds.json"


def create_database(account: str, creds_path=default_creds_path, do_connect=False):
    with open(creds_path) as creds_file:
        creds = json.load(creds_file)

    if account == 'Adidas':
        return AdidasDatabase(**creds, do_connect=do_connect)
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


class AdidasDatabase(BaseDatabase):
    def __init__(self, server, database, uid, pwd, do_connect=False):
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
        signin_string = "Driver={SQL Server Native Client 11.0};Server=%s;Database=%s;uid=%s;pwd=%s" % (
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
