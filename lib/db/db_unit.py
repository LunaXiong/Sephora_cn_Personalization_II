import time

import pandas as pd
import sqlalchemy
from pyhive import presto
import pymssql
import pymysql


class PrestoUnit:
    def __init__(self, username: str, password: str, schema: str = 'default', host: str = 'emra1.harbdata.com',
                 port: int = '10000') -> None:
        self.username = username
        self.password = password
        self.schema = schema
        self.host = host
        self.port = port

    def create_conn_sqlalchemy(self):
        engine_info = 'presto://%s:%s/hive/%s' % (
            self.host, self.port, self.schema)
        print(engine_info)

        # return the engine
        conn = sqlalchemy.create_engine(engine_info)
        return conn

    def create_conn_pyhive(self):
        conn = presto.Connection(host=self.host, port=self.port, username=self.username, schema=self.schema)
        return conn

    def _execute_one(self, query):
        conn = self.create_conn_pyhive()
        cursor = conn.cursor()
        cursor.execute(query)
        response = cursor.fetchall()
        while not response:
            time.sleep(5)
            response = cursor.fetchall()

    def execute(self, query):
        queries = query.split(';')
        for q in queries:
            if q.strip():
                self._execute_one(q)

    def read_sql(self, query):
        query = query.replace('%Y%m%d', '%%Y%%m%%d')
        query = query.replace(';', '')
        conn = self.create_conn_sqlalchemy()
        df = pd.read_sql(query, conn)
        return df

    def to_sql(self, df, name, schema='analyst', if_exists='fail', index=False, **kwargs):
        conn = self.create_conn_sqlalchemy()
        df.to_sql(name, conn, schema=schema, if_exists=if_exists, index=index, **kwargs)

    def read_table(self, name, columns='all', limit=None):
        """
        Read certain table
        :param name: table name
        :param columns: columns(s), list or string
        :param limit: limit of selected rows
        :return:
        """
        if columns == 'all':
            query = 'select * from %s' % name
        elif isinstance(columns, list):
            query = 'select %s from %s' % (','.join(columns), name)
        else:
            query = 'select %s from %s' % (columns, name)

        if limit is not None:
            query = query + ' limit %s ' % limit
        return self.read_sql(query)

    def show_columns(self, name):
        query = 'show columns from %s' % name
        return self.read_sql(query)

    def count(self, name):
        query = 'select count(*) from (%s) t' % name
        return self.read_sql(query)

    def show_tables(self, schema=None, like=''):
        if schema is None:
            schema = self.schema
        query = ''' SHOW TABLES from %s like '%s' ''' % (schema, like)
        return self.read_sql(query)

    def head(self, table_name, limit=5):
        """
        Show the head of certain table
        :param table_name: table name
        :param limit: limit
        :return:
        """
        query = ' select * from %s limit %s ' % (table_name, limit)
        return self.read_sql(query)


class DBUnit():
    dbs = {
        'mysql': pymysql,
        'mssql': pymssql
    }

    def __init__(self, db_type, host, user, password, database, hive_unit=None):
        conn = self.dbs[db_type].connect(host, user, password, database)
        self.engine = conn
        self.hive_unit = hive_unit

    def add_hive_unit(self, hive_unit):
        self.hive_unit = hive_unit

    def get_df(self, sql_cmd):
        return pd.read_sql(sql_cmd, self.engine)

    def to_hive(self, cmd, tab_name):
        if self.hive_unit is None:
            raise RuntimeError("to_hive operation requires hive_unit")
        df = self.get_df(cmd)
        self.hive_unit.df2eb(df, tab_name)

    def to_sql(self, df, name, schema='analyst', if_exists='fail', index=False, **kwargs):
        conn = self.engine
        df.to_sql(name, conn, schema=schema, if_exists=if_exists, index=index, **kwargs)

    def release(self):
        try:
            self.engine.dispose()
        except:
            pass


class MSSQLUnit(DBUnit):
    def __init__(self, **kwargs):
        super(MSSQLUnit, self).__init__('mssql', **kwargs)


class MySQLUnit(DBUnit):
    def __init__(self, **kwargs):
        super(MySQLUnit, self).__init__('mysql', **kwargs)


def test():
    mysql_unit = MySQLUnit(host='10.157.36.11', user='lvmh_da_user', password='Lvmh@_Da_321#', database='product')
    prod_show = mysql_unit.get_df("select table_name from information_schema.tables where table_schema='product'")
    print(prod_show)
    prod_show.to_csv("D:/search_test/prod+list.csv", index=False, encoding='utf_8_sig')
    mysql_unit.release()


if __name__ == '__main__':
    test()