import pandas as pd
from pyhive import hive
from sqlalchemy.engine import create_engine
from sqlalchemy.pool import NullPool

from lib.datastructure.config import HIVE_CONFIG
from lib.utils.df_ops import df_split


class HiveUnit:
    def __init__(self, host, port, username, database='default', auth='NOSASL'):
        self.conn = hive.Connection(
            host=host, port=port, username=username, auth=auth)
        self.engine = create_engine(
            "hive://{user_name}@{host}:{port}/{db}".format(user_name=username, host=host, port=port, db=database),
            connect_args={'auth': auth},
            poolclass=NullPool
        )

    def release(self):
        try:
            self.conn.close()
            self.engine.dispose()
        except:
            pass

    def __execute_one(self, query):
        """
        :param query:  hive query
        :return:
        """
        cursor = self.conn.cursor()
        cursor.execute("set hive.execution.engine = tez")
        cursor.execute("set tez.queue.name = sephora_internal")
        cursor.execute(query)

    def execute(self, query: object) -> object:
        """
        Execute hive

        :param query: hive query
        :return: None
        """
        queries = query.split(';')
        for q in queries:
            if q.strip():
                self.__execute_one(q)

    def drop_tab(self, tab_name: str):
        self.execute("set hive.execution.engine = tez")
        self.execute("set tez.queue.name = sephora_internal")
        self.execute("drop table if exists %s" % tab_name)

    def get_df_from_db(self, query):
        """
        This function is going to read date from data base

        :param query: hive query
        :return: pandas data frame
        """
        cursor = self.conn.cursor()
        cursor.execute("set hive.execution.engine = tez")
        cursor.execute("set tez.queue.name = sephora_internal")
        cursor.execute(query)
        data = cursor.fetchall()
        col_des = cursor.description
        col_des = [tuple([x[0].split('.')[1] if '.' in x[0] else x[0]] + list(x[1:])) for x in col_des]
        col_name = [col_des[i][0] for i in range(len(col_des))]
        df = pd.DataFrame([list(i) for i in data], columns=col_name)
        cursor.close()
        return df

    def _get_df_from_db(self, tab_name: str, cols: list or str = "*",
                        condition: str or None = None, limit: int or None = None):
        """
        Load df from db
        :param tab_name: table name
        :param cols: list of columns, if pass "*" will select all columns
        :param condition: selection condition
        :param limit: limit number
        :return:
        """
        cols = ', '.join(cols) if cols != '*' else cols
        sql_query = """SELECT {cols} FROM {tab} """.format(cols=cols, tab=tab_name)
        if condition:
            sql_query += """WHERE {cond} """.format(cond=condition)
        if limit:
            sql_query += """LIMIT {limit}""".format(limit=limit)
        df = pd.read_sql(sql_query, self.engine)
        return df

    def df2db(self, df: pd.DataFrame, tab_name):
        """
        Upload a df to db
        :param df: df to upload
        :param tab_name: table name
        :return: None
        """

        self.execute("set hive.execution.engine = tez")
        self.execute("set tez.queue.name = sephora_internal")
        self.execute("drop table if exists {table_name}".format(table_name=tab_name))
        df.to_sql(tab_name, self.engine, method='multi', index=False)

    def df2db_separate(self, df: pd.DataFrame, tab_name):
        """
        Upload a df to db in separate way
        :param df:
        :param tab_name: table name
        :return: None
        """
        self.execute("set hive.execution.engine = tez")
        self.execute("set tez.queue.name = sephora_internal")
        self.execute("drop table if exists {table_name}".format(table_name=tab_name))

        max_df_size = 50000

        dfs = df_split(df, batch_size=max_df_size)
        num_piece = len(dfs)

        dfs[0].to_sql(tab_name, self.engine, method='multi', index=False)
        if num_piece > 1:
            for pdf in dfs[1:]:
                self.execute("DROP TABLE IF EXISTS {tt}".format(tt=tab_name + '_tmp'))
                pdf.to_sql(tab_name + '_tmp', self.engine, method='multi', index=False)
                self.execute("INSERT INTO TABLE {tn} SELECT * FROM {tt}".format(
                    tn=tab_name, tt=tab_name + '_tmp'
                ))
                print(len(pdf))
            self.execute("DROP TABLE IF EXISTS {tt}".format(tt=tab_name + '_tmp'))


if __name__ == '__main__':
    hive_unit = HiveUnit(**HIVE_CONFIG)
    df = hive_unit.get_df_from_db("select * from da_dev.iris_app_user_id_mapping limit 10")
    print(df)
    hive_unit.release()
