import pandas as pd
import sqlalchemy

from lib.datastructure.config import MYSQL_CONFIG


class MySQLUnit:
    def __init__(self, user: str, password: str, host: str, database: str, port: int = 3306):
        # dialect[+driver]://<user>:<password>@<host>[:<port>]/<dbname>
        engine_info = 'mysql+pymysql://%s:%s@%s:%s/%s' % (
            user, password, host, port, database)
        self.engine = sqlalchemy.create_engine(engine_info, echo=True)
        self.conn = self.engine.connect()

    def release(self):
        try:
            self.conn.close()
            self.engine.dispose()
        except:
            pass


if __name__ == "__main__":
    try:
        mysql_unit = MySQLUnit(**MYSQL_CONFIG['test'])
    except:
        mysql_unit = MySQLUnit(**MYSQL_CONFIG['prod'])
    res = pd.read_sql('show tables', mysql_unit.conn)
    mysql_unit.release()

