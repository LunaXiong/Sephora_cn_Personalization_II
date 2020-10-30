from datetime import datetime

import pandas as pd

from lib.datastructure.config import MYSQL_CONFIG
from lib.datastructure.files import USER2KW
from lib.utils.utils import load_json
from lib.db.mysql_unit import MySQLUnit

# default keywords
user2kw = load_json(USER2KW)
user2kw['default'] = ['香水套装', '周末秒杀/周周有惊喜', '丝芙兰好物精选', '秋日底妆盘点', '丝芙兰', '抗初老',
                      '眉部彩妆', '彼得罗夫', '保湿补水', '保湿', '美容仪', '丝芙兰美容仪']

user2kw = pd.DataFrame.from_dict(user2kw, orient='index').reset_index().rename(columns={'index': 'open_id'})
user2kw['default_key'] = user2kw[user2kw.columns[1]]
user2kw['default_hot'] = user2kw[user2kw.columns[2:]].apply(lambda x: ','.join(x.dropna()), axis=1)
user2kw['create_time'] = datetime.now()
user2kw['update_time'] = datetime.now()
user2kw = user2kw[['open_id', 'default_key', 'default_hot', 'create_time', 'update_time']]
mysql_unit = MySQLUnit(**MYSQL_CONFIG['prod'])
user2kw.to_sql(name='t_search_default',
               con=mysql_unit.conn,
               if_exists='replace')
mysql_unit.release()
