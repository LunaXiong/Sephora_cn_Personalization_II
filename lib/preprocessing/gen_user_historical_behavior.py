from datetime import date, timedelta

import pandas as pd

from lib.datastructure.config import HIVE_CONFIG
from lib.db.hive_utils import HiveUnit

SEARCH_BEHAVIOR_ONLINE_TABLE = 'da_dev.search_behavior_online'
SEARCH_BEHAVIOR_ONLINE_OPENID_TABLE = 'da_dev.search_behavior_online_open_id'
SEARCH_BEHAVIOR_OFFLINE_OPENID_TABLE = 'da_dev.search_behavior_offline_open_id'
SEARCH_HISTORICAL_QUERY_TABLE = 'da_dev.historical_query_90d'
SEARCH_HISTORICAL_UESR_TABLE = 'da_dev.user_sample_0601_0901'


def get_user_item(hive_unit: HiveUnit, days: int = 180):
    """
    generate online user behaviors in last several days
    used for training item embedding model
    :param hive_unit
    :param days
    :return user_item_df, [user_id, time, op_code]
    """
    today = date.today()
    start_day = today + timedelta(days=-days)
    query = r"""
    select open_id as user_id, time, op_code
    from {table}
    where time between '{start_date}' and '{end_date}'
    and op_code <>'' and op_code <>'NULL'
    and platform='MiniProgram'
    order by user_id, time
    """.format(table=SEARCH_BEHAVIOR_ONLINE_TABLE,
               start_date=str(start_day), end_date=str(today))
    user_item_df = hive_unit.get_df_from_db(query)
    user_item_df.dropna(inplace=True)
    return user_item_df


def gen_user_behavior(hive_unit: HiveUnit, online_days: int = 7, offline_days=180):
    """
    generate online and offline user behavior in last several days
    used for generate user2item
    :param offline_days:
    :param online_days:
    :param hive_unit
    :return user_item_df, [user_id, op_code, behavior]
    """
    today = date.today()
    online_start_day = today + timedelta(days=-online_days)
    offline_start_day = today + timedelta(days=-offline_days)
    query = r"""
    select open_id as user_id, op_code, behavior
    from {table}
    where time between '{start_date}' and '{end_date}'
    and op_code <>'' and op_code <>'NULL'
    """
    # online user behavior
    online_query = query.format(table=SEARCH_BEHAVIOR_ONLINE_OPENID_TABLE,
                                start_date=str(online_start_day),
                                end_date=str(today))
    online_user_item = hive_unit.get_df_from_db(online_query)
    online_user_item.dropna(inplace=True)
    print('online', online_user_item.info())
    # offline user behavior
    offline_query = query.format(table=SEARCH_BEHAVIOR_OFFLINE_OPENID_TABLE,
                                 start_date=str(offline_start_day),
                                 end_date=str(today))
    offline_user_item = hive_unit.get_df_from_db(offline_query)
    offline_user_item.dropna(inplace=True)
    print('offline', offline_user_item.info())
    user_item_df = pd.concat([online_user_item, offline_user_item])
    return user_item_df


def get_query_df(hive_unit: HiveUnit, days: int = 180):
    """get historical queries"""
    query = r"""
    select distinct query as keyword
    from {table}
    """.format(table=SEARCH_HISTORICAL_QUERY_TABLE)
    query_df = hive_unit.get_df_from_db(query)
    return query_df


def get_user_session(hive_unit: HiveUnit):
    """
    :return user_item_df, [session_id, user_id, op_code, time]
    """
    query = r"""
    select * from da_dev.user_sample
    """
    user_session_df = hive_unit.get_df_from_db(query).drop(columns=['key_words']).dropna()
    print(user_session_df)
    # raw_df = pd.read_csv('./data/user_session.csv').drop(columns=['key_words']).dropna()
    user_session_df['op_code'] = user_session_df['op_code'].astype('str')
    user_session_df['user_id'] = user_session_df['user_id'].astype('str')
    user_session_df.dropna(inplace=True)
    print(user_session_df)
    return user_session_df


if __name__ == '__main__':
    hive_unit = HiveUnit(**HIVE_CONFIG)
    get_user_session(hive_unit)
    hive_unit.release()