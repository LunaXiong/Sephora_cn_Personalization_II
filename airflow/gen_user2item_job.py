"""
23 minutes
"""
from datetime import datetime

from lib.datastructure.config import HIVE_CONFIG
from lib.db.hive_utils import HiveUnit
from lib.model.linking import gen_user2item
from lib.preprocessing.gen_user_historical_behavior import gen_user_behavior


print(datetime.now(), 'generate last week user behavior data start...')
hive_unit = HiveUnit(**HIVE_CONFIG)
user_behavior_df = gen_user_behavior(hive_unit)
hive_unit.release()
print(datetime.now(), 'generate last week user behavior data done')
print(datetime.now(), 'generate user2item start...')
gen_user2item(behavior_df=user_behavior_df)
print(datetime.now(), 'generate user2item done')

