from datetime import datetime
import sys
import os

print(os.getcwd())
sys.path.append('./')
from lib.datastructure.config import HIVE_CONFIG
from lib.db.hive_utils import HiveUnit

from lib.processing.gen_feature import FeatureGenerator, gen_last_click_feature


def gen_ranking_features():
    hive_unit = HiveUnit(**HIVE_CONFIG)
    feature_generator = FeatureGenerator(hive_unit)
    print(datetime.now(), 'generate user feature start...')
    feature_generator.gen_ranking_user_df()
    print(datetime.now(), 'generate user feature done, generate item feature start...')
    feature_generator.gen_ranking_item_df()
    feature_generator.dump()
    hive_unit.release()
    print(datetime.now(), 'generate item feature done, generate last click feature start...')
    gen_last_click_feature()
    print(datetime.now(), 'generate last click feature done')


def gen_embedding_features():
    hive_unit = HiveUnit(**HIVE_CONFIG)
    feature_generator = FeatureGenerator(hive_unit)
    print(datetime.now(), 'generate embedding user feature start...')
    feature_generator.gen_embedding_user_df()
    print(datetime.now(), 'generate embedding user feature done, dump embedding feature start...')
    feature_generator.gen_embedding_item_df()
    feature_generator.dump()
    print(datetime.now(), 'dump embedding item user embedding feature end...')
    hive_unit.release()


if __name__ == '__main__':
    # gen_ranking_features()
    gen_embedding_features()

