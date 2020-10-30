# -*- coding:utf-8 -*-
import pandas as pd

from lib.datastructure.config import HIVE_CONFIG
from lib.db.hive_utils import HiveUnit
from lib.model.recall import Recall
from lib.processing.encoder import padding, UserEncoder, cut_name, ItemEncoder
from lib.utils.utils import today
from lib.processing.for_ranking import gen_index, LastClick

TODAY = today()
INDEX_COLS = ['open_id', 'op_code', 'time']

# BEHAVIOR_WITH_NEG = "./data/sample_click_aft_search_neg_10000.csv"
# USER_FN = "./data/user_profile_rand123_10000.csv"
# ITEM_FN = "./data/0918/product_list.csv"


# BEHAVIOR_WITH_NEG
def gen_posi_items():
    hive_unit = HiveUnit(**HIVE_CONFIG)
    raw_click = hive_unit.get_df_from_db("""select * from da_dev.search_click_aft_search""")
    print(raw_click.info())
    raw_click = raw_click.dropna()
    raw_click_sample = raw_click.head(10000)
    print(raw_click_sample)
    # raw_click_sample.to_csv("sample_click_aft_search.csv", index=None)
    hive_unit.release()
    return raw_click_sample


neg = Recall()
def gen_neg_items():
    neg_df = pd.read_csv("./data/sample_click_aft_search.csv")
    querys = neg_df['query'].to_list()
    neg_df['query'].apply(str).str.upper()
    neg_df['query'] = neg_df['query'].str.upper()
    for i in range(0, len(querys)):
        # print(df_raw['query'][i])
        num_list = neg.word_tag_prod(neg_df.loc[i, 'query'])
        num_list_new = map(lambda x: str(x), num_list[0:20])
        neg_df.loc[i, 'neg_items'] = '/'.join(num_list_new)
        # print('now time: ', t.time())
    # df_raw.to_csv("./data/search_neg_click.csv", index=None)
    print(neg_df)
    return neg_df


# USER_FN
def gen_user_profile(hive_unit: HiveUnit):
    user_df = hive_unit.get_df_from_db(r"""
    select * from da_dev.search_user_profile limit 10000
    """)
    return user_df


# ITEM_FN
def gen_item_list(hive_unit: HiveUnit):
    item_df = hive_unit.get_df_from_db(r"""
    select product_id,sku_code,sku_name,standardskuname,category,
    subcategory,thirdcategory,brand,product_line,nickname,brand_origin,
    skincare_function_basic,skincare_function_special,skincare_ingredients,
    makeup_function,makeup_feature_look,makeup_feature_color,
    makeup_feature_scene,target_agegroup,function_segmented,skintype,
    fragrance_targetgender,fragrance_stereotype,fragrance_intensity,
    fragrance_impression,fragrance_type,bundleproduct_festival,bundleproduct_main_sku,
    bundleproduct_main_sku_function,bundleproduct_opmix
    from da_dev.search_prod_list """)
    return item_df


# Load index
raw_df = gen_neg_items()
# raw_df = pd.read_csv(BEHAVIOR_WITH_NEG)
raw_df['time'] = pd.to_datetime(raw_df['time'])

# Extract negative samples
index_df = gen_index(raw_df)

# Generate features
# Last click delta
last_click = LastClick()
last_click.gen_click_map(raw_df)
last_click_feature = last_click.gen_last_click(index_df, TODAY)
last_click_feature = last_click_feature[INDEX_COLS + ['last_click']]

print(last_click_feature.loc[0, :])

# Load and process User feature
hive_unit = HiveUnit(**HIVE_CONFIG)
raw_user_df = gen_user_profile(hive_unit)
hive_unit.release()
# raw_user_df = pd.read_csv(USER_FN)
user_pad_cols = [x for x in raw_user_df.columns if x != 'open_id']
user_df = padding(raw_user_df, user_pad_cols, {})

user_encoder = UserEncoder()
user_df = user_encoder.gen_for_ranking(user_df)

# Load and process Item feature
hive_unit = HiveUnit(**HIVE_CONFIG)
raw_item_df = gen_item_list(hive_unit).\
    drop(columns=['sku_code', 'sku_name']).\
    rename(columns={'product_id': 'op_code'})
hive_unit.release()

# raw_item_df = pd.read_csv(ITEM_FN).\
#     drop(columns=['sku_code', 'sku_name']).\
#     rename(columns={'product_id': 'op_code'})

item_df = cut_name(raw_item_df, "StandardSKUName")
item_pad_cols = [x for x in item_df.columns if x != 'op_code']
item_df = padding(item_df, item_pad_cols, {})

item_encode_cols = item_pad_cols

item_encoder = ItemEncoder(item_encode_cols)
item_df = item_encoder.gen_for_ranking(item_df)
# print(item_df.loc[0, :])

