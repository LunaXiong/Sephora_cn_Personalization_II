import os
import json

root = os.path.split(os.path.abspath(__file__))[0]
for i in range(2):
    root = os.path.split(root)[0]
env = json.load(open(os.path.join(root, "config.json")))['ENV']

if env != "prod":
    data_dir = os.path.join(root, 'data')


    def data_fn(fn):
        return os.path.join(data_dir, fn)
else:
    def data_fn(fn):
        return os.path.join('/mnt/resource/app/data/', fn)

BEHAVIOR_FN = data_fn("sample-08-17/phase2_behavior_sample.csv")
ITEM_EMBEDDING = data_fn("embedding/item_embedding")
TAGGING_FN = data_fn("product_tagging.csv")
JIEBA_DICT = data_fn("jieba/cut_list.txt")

LAST_CLICK_FEATURE_FN = data_fn("ranking/last_click.json")

LGB_RANKING_FN = data_fn("ranking/ranking.pkl")

USER_EMBEDDING_ENCODE_MAP = data_fn("encode/user_embedding_encode_map.json")
ITEM_EMBEDDING_ENCODE_MAP = data_fn("encode/item_embedding_encode_map.json")

# raw file path
PRODUCT_LIST = data_fn('ProductList 20201026.xlsx')
BRAND_LIST = data_fn('Brand List 20201026.xlsx')
TRANSLATION = data_fn('translation 20201023.xlsx')

# sheet1: Associated Keyword List, used to generate kw-tag mapping
# sheet2: Brand_kw_priority, [Brand, kw, Priority], used to generate brand priority
# sheet3: stkw_brand, [st_type, stkw, kw], used to generate brand correcting
# sheet4: stkw_brand_mapping, [st_type, stkw, kw], used to generate brand mapping
BRAND_KW_FILE = data_fn('brand_kw.xlsx')
STANDARD_TAG_KEYWORD_FILE = data_fn('default keyword 0921.xlsx')
EXTENDED_TAG_KEYWORD_FILE = data_fn('product tag 1014.xlsx')
BRAND_MAPPING_FILE = data_fn('stkw_brand 0930.xlsx')
ASSOCIATED_KEYWORD_FILE_OLD = data_fn('Associated Keywords20201015.xlsx')


# item embedding model
ITEM_EMBED_MODEL = data_fn('embedding/item_embedding')
ITEM2ITEM = data_fn('embedding/item2item.json')

# query embedding model
QUERY_EMBED_MODEL = data_fn('embedding/query_embedding')
QUERY2QUERY = data_fn('embedding/query2query.json')

# pre-trained word2vec model
WORD2VEC_MODEL = data_fn('embedding/word2vec_wx')

# user embedding model
USER_EMBED_MODEL = data_fn('embedding/user_embedding')
USER2USER = data_fn('embedding/user2user.json')

# generated file path
# item-tag mapping with combined tag
ITEM2TAG_EXTENDED = data_fn('item2tag_extended.json')
TAG2ITEM_EXTENDED = data_fn('tag2item_extended.json')
CUT_TAG2ITEM_EXTENDED = data_fn('cut_tag2item.json')
# item-tag mapping with single tag
ITEM2TAG_BASIC = data_fn('item2tag_basic.json')
TAG2ITEM_BASIC = data_fn('tag2item_basic.json')
CUT_TAG2ITEM_BASIC = data_fn('cut_tag2item_basic.json')
# item-tag mapping for new product
ITEM2TAG_NEW = data_fn('item2tag_new.json')
TAG2ITEM_NEW = data_fn('tag2item_new.json')

#
USER2ITEM = data_fn('user2item.json')
# standard tag-keyword mapping, using for default query and hot query recommendation before search
STANDARD_TAG2KW = data_fn('standard_tag2kw.json')
STANDARD_KW2TAG = data_fn('standard_kw2tag.json')
# extended tag-keyword mapping, using for query suggestion during search
EXTENDED_TAG2KW = data_fn('extended_tag2kw.json')
EXTENDED_KW2TAG = data_fn('extended_kw2tag.json')
KW2TAG_FIXED = data_fn('kw2tag_fixed.json')

USER2KW = data_fn('user2kw.json')
USER2TAG = data_fn('user2tag.json')
KW2ITEM = data_fn('kw2item.json')
KW2ITEM_NEW = data_fn('item_kw.json')


# after search
# intermediate file
RANKING_USER_DF = data_fn('ranking_user_df.csv')
RANKING_ITEM_DF = data_fn('ranking_item_df.csv')

EMBEDDING_USER_DF = data_fn('embedding_user_df.csv')
EMBEDDING_ITEM_DF = data_fn('embedding_item_df.csv')

ITEM_TAG_FILE = data_fn('item_tag.xlsx')

# during search
# raw data
ASSOCIATED_KEYWORD_FILE = data_fn('Associated Keyword List - Total AVG Click 20201026.xlsx')
ASSOCIATED_KEYWORD_FILE_TOP = data_fn('Associated Keyword List - TOP 4 AVG Click 20201026.xlsx')
# TRIE
SPLIIT_LIST = data_fn('split_list0917.txt')
# intermediate file
ITEM_CLICK = data_fn('item_click.json')
SELECT_SKU = data_fn('select_sku.csv')

KW_POP = data_fn('kw_pop.csv')
KW_POP_TOP = data_fn('kw_pop_top.csv')

KW2POP_SCORE = data_fn('kw2pop_score.json')
KW2POP_SCORE_TOP = data_fn('kw2pop_score_top.json')

KW_PRIORITY = data_fn('kw_priority.json')
KW_PRIORITY_TOP = data_fn('kw_priority_top.json')

BRAND_MAPPING = data_fn('brand_mapping.json')
BRAND_CORRECTING = data_fn('brand_correcting.json')
QUERY_KW_TEST = data_fn('during_search_test.json')


# for test
HISTORY_QUERY = data_fn('his_query.csv')
HISTORY_QUERY_1000 = data_fn('historical_query_90d_.csv')

USER_ITEM_EMBEDDING_USER = data_fn('user_item_embedding_model/users.txt')
USER_ITEM_EMBEDDING_USER_VEC = data_fn('user_item_embedding_model/user_vec.npy')
USER_ITEM_EMBEDDING_ITEM = data_fn('user_item_embedding_model/items.txt')
USER_ITEM_EMBEDDING_ITEM_VEC = data_fn('user_item_embedding_model/item_vec.npy')

DEFAULT_SKU = data_fn('default_sku.json')

