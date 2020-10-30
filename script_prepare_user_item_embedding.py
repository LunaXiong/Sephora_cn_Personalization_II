import pandas as pd
import jieba

from lib.utils.utils import jieba_wrap, dump_json

jieba = jieba_wrap(jieba)

# load and process user
user_profile = pd.read_csv('./data/user_profile_rand123_10000.csv').drop(columns=['age'])
user_profile_repl = pd.read_csv('./data/user_profile_num_process.csv')
user_profile = user_profile.sort_values(by='open_id')
user_profile_repl = user_profile_repl.sort_values(by='open_id')

for col in user_profile_repl.columns:
    unk_pad = col + "_UNK"
    user_profile_repl[col] = user_profile_repl[col].apply(lambda x: x if x != "-1" else unk_pad)

for col in user_profile_repl.columns:
    if col != 'open_id':
        user_profile[col] = user_profile_repl[col]


# load and process item
prod_tag = pd.read_csv('./data/product_tagging.csv').drop(columns=['sku_code', 'sku_name'])
NAME_CUT_LEN = 5
item_names = []
for item in prod_tag['StandardSKUName']:
    item_names.append(jieba.lcut(item)[:NAME_CUT_LEN])
item_names = [x+['</PAD>' for _ in range(NAME_CUT_LEN-len(x))] for x in item_names]

for i in range(NAME_CUT_LEN):
    prod_tag["item_name_"+str(i+1)] = [x[i] for x in item_names]
prod_tag = prod_tag.drop(columns=['StandardSKUName'])


def feature_encode(raw_feature: pd.DataFrame, index_col):
    feature_map = []
    for col in raw_feature.columns:
        if col != index_col:
            raw_feature[col] = raw_feature[col].fillna(col+"_UNK")
            feature_map += raw_feature[col].drop_duplicates().to_list()
    feature_map = {x[1]: x[0] for x in enumerate(feature_map)}
    for col in raw_feature.columns:
        if col != index_col:
            raw_feature[col] = raw_feature[col].apply(lambda x: feature_map[x])
    return raw_feature, feature_map


prod_tag, item_feature_map = feature_encode(prod_tag, 'product_id')
prod_tag.to_csv('./data/nn/item_feature.csv', index=False)
dump_json(item_feature_map, './data/nn/item_feature_map.json')

user_profile, user_feature_map = feature_encode(user_profile, 'open_id')
user_profile.to_csv('./data/nn/user_feature.csv', index=False)
dump_json(user_feature_map, './data/nn/user_feature_map.json')

