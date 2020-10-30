import pandas as pd
from gensim.models.word2vec import Word2Vec

from lib.datastructure.files import *
from lib.processing.for_embedding import gen_user2item, gen_sim_items, gen_item2tag
from lib.model.linking import gen_user2tag
from lib.utils.utils import dump_json

user_behavior_df = pd.read_csv(BEHAVIOR_FN)
user_behavior_df = user_behavior_df.dropna().query('op_code != 0')
user_behavior_df['op_code'] = user_behavior_df['op_code'].astype('int').astype('str')
user_rel_items = gen_user2item(user_behavior_df)

for k, v in user_rel_items.items():
    print(k)
    print(v)
    break


embedding_model = Word2Vec.load(ITEM_EMBEDDING)
all_items = user_behavior_df['op_code'].astype('int').astype('str').drop_duplicates().to_list()
sim_items = gen_sim_items(all_items, embedding_model)

for k, v in sim_items.items():
    print(k)
    print(v)
    break

item_tags = pd.read_csv(TAGGING_FN).drop(columns=['sku_code'])
item_tags['product_id'] = item_tags['product_id'].astype('int').astype('str')
tag_cols = [x for x in item_tags.columns if x not in ['product_id', 'sku_name']]
item_tags = gen_item2tag(item_tags, "product_id", tag_cols)

# Filter and translate with given map
tag_trans_map = pd.read_csv('./data/tag_trans_map.csv')
trans_map = dict()
for index, row in tag_trans_map.iterrows():
    trans_map[row['details']] = row['Detail CN']

for k, v in item_tags.items():
    trans_tags = []
    for tag in v:
        trans_tag = trans_map.get(tag, None)
        if trans_tag:
            trans_tags.append(trans_tag)
    item_tags[k] = trans_tags

for k, v in item_tags.items():
    print(k)
    print(v)
    break

ret = gen_user2tag(user_rel_items, sim_items, item_tags, 10)

dump_json(ret, './data/user_tags.json')
