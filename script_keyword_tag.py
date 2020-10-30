import pandas as pd
from gensim.models.word2vec import Word2Vec

from lib.datastructure.files import TAGGING_FN
from lib.model.linking import KeywordTag
from lib.model.nlp import tf_idf
from lib.processing.for_embedding import gen_item2tag
from lib.utils.utils import dump_json

w2v_model = Word2Vec.load('./data/query_w2v_full_new_cut')

kws = pd.read_excel('./data/词库整理_20200904.xlsx', sheet_name=0)['StandardKeyword'].to_list()
# kws = ["3CE红梨色"]


item_tags = pd.read_csv(TAGGING_FN).drop(columns=['sku_code', 'sku_name'])
item_tags['product_id'] = item_tags['product_id'].astype('int').astype('str')
# item_names = {k: v for k, v in zip(item_tags['product_id'].values, item_tags['StandardSKUName'].values)}
item_names = {row['product_id']: row['StandardSKUName'] for index, row in item_tags.iterrows()}
tag_cols = [x for x in item_tags.columns if x not in ['product_id', 'sku_name']]
item_tags = gen_item2tag(item_tags, "product_id", tag_cols)
item_tags = {item_names[item]: tags for item, tags in item_tags.items()}

tag_mapping = pd.read_csv('./data/tag_trans_map.csv')
tag_mapping = {row['details']: row['Detail CN'] for _, row in tag_mapping.iterrows()}

tags = list([tag_mapping[x] for x in ts if tag_mapping.get(x)]
            for ts in item_tags.values())

tag_tf_idf = tf_idf(tags)
tag_idf = {}
for line in tag_tf_idf:
    for k, v in line.items():
        tag_idf[k] = v


kw_tag = KeywordTag(w2v_model, kws, item_tags, tag_mapping)
ret = kw_tag.gen_kw_rel_tag(idf=tag_idf)



dump_json(ret, './data/kw_rel_tag_3.json')

