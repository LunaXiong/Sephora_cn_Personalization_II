"""
user-item data -> item sequence -> item2vec model -> item2item
"""
import pandas as pd
from lib.model.embedding import ItemEmbeddingGenSim
from lib.db.hive_utils import HiveUnit


# generate (user, item, time, behavior) data
sql_cmd = """
select t1.user_id,t2.time,t2.behavior,t2.key_words,t2.op_code
from da_dev.iris_phase2_sample_behavior_user t1
left outer join
da_dev.iris_phase2_sample_behavior t2 on t1.user_id=t2.user_id
where t1.behavior_cnt>5
"""
hive_unit = HiveUnit(host='10.157.58.198', port=8088, username='diyang', database='dwd', auth='NOSASL')
raw_df = hive_unit.get_df_from_db(sql_cmd)
hive_unit.release()
raw_df.to_csv('./data/embedding/item_embedding_raw.csv', index=False)
raw_df = pd.read_csv('./data/embedding/item_embedding_raw.csv')
raw_df = raw_df[['user_id', 'time', 'op_code']].dropna(inplace=True)
raw_df = raw_df.astype(dtype={'user_id': str, 'op_code': str})
item_embedding_model = ItemEmbeddingGenSim()
# generate item sequence from raw dataframe
item_embedding_model.gen_seq(raw_df)
# train word2vec model with item sequence
item_embedding_model.gen_w2v_model()
# save model
item_embedding_model.dump('./data/embedding/item_embedding')
# load model
item_embedding_model.load('./data/embedding/item_embedding')
# generate item2item result and save
item_embedding_model.gen_sim_dict('./data/embedding/item2item.json')
