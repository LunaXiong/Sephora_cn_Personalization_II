import jieba
import pandas as pd

from lib.model.embedding import QueryEmbedding
from lib.utils.utils import jieba_wrap

_jieba = jieba_wrap(jieba)

query_df = pd.read_csv('./data/embedding/query_df.csv')

qe = QueryEmbedding()
qe.gen_seq(query_df=query_df, j=_jieba)
qe.gen_w2v_model()
qe.dump('./data/embedding/query_embedding')
qe.load('./data/embedding/query_embedding')
qe.gen_sim_dict('./data/embedding/query2query.json')
