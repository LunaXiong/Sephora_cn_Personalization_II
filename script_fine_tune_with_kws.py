import pandas as pd
from gensim.models.word2vec import Word2Vec
import jieba

from lib.model.embedding import QueryEmbedding

query_w2v = Word2Vec.load('./data/query_w2v_full')
print(query_w2v.most_similar("欧缇丽"))
keywords = pd.read_excel("./data/词库整理_20200904.xlsx")['StandardKeyword'].to_list()
cut_kws = [jieba.lcut(w) for w in keywords]

query_embedding = QueryEmbedding(model=query_w2v)
query_embedding.fine_tune(cut_kws)


