"""
train query embedding model
used for generating most similar words for specific word
queries -> split word -> word sequence -> query2vec model -> word2word
train_query_embedding: 10 minutes
gen_query2query: 3 hour 20 minutes
"""
from datetime import datetime
from typing import Dict, List

from gensim.models import Word2Vec

from lib.datastructure.config import HIVE_CONFIG
from lib.datastructure.files import QUERY_EMBED_MODEL, QUERY2QUERY, WORD2VEC_MODEL
from lib.db.hive_utils import HiveUnit
from lib.model.embedding import QueryEmbedding
from lib.preprocessing.gen_user_historical_behavior import get_query_df
from lib.utils.utils import load_json


def train_query_embedding():
    """train query embedding model and gen word2vec
    """
    print(datetime.now(), 'generate query data start...')
    hive_unit = HiveUnit(**HIVE_CONFIG)
    query_df = get_query_df(hive_unit, days=180)
    hive_unit.release()
    print(datetime.now(), 'generate query data done')
    query_embedding = QueryEmbedding()
    query_embedding.gen_seq(query_df)
    query_embedding.gen_w2v_model()
    query_embedding.dump(QUERY_EMBED_MODEL)
    print(datetime.now(), 'generate query embedding model done')
    query_embedding.gen_sim_dict(QUERY2QUERY)
    print(datetime.now(), 'generate query2query done')


def load_query_embedding_model() -> Word2Vec:
    query_embedding = QueryEmbedding()
    query_embedding.load(WORD2VEC_MODEL)
    return query_embedding.model


def gen_query2query():
    """load pre-trained word2vec model and gen similar words"""
    query_embedding = QueryEmbedding()
    query_embedding.load(WORD2VEC_MODEL)
    query_embedding.gen_sim_dict(QUERY2QUERY)


def get_query2query() -> Dict:
    return load_json(QUERY2QUERY)


if __name__ == "__main__":
    # train_query_embedding()
    gen_query2query()
