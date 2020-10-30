"""
train embedding model, contains user embedding, query embedding and user embedding
"""
from collections import defaultdict
from typing import List

import pandas as pd
from gensim.models.word2vec import Word2Vec
from tqdm import tqdm

from lib.datastructure.files import ITEM2ITEM, QUERY2QUERY
from lib.utils.utils import dump_json, load_json


class Embedding:
    def __init__(self, seq: list = None, model: Word2Vec = None):
        """
        param seq: list
        param model: class: gensim.word2vec.Word2Vec
        """
        self.seq = seq
        self.model = model
        self.emb_dim = 64

    def gen_seq(self, **kwargs):
        """generate sequence for model training if self.seq is None"""
        raise Exception('the method is not implemented')

    def gen_w2v_model(self, params=None):
        """train word2vec model with sequence"""
        if self.seq is None:
            raise ValueError("Cannot train without sequence, please call gen_seq() firstly")
        model_param = {'size': self.emb_dim, 'window': 5, 'min_count': 64,
                       'sg': 1, 'hs': 1, 'iter': 5, 'workers': 8}
        if params:
            model_param.update(params)
        self.model = Word2Vec(self.seq, **model_param)

    def fine_tune(self, seq, **kwargs):
        """Fine tune with higher quality keywords
        """
        train_args = {"total_examples": len(seq), "epochs": 1}
        train_args.update(kwargs)
        self.model.train(seq, **train_args)

    def gen_sim_dict(self, file_path: str, top_n: int = 10, min_sim_score: float = 0):
        """generate at most N similar items per item,
        for each item, the items in similar item list all are greater than min_sim_score,
        param top_n: int, the max number of most similar items per item, default 10
        param min_sim_score: float, only the items whose similarity score greater than min_sim_score are concerned.
        return item2item_dict: dict, {item_1: [item_2, item_3, ...], item_2: [item_3, ...], ...}
        """
        item2item_dict = defaultdict(list)
        for item in tqdm(self.model.wv.index2entity):
            item2item_dict[item].extend([{_[0]: _[1]} for _ in self.model.wv.most_similar(item, topn=top_n)])
            item2item_dict[item] = [k for sim_items in item2item_dict[item]
                                    for k, v in sim_items.items()
                                    if v >= min_sim_score]
        dump_json(item2item_dict, file_path)
        return item2item_dict

    def dump(self, fp):
        self.model.save(fp)

    def load(self, fp):
        self.model = Word2Vec.load(fp)


class ItemEmbeddingGenSim(Embedding):
    def __init__(self, seq=None, model=None):
        super(ItemEmbeddingGenSim, self).__init__(seq, model)

    def gen_seq_with_pad(self, raw_df, delta):
        raw_df = raw_df[['user_id', 'time', 'op_code']].sort_values(by=['user_id', 'time'])
        raw_df['time'] = pd.to_datetime(raw_df['time'])
        seq = raw_df.groupby(['user_id'])[['op_code', 'time']]. \
            apply(lambda x: (x['time'].to_list(), x['op_code'].to_list())).reset_index()
        seq = seq[0].to_list()
        pad_seq = []
        for pair in seq:
            if len(pair[0]) == 1:
                continue
            pad_pair = [pair[1][0]]
            for i in range(1, len(pair[0])):
                d = (pair[0][i] - pair[0][i - 1]).days
                if d > delta:
                    pad_pair.append("-10000")
                pad_pair.append(pair[1][i])
            pad_seq.append(pad_pair)
        self.seq = pad_seq

    def gen_seq(self, raw_df: pd.DataFrame):
        """generate op_code sequence with user behaviors by time
        param raw_df: pd.DataFrame, [user_id, time, op_code]
        return item_sequence: List[List[op_code]]
        """
        raw_df = raw_df[['user_id', 'time', 'op_code']].sort_values(by=['user_id', 'time'])
        seq = raw_df.groupby(['user_id'])['op_code'].apply(lambda x: x.to_list()).reset_index()
        self.seq = seq['op_code'].to_list()


class ItemEmbeddingAirbnb:
    def __init__(self):
        pass


class QueryEmbedding(Embedding):
    """query keyword word2vec model"""

    def __init__(self, seq=None, model=None):
        super(QueryEmbedding, self).__init__(seq, model)

    def gen_seq(self, query_df: pd.DataFrame, j=None):
        """generate query sequence with historical query keyword
        param query_df: pd.DataFrame, ['key_words']
        return word_sequence, List[List[word]]
        """
        if j is None:
            import jieba
            j = jieba
        queries = query_df['keyword'].dropna().astype('str').apply(lambda x: x.upper()).to_list()
        self.seq = [j.lcut(x) for x in queries]


class UserEmbedding(Embedding):
    def __init__(self, seq=None, model=None):
        super(UserEmbedding, self).__init__(seq, model)

    def gen_seq(self, raw_df: pd.DataFrame, cut_len: int or None = None):
        """generate user sequence who has behavior on specific item within a session
        param raw_df: pd.DataFrame, [session_id, user_id, op_code, time]
        param cut_len: only length of user sequence greater than cut_len will be concerned
        return user_sequence: List[List[user_id]]
        """
        raw_df = raw_df.sort_values(by=['op_code', 'time'])
        seq = raw_df.groupby(['session_id', 'op_code'])['user_id']. \
            apply(lambda x: x.to_list()).reset_index()
        if cut_len:
            seq = seq.loc[seq['user_id'].apply(lambda x: len(x) >= cut_len)].reset_index(drop=True)

        seq = seq['user_id'].to_list()
        self.seq = seq


def train_user_embedding(model_path: str,
                         sim_dict_path: str,
                         raw_df: pd.DataFrame = None,
                         seq: List = None):
    """train item embedding model and gen item2item
    :param model_path
    :param sim_dict_path
    :param raw_df
    :param seq
    """
    user_embedding = UserEmbedding()
    if not seq:
        if not raw_df.empty:
            user_embedding.gen_seq(raw_df)
        else:
            raise ValueError(r'raw_df and seq can not both be None')
    else:
        user_embedding.seq = seq
    user_embedding.gen_w2v_model()
    user_embedding.dump(model_path)
    user_embedding.gen_sim_dict(sim_dict_path)


def load_user_embedding_model(model_path):
    user_embedding = UserEmbedding()
    user_embedding.load(model_path)


def load_sim_user_dict(sim_dict_path: str):
    return load_json(sim_dict_path)


def _item_embedding_test():
    # item data -> item sequence -> train model -> item similarity dict
    print('-' * 30 + 'item embedding' + '-' * 30)
    my_item_emd = ItemEmbeddingGenSim()
    item_df = pd.DataFrame({'user_id': ['u1'] * 3 + ['u2'] * 4 + ['u3'] * 5,
                            'time': list(range(3)) + list(range(4)) + list(range(5)),
                            'op_code': ['i1', 'i2', 'i3']
                                       + ['i3', 'i2', 'i4', 'i1']
                                       + ['i1', 'i3', 'i2', 'i1', 'i4']})
    print(item_df.info())
    my_item_emd.gen_seq(item_df)  # generate seq from raw data
    print(my_item_emd.seq)
    my_item_emd.gen_w2v_model(params={'min_count': 1})  # train model with seq
    item2item_dict = my_item_emd.gen_sim_dict(ITEM2ITEM)
    print(item2item_dict.items())
    my_item_emd.fine_tune(['i4', 'i3', 'i2', 'i1'])
    item2item_dict = my_item_emd.gen_sim_dict(ITEM2ITEM)
    print(item2item_dict.items())


def _query_embedding_test():
    # query data -> query sequence -> train model -> fine tune model -> gen sim dict
    print('-' * 30 + 'query embedding' + '-' * 30)
    my_query_emd = QueryEmbedding()
    query_df = pd.DataFrame({'key_words': ['兰蔻粉底液', '欧莱雅保湿霜']})
    print(query_df.info())
    my_query_emd.gen_seq(query_df)
    print(my_query_emd.seq)
    my_query_emd.gen_w2v_model(params={'min_count': 1})
    item2item_dict = my_query_emd.gen_sim_dict(QUERY2QUERY)  # gen most similar items
    print(item2item_dict)
    my_query_emd.fine_tune(seq=['巴黎欧莱雅'])
    item2item_dict = my_query_emd.gen_sim_dict(QUERY2QUERY)  # gen most similar items
    print(item2item_dict)


def _user_embedding_test():
    # raw data -> generate seq -> train model -> fine tune model
    print('-' * 30 + 'user embedding' + '-' * 30)
    my_user_emd = UserEmbedding()
    # raw_df: [session_id, user_id, op_code, time]
    raw_df = pd.DataFrame({'session_id': ['s1'] * 10,
                           'user_id': ['u1'] * 5 + ['u2'] * 5,
                           'op_code': ['i1', 'i2'] * 5,
                           'time': range(10)})
    print(raw_df.info())
    if not my_user_emd.seq:
        my_user_emd.gen_seq(raw_df)  # generate seq from raw data
    print(my_user_emd.seq)
    my_user_emd.gen_w2v_model(params={'min_count': 1})  # train model with seq
    user2user = my_user_emd.gen_sim_dict('user2user.json')  # gen most similar items
    print(user2user)
    my_user_emd.fine_tune(seq=['u1', 'u1', 'u2'])
    user2user = my_user_emd.gen_sim_dict('user2user.json')  # gen most similar items
    print(user2user)


if __name__ == '__main__':
    # _item_embedding_test()
    _query_embedding_test()
    # _user_embedding_test()
