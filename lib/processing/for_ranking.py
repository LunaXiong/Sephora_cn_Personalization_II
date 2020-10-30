from datetime import date, datetime

import jieba
import numpy as np
import pandas as pd
from gensim.models import Word2Vec
from tqdm import tqdm

from lib.datastructure.config import HIVE_CONFIG
from lib.datastructure.constants import DATE_FORMAT
from lib.datastructure.files import LAST_CLICK_FEATURE_FN
from lib.db.hive_utils import HiveUnit
from lib.model.recall import Recall
from lib.utils.utils import datetime2date, load_json, dump_json
from lib.utils.utils import jieba_wrap

jieba = jieba_wrap(jieba)


def click_aft_search(hive_unit: HiveUnit):
    hive_unit.execute(r"""
        drop table if exists da_dev.search_click_aft_search;
        create table da_dev.search_click_aft_search
        (
            time string,
            op_code int,
            query string,
            open_id string
        );
        insert into da_dev.search_click_aft_search
        select time,op_code,query,open_id
        from(
        select user_id,time,op_code,event,query
        from(
            select user_id,time,op_code,event
            ,lead(key_words,1,null) over(partition by user_id order by rn,event_new) as query
            from 
            (
                select user_id,time,op_code,event,key_words,event_new
                ,dense_rank() over(partition by user_id order by time,event_new) as rn 
                from 
                (

                    select user_id,time,op_code,event,key_words,
                    case when event='ListProductClick' then 2 else 1 end as event_new
                    from dwd.v_events
                    where dt between date_sub(current_date,181) and date_sub(current_date,1)
                    and ((event='ListProductClick' and op_code<>'' and page_type_detail='search_list')
                    or (event= '$MPViewScreen'and page_type_detail='search_list'))
                )t1   
            )t2   
        )t3
        where event='ListProductClick'
        )tt1
        left join 
        (
            select distinct open_id,sensor_id
            from da_dev.tagging_id_mapping) tt2 on tt1.user_id = tt2.sensor_id
    """)


def run_click_aft_search():
    hive_unit = HiveUnit(**HIVE_CONFIG)
    click_aft_search(hive_unit)
    hive_unit.release()


def gen_train_index(hive_unit: HiveUnit, recall_obj: Recall):
    """generate positive and negative samples
    :return train_index, pd.DataFrame, [open_id, time, query, op_code, label]
    """
    query = r"""
        select time, open_id, query, op_code 
        from da_dev.search_click_aft_search
        where open_id <>'' and query <>'' and op_code is not null 
        """
    click_after_search_df = hive_unit.get_df_from_db(query)
    click_after_search_df['time'] = click_after_search_df['time'].apply(lambda x: x.split(' ')[0])
    click_after_search_df[['time', 'open_id', 'query', 'op_code']].drop_duplicates(inplace=True)
    res_df = click_after_search_df.groupby(['open_id', 'time', 'query'])['op_code'].apply(set).reset_index()

    # generate relative items with query
    rel_item_df = []
    for query in tqdm(res_df['query'].unique()):
        try:
            rel_items = set(recall_obj.word_tag_prod(query)[:10])
        except:
            rel_items = set()
        rel_item_df.append([query, rel_items])
    rel_item_df = pd.DataFrame(rel_item_df, columns=['query', 'rel_items'])

    # generate negative samples
    res_df = res_df.merge(rel_item_df)
    res_df['neg_opcode'] = res_df['rel_items'] - res_df['op_code']

    # explode positive and negative samples
    res_df['op_code'] = res_df['op_code'].apply(list)
    res_df['neg_opcode'] = res_df['neg_opcode'].apply(list)
    pos_samples = res_df[['open_id', 'time', 'query', 'op_code']].explode('op_code').reset_index(drop=True)
    pos_samples['label'] = 1
    # print(pos_samples)
    # pos_samples.to_csv("pos_samples.csv")
    neg_samples = res_df[['open_id', 'time', 'query', 'neg_opcode']].explode('neg_opcode').reset_index(drop=True)
    neg_samples['label'] = 0
    neg_samples.rename(columns={'neg_opcode': 'op_code'}, inplace=True)
    # print(neg_samples)
    # neg_samples.to_csv("neg_samples.csv")
    train_index = pd.concat([pos_samples, neg_samples]).sample(frac=1).reset_index(drop=True)
    # print(train_index)
    train_index['open_id'] = train_index['open_id'].astype(str)
    train_index['op_code'] = train_index['op_code'].astype(str)
    print('all data index: ', len(train_index), 'positive: ', len(pos_samples))
    # train_index.to_csv("train_index.csv")
    return train_index


def gen_index(raw_behavior_df):
    ret = []
    for index, row in tqdm(raw_behavior_df.iterrows()):
        row_items = dict(row)
        pos_op = row_items.pop('op_code')
        try:
            neg_ops = row_items.pop('op_code_not_click').split(' / ')
        except:
            # print(row)
            continue
        part_ret = []
        pos = row_items.copy()
        pos['op_code'] = pos_op
        pos['label'] = 1
        part_ret.append(pos)
        for neg_op in neg_ops:
            neg = row_items.copy()
            neg['op_code'] = neg_op
            neg['label'] = 0
            part_ret.append(neg)
        ret += part_ret
    return pd.DataFrame(ret)


class LastClick:
    def __init__(self):
        self.item_col = "op_code"
        self.user_col = "open_id"
        self.time_col = "time"

        self.date_start = date(2019, 1, 1)

        self.click_map = None

    @classmethod
    def load(cls, fn):
        obj = cls()
        click_map = load_json(fn)
        for k, v in click_map.items():
            for _k, _v in v.items():
                click_map[k][_k] = datetime2date(datetime.strptime(_v, DATE_FORMAT))
        obj.click_map = click_map
        return obj

    def dump(self, fn):
        ds_click_map = self.click_map.copy()
        for k, v in ds_click_map.items():
            for _k, _v in v.items():
                ds_click_map[k][_k] = _v.strftime(DATE_FORMAT)
        dump_json(ds_click_map, fn)

    def gen_click_map(self, behavior_df):
        """

        :param behavior_df: user, item, time
        :return:
        """
        click_map = {}
        for index, row in behavior_df.iterrows():
            click_date = datetime2date(row[self.time_col])
            user = row[self.user_col]
            item = row[self.item_col]
            if click_map.get(user):
                click_map[user].setdefault(item, []).append(click_date)
            else:
                click_map[user] = {item: [click_date]}
        for k, v in click_map.items():
            for _k, _v in v.items():
                click_map[k][_k] = max(_v)
        self.click_map = click_map

    def gen_last_click(self, index_df, date_anchor):
        """
        :param index_df: Index df for ranking
        :param date_anchor: for time delta calculation
        :return:
        """
        index_df['last_click'] = index_df.apply(
            lambda row: self.click_map[row[self.user_col]][row[self.item_col]]
            if self.click_map.get(row[self.user_col]) and self.click_map[row[self.user_col]].get(row[self.item_col])
            else self.date_start,
            axis=1
        )
        index_df['last_click'] = index_df['last_click'].apply(
            lambda x: (date_anchor - x).days)
        return index_df


# BEHAVIOR_WITH_NEG
def gen_posi_items():
    hive_unit = HiveUnit(**HIVE_CONFIG)
    raw_click = hive_unit.get_df_from_db("""select * from da_dev.search_click_aft_search limit 15000""")
    hive_unit.release()
    # print(raw_click)
    raw_click = raw_click.dropna()
    raw_click_sample = raw_click.head(10000)
    # raw_click_sample.to_csv("sample_click_aft_search.csv", index=None)

    return raw_click_sample


def gen_neg_items():
    neg = Recall()
    neg_df = gen_posi_items()
    querys = neg_df['query'].to_list()
    neg_df['query'] = neg_df['query'].str.upper()
    for i in range(0, len(querys)):
        try:
            num_list = neg.word_tag_prod(neg_df.loc[i, 'query'])
            num_list_new = map(lambda x: str(x), num_list[0:20])
            neg_df.loc[i, 'neg_items'] = '/'.join(num_list_new)
        except:
            continue
        # print('now time: ', t.time())
    # df_raw.to_csv("./data/search_neg_click.csv", index=None)
    return neg_df


def gen_neg_items_new():
    neg = Recall()
    neg_df = gen_posi_items()
    neg_df['query'] = neg_df['query'].astype('str').str.upper()
    neg_df['neg_items'] = neg_df['query'].apply(
        lambda x: './'.join([str(x) for x in neg.word_tag_prod(x)[:20]])
    )
    return neg_df


# USER_FN
def gen_user_df(hive_unit):
    user_df = hive_unit.get_df_from_db(r"""
    select * from da_dev.search_user_profile
    """)
    return user_df


# ITEM_FN
def gen_item_df(hive_unit):
    item_df = hive_unit.get_df_from_db(r"""
    select * from da_dev.search_item_profile""")
    return item_df


class QueryProcessor:
    def __init__(self, embedding_model: Word2Vec):
        self.emb_mod = embedding_model
        self.vec_size = embedding_model.vector_size

    def process_query(self, query):
        # query -> [vec1, vec2, ...] -> mean_vec
        query_vecs = []
        for w in jieba.cut(query):
            if self.emb_mod.wv.vocab.get(w):
                query_vecs.append(self.emb_mod.wv[w])

        # return query_vecs
        if len(query_vecs) > 0:
            return np.mean(np.array(query_vecs), axis=0)
        else:
            return np.array([0 for _ in range(self.vec_size)])


def lask_click_test(hive_unit: HiveUnit):
    recall = Recall()
    index_df = gen_train_index(hive_unit, recall)
    index_df['open_id'] = index_df['open_id'].astype(str)
    index_df['op_code'] = index_df['op_code'].astype(str)
    last_click = LastClick.load(LAST_CLICK_FEATURE_FN)
    index_df = last_click.gen_last_click(index_df, date.today())
    index_df.drop(columns=['time'], inplace=True)
    print(index_df)


if __name__ == '__main__':
    hive_unit = HiveUnit(**HIVE_CONFIG)
    # lask_click_test(hive_unit)
    # hive_unit.release()
    recall_obj = Recall()
    gen_train_index(hive_unit, recall_obj)
