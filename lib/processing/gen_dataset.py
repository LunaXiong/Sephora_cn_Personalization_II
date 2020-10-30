from datetime import date

import pandas as pd

from airflow.query_embedding_model import load_query_embedding_model
from lib.datastructure.constants import USER_FEATURE_COLS
from lib.datastructure.files import LAST_CLICK_FEATURE_FN
from lib.db.hive_utils import HiveUnit
from lib.model.recall import Recall
from lib.processing.for_ranking import gen_train_index, LastClick, QueryProcessor
from lib.processing.gen_feature import get_ranking_user_feature, get_ranking_item_feature


def gen_batch_dataset(index_df: pd.DataFrame,
                      last_click: LastClick,
                      query_processor: QueryProcessor,
                      user_feature_mat,
                      user_index,
                      user_feature_col_index,
                      item_feature):
    # last click feature, feature col: 'last_click'
    index_df = last_click.gen_last_click(index_df, date.today())
    index_df.drop(columns=['time'], inplace=True)
    # query feature, feature cols: query_{i} for i in range(64)
    index_df['query_vec'] = index_df['query'].apply(query_processor.process_query)
    for i in range(query_processor.emb_mod.vector_size):
        index_df['query_%d' % i] = index_df['query_vec'].apply(lambda x: x[i])
    index_df.drop(columns=['query_vec'], inplace=True)
    # user feature: USER_FEATURE_COLS
    index_df['user_feature'] = index_df['open_id'].apply(
        lambda x: user_feature_mat[user_index[x]]
    )
    for col, index in user_feature_col_index.items():
        index_df[col] = index_df['user_feature'].apply(lambda x: x[index])
    index_df.drop(columns=['user_feature'], inplace=True)
    # item feature
    index_df['op_code'] = index_df['op_code'].apply(str)
    item_feature['op_code'] = item_feature['op_code'].apply(str)
    index_df = pd.merge(index_df, item_feature, on='op_code', how='left')
    index_df.fillna(value=0)
    index_df = index_df.drop_duplicates()
    return index_df


def generate_dataset(hive_unit: HiveUnit, batch_size=5000000):
    # positive and negative sample index: ['open_id', 'time', 'query', 'op_code', 'label']
    recall = Recall()
    index_df = gen_train_index(hive_unit, recall)
    last_click = LastClick.load(LAST_CLICK_FEATURE_FN)
    query_embedding_model = load_query_embedding_model()
    query_processor = QueryProcessor(query_embedding_model)
    user_df = get_ranking_user_feature()
    user_feature_mat = user_df[USER_FEATURE_COLS].values
    user_index = {open_id: idx for idx, open_id in enumerate(user_df['open_id'])}
    user_feature_col_index = {col: idx for idx, col in enumerate(USER_FEATURE_COLS)}
    item_feature = get_ranking_item_feature()
    item_feature['op_code'] = item_feature['op_code'].astype(str)
    # generate batch-size train data
    i = 0
    while i < len(index_df):
        index_batch = index_df.iloc[i: min(len(index_df), i+batch_size)]
        train_data_batch = gen_batch_dataset(index_batch,
                                             last_click,
                                             query_processor,
                                             user_feature_mat,
                                             user_index,
                                             user_feature_col_index,
                                             item_feature)
        i += batch_size
        yield train_data_batch

