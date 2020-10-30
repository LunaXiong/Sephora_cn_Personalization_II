import pandas as pd
import sys
import numpy as np

from lib.datastructure.files import USER_ITEM_EMBEDDING_USER, USER_ITEM_EMBEDDING_USER_VEC, USER_ITEM_EMBEDDING_ITEM, \
    USER_ITEM_EMBEDDING_ITEM_VEC
from lib.utils.utils import save_list

sys.path.append('./')


from lib.datastructure.config import HIVE_CONFIG
from lib.model.nn import UserItemEmbedding
from lib.db.hive_utils import HiveUnit
from lib.model.recall import Recall
from lib.processing.for_ranking import gen_train_index
from lib.processing.gen_feature import get_embedding_user_feature, get_embedding_item_feature

def run(hive_unit):
    # positive and negative sample index: ['open_id', 'time', 'query', 'op_code', 'label']
    # recall = Recall()
    # index_df = gen_train_index(hive_unit, recall)
    index_df = pd.read_csv('./data/index_df.csv').dropna()
    
    print(index_df.loc[0, :])

    # last click feature, feature col: 'last_click'
    index_df['open_id'] = index_df['open_id'].astype(str)
    index_df['op_code'] = index_df['op_code'].astype(int).astype(str)

    index_df.drop(columns=['time'], inplace=True)

    # User
    user_df = get_embedding_user_feature()
    user_df['open_id'] = user_df['open_id'].astype(str)
    print(user_df.loc[0, :])

    # Item
    item_df = get_embedding_item_feature()
    item_df['op_code'] = item_df['op_code'].astype(int).astype(str)
    print(item_df.loc[0, :])

    train_data = pd.merge(index_df, user_df, on='open_id', how='inner')
    print(len(train_data))
    train_data = pd.merge(train_data, item_df, on='op_code', how='inner').dropna()
    print(len(train_data))

    user_cols = user_df.drop(columns=['open_id']).columns
    item_cols = item_df.drop(columns=['op_code']).columns

    user_feature = train_data[list(user_cols)].values
    item_feature = train_data[list(item_cols)].values
    train_labels = train_data['label'].values
    print(user_feature.shape)
    print(item_feature.shape)

    feature_range = max(user_feature.max(), item_feature.max()) + 1

    uie = UserItemEmbedding(
        num_user_feature=user_feature.shape[1],
        num_item_feature=item_feature.shape[1],
        max_feature_dim=feature_range
    )
    uie.train((user_feature, item_feature, train_labels))

    uie.save('./data/user_item_embedding_model/')

    users = user_df['open_id'].to_list()
    user_emb_features = user_df[user_cols].values
    user_vectors = uie.get_user_embedding(user_emb_features)

    items = item_df['op_code'].to_list()
    item_ebm_features = item_df[item_cols].values
    item_vectors = uie.get_item_embedding(item_ebm_features)

    save_list(users, USER_ITEM_EMBEDDING_USER)
    np.save(USER_ITEM_EMBEDDING_USER_VEC, user_vectors)

    save_list(items, USER_ITEM_EMBEDDING_ITEM)
    np.save(USER_ITEM_EMBEDDING_ITEM_VEC, item_vectors)


if __name__ == '__main__':
    # hive_unit = HiveUnit(**HIVE_CONFIG)
    hive_unit = None
    run(hive_unit)

