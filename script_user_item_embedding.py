import pandas as pd
import numpy as np

from lib.utils.utils import load_json
from lib.model.nn import UserItemEmbedding
np.random.seed(111)


user_feature = pd.read_csv('./data/nn/user_feature.csv')
item_feature = pd.read_csv('./data/nn/item_feature.csv').rename(columns={'product_id': 'op_code'})
user_feature_map = load_json('./data/nn/user_feature_map.json')
item_feature_map = load_json('./data/nn/item_feature_map.json')

feature_range = max(max(user_feature_map.values()), max(item_feature_map.values()))

users = user_feature['open_id']
items = item_feature['op_code']


behavior_data = pd.read_csv('./data/behavior_online0921.csv').drop_duplicates().drop(columns='time').dropna()
behavior_data['op_code'] = behavior_data['op_code'].astype('int')

behavior_scores = {'click': 1, 'order': 3}
behavior_data['label'] = behavior_data['behavior'].apply(lambda x: behavior_scores[x])
behavior_data = behavior_data.drop(columns=['behavior'])


emb_users = behavior_data['open_id'].drop_duplicates().to_list()
emb_items = behavior_data['op_code'].drop_duplicates().to_list()


neg_data = []
for item in emb_items:
    item_rel_users = behavior_data.\
        where(behavior_data['op_code'] == item)['open_id'].drop_duplicates().to_list()

    neg_users = np.random.choice(
        [x for x in emb_users if x not in set(item_rel_users)],
        len(item_rel_users), replace=False
    )
    for u in neg_users:
        neg_data.append([item, u])

neg_data = pd.DataFrame(neg_data, columns=['op_code', 'open_id'])
neg_data['label'] = 0

train_index = pd.concat((behavior_data, neg_data)).reset_index(drop=True).sample(frac=1)

train_data = pd.merge(train_index, user_feature, on=['open_id'], how='inner')
train_data = pd.merge(train_data, item_feature, on=['op_code'], how='inner')
train_data = train_data.dropna()

user_cols = user_feature.drop(columns=['open_id']).columns
item_cols = item_feature.drop(columns=['op_code']).columns

user_feature = train_data[list(user_cols)].values
item_feature = train_data[list(item_cols)].values
train_labels = train_data['label'].values

uie = UserItemEmbedding(
    num_user_feature=user_feature.shape[1],
    num_item_feature=item_feature.shape[1],
    max_feature_dim=feature_range
)

uie.train((user_feature, item_feature, train_labels))


