import numpy as np

from lib.model.nn import UserItemEmbedding

item_feature = np.load('./data/item_feature.npy')
user_feature = np.load('./data/user_feature.npy')
labels = np.load('./data/train_labels.npy')

print(item_feature.shape)
print(user_feature.shape)
print(labels.shape)

max_feature_dim = max(item_feature.max(), user_feature.max()) + 1
print(max_feature_dim)
emb_mod = UserItemEmbedding(user_feature.shape[1], item_feature.shape[1], max_feature_dim)

print(user_feature[0])
print(user_feature[-1])

emb_mod.train([user_feature, item_feature, labels])

user_emb = emb_mod.get_user_embedding(user_feature)
item_emb = emb_mod.get_item_embedding(item_feature)
print(user_emb[0])
print(user_emb[-1])

# np.save('./data/user_emb.npy', user_emb)
# np.save('./data/item_emb.npy', item_emb)

