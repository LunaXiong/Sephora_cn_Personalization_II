import json
from gensim.models.word2vec import Word2Vec
from lib.model.embedding import ItemEmbeddingGenSim


item_w2v = Word2Vec.load('./data/embedding/item_embedding')
v1 = item_w2v.wv.get_vector("2626")
print(v1)
filepath = './data/embedding/user_session_fine_tune.json'
with open(filepath, 'r') as f:
    seqs = json.load(f)
# print(seqs)
user_sess = ItemEmbeddingGenSim(model=item_w2v)
user_sess.fine_tune(seqs)

user_sess.dump('./data/embedding/item_fine_tune')
item_w2v2 = Word2Vec.load('./data/embedding/item_fine_tune')
v2 = item_w2v.wv.get_vector("2626")
print(v2)