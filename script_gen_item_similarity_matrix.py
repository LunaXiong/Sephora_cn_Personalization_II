"""
generate item similarity matrix according to item embedding
"""
import numpy as np

from lib.model.embedding import ItemEmbeddingGenSim

# load item embedding model
item_embedding = ItemEmbeddingGenSim()
item_embedding.load('data/embedding/item_embedding')

# item_index: numpy.ndarray(n,)
item_index = np.array(item_embedding.model.wv.index2word)
# item cosine similarity matrix: numpy.ndarray(n,n)
item_embedding.model.wv.init_sims()  # L2 normalization
item_sim_matrix = np.dot(item_embedding.model.wv.syn0norm,
                         item_embedding.model.wv.syn0norm.T)  # cosine similarity

# save item index and item sim matrix into a file
np.savez('data/embedding/item_sim_matrix.npz',
         item_index, item_sim_matrix, index=item_index, matrix=item_sim_matrix)
# load item index and item sim matrix
item_matrix = np.load('data/embedding/item_sim_matrix.npz')
item_index = item_matrix['index']
item_sim_matrix = item_matrix['matrix']
print(item_index)
print(item_sim_matrix.shape)