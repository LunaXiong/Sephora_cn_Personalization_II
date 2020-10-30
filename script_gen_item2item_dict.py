"""
generate similar items according to item embedding vector
"""
from collections import defaultdict

from lib.model.embedding import ItemEmbeddingGenSim
from lib.utils.utils import dump_json, load_json

top_N = 10
min_sim_score = 0.63

# load item embedding model
item_embedding = ItemEmbeddingGenSim()
item_embedding.load('data/embedding/item_embedding')
# For each item, at most top-N items whose similar score with the specific item greater than min_sim_score
# with the highest similarity are retained
item2item_dict = defaultdict(list)
for item in item_embedding.model.wv.index2entity:
    item2item_dict[item].extend([{_[0]: _[1]} for _ in item_embedding.model.wv.most_similar(item, topn=top_N)])
    item2item_dict[item] = [k for sim_items in item2item_dict[item]
                            for k, v in sim_items.items()
                            if v >= min_sim_score]
# eval item2item_dict
for item, sim_items in item2item_dict.items():
    print(item, sim_items)
# save item2item recall res
dump_json(item2item_dict, './data/item2item.json')
# load item2item recall res
item2item_dict = load_json('./data/item2item.json')
