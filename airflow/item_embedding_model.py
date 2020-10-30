"""
train item embedding model,
used for generating most similar items for specific item
user-item data -> item sequence -> item2vec model -> item2item
read 180 days user behavior data: 15 minutes
train item embedding model: 15 minutes
generate item2item dict: 1 minute
"""
from datetime import datetime
from typing import Dict

from gensim.models import Word2Vec

from lib.datastructure.config import HIVE_CONFIG
from lib.datastructure.files import ITEM_EMBED_MODEL, ITEM2ITEM
from lib.db.hive_utils import HiveUnit
from lib.model.embedding import ItemEmbeddingGenSim
from lib.preprocessing.gen_user_historical_behavior import get_user_item
from lib.utils.utils import load_json


def train_item_embedding():
    """train item embedding model and gen item2item
    """
    print(datetime.now(), 'generate last 180 days user behavior data start...')
    hive_unit = HiveUnit(**HIVE_CONFIG)
    user_item_df = get_user_item(hive_unit, days=180)
    hive_unit.release()
    print(datetime.now(), 'generate last 180 days user behavior data done')
    item_embedding = ItemEmbeddingGenSim()
    item_embedding.gen_seq(user_item_df)
    item_embedding.gen_w2v_model()
    print(datetime.now(), 'train item embedding model done')
    item_embedding.dump(ITEM_EMBED_MODEL)
    item_embedding.gen_sim_dict(ITEM2ITEM)
    print(datetime.now(), 'generate item2item done')


def load_item_embedding_model() -> Word2Vec:
    item_embedding = ItemEmbeddingGenSim()
    item_embedding.load(ITEM_EMBED_MODEL)
    return item_embedding.model


def get_item2item() -> Dict:
    return load_json(ITEM2ITEM)


if __name__ == "__main__":
    train_item_embedding()
