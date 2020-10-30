"""
train user embedding model,
used for generating most similar items for specific item
user-session data -> user sequence -> user2vec model
read 180 days user behavior data: 15 minutes
train user embedding model: 15 minutes
generate user2user dict: 1 minute
"""
from datetime import datetime
from typing import Dict

from gensim.models import Word2Vec

from lib.datastructure.config import HIVE_CONFIG
from lib.datastructure.files import USER_EMBED_MODEL, USER2USER
from lib.db.hive_utils import HiveUnit
from lib.model.embedding import UserEmbedding
from lib.preprocessing.gen_user_historical_behavior import get_user_session
from lib.utils.utils import load_json


def train_user_embedding():
    """train user embedding model
    """
    print(datetime.now(), 'generate last 180 days user session data start...')
    hive_unit = HiveUnit(**HIVE_CONFIG)
    user_sample_df = get_user_session(hive_unit)
    user_sample_df['session_id'] = user_sample_df['session_id'].apply(str)
    user_sample_df['user_id'] = user_sample_df['user_id'].apply(str)
    hive_unit.release()
    print(datetime.now(), 'generate last 180 days user session data done')
    user_embedding = UserEmbedding()
    user_embedding.gen_seq(user_sample_df)
    user_embedding.gen_w2v_model()
    print(datetime.now(), 'train user embedding model done')
    user_embedding.dump(USER_EMBED_MODEL)
    user_embedding.gen_sim_dict(USER2USER)
    print(datetime.now(), 'generate user2user done')


def load_user_embedding_model() -> Word2Vec:
    user_embedding = UserEmbedding()
    user_embedding.load(USER_EMBED_MODEL)
    return user_embedding.model


def get_user2user() -> Dict:
    return load_json(USER2USER)


if __name__ == "__main__":
    train_user_embedding()



