import pandas as pd

from lib.datastructure.constants import USER_ENCODE_COLS
from lib.datastructure.files import RANKING_USER_DF, RANKING_ITEM_DF, LAST_CLICK_FEATURE_FN, EMBEDDING_USER_DF, \
    EMBEDDING_ITEM_DF
from lib.db.hive_utils import HiveUnit
from lib.processing.encoder import padding, UserEncoder, cut_name, ItemEncoder
from lib.processing.for_ranking import gen_user_df, gen_item_df, gen_neg_items, LastClick
from lib.processing.user_item_profile import gen_num_process


class FeatureGenerator:
    def __init__(self, hive_unit: HiveUnit):
        """

        Parameters
        ----------
        hive_unit
        """
        self.hive_unit = hive_unit

        self.user_encoder = UserEncoder()
        self.ranking_user_feature = None
        self.embedding_user_feature = None

        self.raw_item_df, self.item_encoder = self.load_raw_item()
        self.ranking_item_feature = None
        self.embedding_item_feature = None

    def gen_ranking_user_df(self):
        user_df = gen_user_df(self.hive_unit)
        user_df = padding(user_df, USER_ENCODE_COLS, {})
        ranking_user_df = self.user_encoder.gen_for_ranking(user_df)
        self.ranking_user_feature = ranking_user_df
        return ranking_user_df

    def gen_embedding_user_df(self):
        user_df = gen_num_process(self.hive_unit)
        user_df = padding(user_df, USER_ENCODE_COLS, {})
        embedding_user_df = self.user_encoder.gen_for_embedding(user_df)
        self.embedding_user_feature = embedding_user_df
        return embedding_user_df

    def load_raw_item(self):
        item_df = gen_item_df(self.hive_unit)
        item_df = item_df.rename(columns={'product_id': 'op_code'})
        item_df = cut_name(item_df, "standardskuname")
        item_encode_cols = [x for x in item_df.columns if x != 'op_code']
        item_df = padding(item_df, item_encode_cols, {})
        item_encoder = ItemEncoder(item_encode_cols)
        return item_df, item_encoder

    def gen_ranking_item_df(self):
        ranking_item_df = self.item_encoder.gen_for_ranking(self.raw_item_df)
        self.ranking_item_feature = ranking_item_df
        return ranking_item_df

    def gen_embedding_item_df(self):
        embedding_item_df = self.item_encoder.gen_for_embedding(self.raw_item_df)
        self.embedding_item_feature = embedding_item_df
        return embedding_item_df

    def dump(self):
        if self.ranking_user_feature is not None:
            self.ranking_user_feature.to_csv(RANKING_USER_DF, index=False)
        if self.ranking_item_feature is not None:
            self.ranking_item_feature.to_csv(RANKING_ITEM_DF, index=False)
        if self.embedding_user_feature is not None:
            self.embedding_user_feature.to_csv(EMBEDDING_USER_DF, index=False)
        if self.embedding_item_feature is not None:
            self.embedding_item_feature.to_csv(EMBEDDING_ITEM_DF, index=False)


def get_ranking_user_feature():
    return pd.read_csv(RANKING_USER_DF)


def get_ranking_item_feature():
    return pd.read_csv(RANKING_ITEM_DF)


def get_embedding_user_feature():
    return pd.read_csv(EMBEDDING_USER_DF)


def get_embedding_item_feature():
    return pd.read_csv(EMBEDDING_ITEM_DF)


def gen_last_click_feature():
    behavior_df = gen_neg_items()
    behavior_df['time'] = pd.to_datetime(behavior_df['time'])
    # Generate and save last-click feature
    last_click = LastClick()
    last_click.gen_click_map(behavior_df)
    last_click.dump(LAST_CLICK_FEATURE_FN)

