from lib.datastructure.files import USER_EMBEDDING_ENCODE_MAP, ITEM_EMBEDDING_ENCODE_MAP
from lib.processing.encoder import UserEncoder, ItemEncoder


class UserItemEmbeddingService:
    def __init__(self, user_item_embedding, ):
        self.user_item_embedding = user_item_embedding
        self.user_encoder = UserEncoder()
        self.item_encoder = ItemEncoder(ranking_encode_cols=[])

        self.user_encoder.load(emb_fn=USER_EMBEDDING_ENCODE_MAP)
        self.item_encoder.load(emb_fn=ITEM_EMBEDDING_ENCODE_MAP)

    def get_user_vec(self, user_df):
        user_df = self.user_encoder.apply_embedding_map(user_df)

    def get_item_vec(self, item_df):
        pass

