from typing import List

from lib.datastructure.constants import format_unk, USER_ENCODE_COLS, PROD_TAG_COLS
from lib.utils.utils import dump_json, load_json


class Encoder:
    """
    Encode for sequential embedding: The element of the sequence will be treated the same way,
    every distinct element of the feature DataFrame will have its own ID.
    Encode for Light GBM model: The encoding will be column-wise, and only encode indicated columns.
    """

    def __init__(self, index_col: str, ranking_encode_cols: List[str]):
        self.index_col = index_col
        self.ranking_encode_cols = ranking_encode_cols
        self.emb_feature_map = None
        self.ranking_feature_map = None

    @staticmethod
    def _for_embedding(raw_df, index_col: str, start_index: int = 0):
        feature_map = []
        feature_cols = [x for x in raw_df.columns if x != index_col]
        for col in feature_cols:
            feature_map += raw_df[col].drop_duplicates().to_list()
        # In case the column does not have UNK values
        for col in feature_cols:
            if format_unk(col) not in feature_map:
                feature_map.append(format_unk(col))
        feature_map = {x[1]: x[0] for x in enumerate(feature_map, start=start_index)}
        # feature_map = {k: v + start_index for k, v in feature_map.items()}

        for col in feature_cols:
            raw_df[col] = raw_df[col].map(feature_map)

        return raw_df, feature_map

    @staticmethod
    def _for_ranking(raw_df, encode_cols):
        # reserve_cols = [x for x in raw_df.columns if x != index_col and x not in encode_cols]
        feature_map = {}
        for col in encode_cols:
            col_val = list(raw_df[col].drop_duplicates())
            if format_unk(col) not in col_val:
                col_val.append(format_unk(col))
            feature_map[col] = {x[1]: x[0] for x in enumerate(col_val)}
        for col in encode_cols:
            raw_df[col] = raw_df[col].map(feature_map[col])
        return raw_df, feature_map

    def _apply_embedding_map(self, raw_df, index_col):
        encode_cols = [x for x in raw_df.columns if x != index_col]
        for col in encode_cols:
            raw_df[col] = raw_df[col].apply(
                lambda x: self.emb_feature_map[x] if x in self.emb_feature_map
                else self.emb_feature_map[format_unk(col)])
        return raw_df

    def _apply_ranking_map(self, raw_df, encode_cols):
        for col in encode_cols:
            encode_map = self.ranking_feature_map[col]
            raw_df[col] = raw_df[col].apply(
                lambda x: encode_map[x] if x in encode_map else encode_map[format_unk(col)])
        return raw_df

    def gen_for_embedding(self, raw_df):
        emb_df, feature_map = self._for_embedding(raw_df, self.index_col)
        self.emb_feature_map = feature_map
        return emb_df

    def gen_for_ranking(self, raw_df):
        rank_df, feature_map = self._for_ranking(raw_df, self.ranking_encode_cols)
        self.ranking_feature_map = feature_map
        return rank_df

    def apply_embedding_map(self, raw_df):
        return self._apply_embedding_map(raw_df, self.index_col)

    def apply_ranking_map(self, raw_df):
        return self._apply_ranking_map(raw_df, self.ranking_encode_cols)

    def dump(self, emb_fn=None, rank_fn=None):
        if emb_fn and self.emb_feature_map:
            dump_json(self.emb_feature_map, emb_fn)
        if rank_fn and self.ranking_feature_map:
            dump_json(self.emb_feature_map, emb_fn)

    def load(self, emb_fn=None, rank_fn=None):
        if emb_fn:
            self.emb_feature_map = load_json(emb_fn)
        if rank_fn:
            self.ranking_feature_map = load_json(rank_fn)


class ItemEncoder(Encoder):
    def __init__(self, ranking_encode_cols):
        index_col = "op_code"
        super(ItemEncoder, self).__init__(index_col, ranking_encode_cols)


class UserEncoder(Encoder):
    def __init__(self):
        index_col = "open_id"
        ranking_encode_cols = USER_ENCODE_COLS
        super(UserEncoder, self).__init__(index_col, ranking_encode_cols)


def padding(raw_df, pad_cols, pad_val_map):
    """
    Pad raw DataFrame empty columns according to pad_val_map
    if pad_val_map is not given or not complete, default pad
    values is "{col}_UNK" for string type columns
    (as in lib.constants.format_unk) and 0 for digital columns.
    Parameters
    ----------
    raw_df: raw DataFrame
    pad_cols: columns to pad
    pad_val_map: {col: pad_val}

    Returns
    -------
    Padded DataFrame
    """
    dtypes = {k: str(v) for k, v in dict(raw_df.dtypes).items()}
    for col in pad_cols:
        if col in pad_val_map:
            raw_df[col] = raw_df[col].fillna(pad_val_map[col])
        elif dtypes[col] == 'object':
            raw_df[col] = raw_df[col].fillna(format_unk(col))
        else:
            raw_df[col] = raw_df[col].fillna(0)
    return raw_df


def cut_name(raw_df, cut_col, cut_len=5, pad="</PAD>", j=None):
    if j is None:
        import jieba
        from lib.utils.utils import jieba_wrap
        j = jieba_wrap(jieba)

    def cut_pad(s):
        cs = j.lcut(s)[:cut_len]
        if len(cs) < cut_len:
            cs += [pad for _ in range(cut_len - len(cs))]
        return cs

    raw_df[cut_col] = raw_df[cut_col].apply(cut_pad)
    for i in range(cut_len):
        raw_df[cut_col+"cut_%d" % i] = raw_df[cut_col].apply(lambda x: x[i])
    return raw_df.drop(columns=cut_col)

