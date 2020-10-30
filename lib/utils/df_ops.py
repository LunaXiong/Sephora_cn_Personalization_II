import pandas as pd

from lib.utils.nlp_ops import process_tag
from lib.utils.utils import dump_json


def df_split(df: pd.DataFrame, batch_size: int):
    ret = []
    for i in range(0, len(df), batch_size):
        ret.append(df[i: i + batch_size])
    return ret


def gen_dict_from_df(file_name: str, key_col: str, val_col: str, dict_fn: str):
    """transform DataFrame with index and value col into dict
    :param file_name: str, dataframe file path
    :param key_col: str, column name of key
    :param val_col: str, column name of value
    :param dict_fn: str, dict file path
    """
    df = pd.read_excel(file_name)
    df[key_col] = df[key_col].apply(lambda x: process_tag(x))
    df[val_col] = df[val_col].apply(lambda x: process_tag(x))
    df = df.groupby(key_col)[val_col].apply(lambda x: x.to_list()).reset_index()
    d = {k: v for k, v in zip(df[key_col].values, df[val_col].values)}
    dump_json(d, dict_fn)