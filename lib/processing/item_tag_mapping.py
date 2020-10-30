"""use for generate and load item-tag mapping dict
"""
from collections import defaultdict

import jieba
import pandas as pd

from lib.datastructure.files import TAG2ITEM_BASIC, ITEM2TAG_BASIC
from lib.datastructure.files import TAG2ITEM_EXTENDED, ITEM2TAG_EXTENDED
from lib.datastructure.files import TAG2ITEM_NEW, ITEM2TAG_NEW
from lib.utils.nlp_ops import process_tag
from lib.utils.utils import dump_json, load_json, jieba_wrap

jieba = jieba_wrap(jieba)


# basic item-tag mapping
def gen_basic_tag2item(product_profile):
    _gen_tag2item(product_profile, TAG2ITEM_BASIC)


def get_basic_tag2item():
    return load_json(TAG2ITEM_BASIC)


def gen_basic_item2tag():
    tag2item = get_basic_tag2item()
    _gen_item2tag(tag2item, ITEM2TAG_BASIC)


def get_basic_item2tag():
    return load_json(ITEM2TAG_BASIC)


# combined item-tag mapping
def gen_extended_tag2item(product_profile):
    _gen_tag2item(product_profile, TAG2ITEM_EXTENDED)


def get_extended_tag2item():
    return load_json(TAG2ITEM_EXTENDED)


def gen_extended_item2tag():
    tag2item = get_extended_tag2item()
    _gen_item2tag(tag2item, ITEM2TAG_EXTENDED)


def get_extended_item2tag():
    return load_json(ITEM2TAG_EXTENDED)


# item-tag mapping for new product
def gen_new_tag2item(product_profile):
    _gen_tag2item(product_profile, TAG2ITEM_NEW)


def get_new_tag2item():
    return load_json(TAG2ITEM_NEW)


def gen_new_item2tag():
    tag2item = get_new_tag2item()
    _gen_item2tag(tag2item, ITEM2TAG_NEW)


def get_new_item2tag():
    return load_json(ITEM2TAG_NEW)


def _gen_tag2item(item_tag_df: pd.DataFrame, file_name: str, index_col: str = 'product_id'):
    """generate tag-item mapping dict
    :param item_tag_df: pd.DataFrame
    :param file_name: str
    :param index_col: str, col name of product
    return tag2item: dict, {tag: set(items), ...}
    """
    item_tag_df[index_col] = item_tag_df[index_col].astype('int')
    tag_cols = [col for col in item_tag_df.columns if col != index_col]
    tag2item = defaultdict(list)
    # tag2item
    for col in tag_cols:
        item_tag_df[col] = item_tag_df[col].apply(lambda x: process_tag(x))
        tag_df = item_tag_df[[index_col, col]].groupby(col)[index_col].apply(list).reset_index()
        for inx, row in tag_df.iterrows():
            tag2item[row[col]].extend(row[index_col])
    # cut_tag2item
    cut_tag2item = defaultdict(list)
    for tag, items in tag2item.items():
        cut_tags = jieba.cut_for_search(tag)
        for cut_tag in cut_tags:
            cut_tag2item[cut_tag].extend(items)
    overall_tag2item = {tag: list(set(items)) for tag, items in dict(tag2item, **cut_tag2item).items()}
    dump_json(overall_tag2item, file_name)


def _gen_item2tag(tag2item: dict, file_name: str):
    """generate item-tag mapping dict
    :param tag2item: dict, {tag: [items]}
    :param file_name: str
    return: item2tag: dict, {item: [tags]}
    """
    item2tag = defaultdict(list)
    for tag, items in tag2item.items():
        if tag:
            for item in items:
                item2tag[item].append(tag)
    item2tag = {item: list(set(tags)) for item, tags in item2tag.items()}
    dump_json(item2tag, file_name)
