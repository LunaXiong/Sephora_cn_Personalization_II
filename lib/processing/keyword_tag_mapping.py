"""keyword tag mapping functions
1. standard tag-keyword mapping, used for before-search
2. extended tag-keyword mapping, used for during-search
"""
from lib.datastructure.config import HIVE_CONFIG
from lib.datastructure.files import STANDARD_TAG2KW, STANDARD_KW2TAG, EXTENDED_KW2TAG, EXTENDED_TAG2KW, KW2TAG_FIXED, \
    PRODUCT_LIST, ITEM_TAG_FILE, TRANSLATION, KW2ITEM_NEW
from lib.db.hive_utils import HiveUnit
from lib.utils.df_ops import gen_dict_from_df
from lib.utils.utils import load_json, dump_json
import pandas as pd


def gen_standard_tag2kw(file_path: str):
    gen_dict_from_df(file_path, 'original tag', 'Keywords Proposed', STANDARD_TAG2KW)


def get_standard_tag2kw():
    return load_json(STANDARD_TAG2KW)


def gen_standard_kw2tag(file_path: str):
    gen_dict_from_df(file_path, 'Keywords Proposed', 'original tag', STANDARD_KW2TAG)


def get_standard_kw2tag():
    return load_json(STANDARD_KW2TAG)


def gen_extended_kw2tag(file_path: str):
    gen_dict_from_df(file_path, 'kw', 'kw_standard', EXTENDED_KW2TAG)


def get_extended_kw2tag():
    return load_json(EXTENDED_KW2TAG)


def gen_extended_tag2kw(file_path: str):
    gen_dict_from_df(file_path, 'kw_standard', 'kw', EXTENDED_TAG2KW)


def get_extended_tag2kw():
    return load_json(EXTENDED_TAG2KW)


def gen_fixed_kw2tag():
    # tags-products mapping
    prod_tags = pd.read_excel(ITEM_TAG_FILE)
    cols = prod_tags.columns.to_list()
    cols.remove('product_id')
    prod_tag_dic = {}
    for col in cols:
        tag_df = prod_tags[['product_id', col]].groupby(col)['product_id'].apply(lambda x: x.to_list()).reset_index()
        for inx, row in tag_df.iterrows():
            if len(list(set(row['product_id']))) > 3:
                prod_tag_dic[row[col]] = list(set(row['product_id']))

    # keyword-tags mapping
    hive_unit = HiveUnit(**HIVE_CONFIG)
    tag_kw_df = hive_unit.get_df_from_db("select kw, kw_standard from da_dev.search_kw_tag_mapping")
    hive_unit.release()
    tag_kw_dic = {}
    for inx, row in tag_kw_df.iterrows():
        tag_kw_dic[row['kw']] = row['kw_standard']

    # keywords-products mapping
    kw2item = {}
    for k, v in tag_kw_dic.items():
        kw2item[k] = prod_tag_dic.get(v)

    # item-tag
    prod_tags = pd.read_excel(PRODUCT_LIST)
    cols = prod_tags.columns.to_list()
    inx_lst = ['product_id', 'sku_code', 'sku_name', 'StandardSKUName', 'Category_CN', 'Brand_Type',
               'SubCategory_CN', 'ThirdCategory_CN', 'Brand_CN', 'Makeup_feature_color_CN', 'Fragrance_targetgender_CN',
               'Fragrance_Stereotype_CN', 'Fragrance_Intensity_CN', 'Fragrance_Impression_CN', 'Fragrance_Type_CN',
               'Bundleproduct_main_SKU']
    for col in inx_lst:
        cols.remove(col)
    item2tag = {}
    for inx, row in prod_tags.iterrows():
        prod = str(int(row['product_id']))
        item2tag[prod] = []
        for col in cols:
            tag = str(row[col])
            if tag != 'nan':
                item2tag[prod].append(str(row[col]))
        item2tag[prod] = list(set(item2tag[prod]))

    # kw-tag
    kw2tag = {}
    relate_lst = ['面膜', '香水', '精华']
    for k, v in kw2item.items():
        for kw in relate_lst:
            if k.startswith(kw) or k.endswith(kw):
                kw2tag[k] = []
                if v:
                    for prod in v:
                        kw2tag[k].extend(item2tag[str(prod)])
        if kw2tag.get(k, []):
            # print(kw2tag[k])
            tag_list = sorted(kw2tag[k], key=lambda x: kw2tag[k].count(x), reverse=True)
            kw2tag[k] = list(set(kw2tag[k]))
            kw2tag[k].sort(key=tag_list.index)
    print(kw2tag)
    # tag mapping
    trans_df = pd.read_excel(TRANSLATION)
    trans_dic = {row['TagNameEN']: row['TagNameCN'] for _, row in trans_df.iterrows()}
    print(trans_dic)
    kw2tag_ret = {}
    for k, v in kw2tag.items():
        kw2tag_ret[k] = []
        for tag in v:
            trans_tag = trans_dic.get(tag, tag)
            tag_upper = tag.strip().replace(' ', '').upper()
            trans_tag_upper = trans_tag.strip().replace(' ', '').upper()
            if not (k.startswith(tag_upper)) and not (k.endswith(tag_upper)) and not (
                    k.startswith(trans_tag_upper)) and not (k.endswith(trans_tag_upper)):
                kw2tag_ret[k].append(trans_dic.get(tag, tag))
        kw2tag_ret[k] = list(set(kw2tag_ret[k]))
    dump_json(kw2tag_ret, KW2TAG_FIXED)


def get_fixed_kw2tag():
    return load_json(KW2TAG_FIXED)


if __name__ == '__main__':
    gen_fixed_kw2tag()
