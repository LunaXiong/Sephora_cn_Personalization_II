# -*- coding:utf-8 -*-
from typing import List, Dict

import numpy as np
import pandas as pd
from datetime import datetime

from lib.datastructure.files import BRAND_MAPPING, BRAND_CORRECTING, KW2POP_SCORE, ASSOCIATED_KEYWORD_FILE, \
    BRAND_KW_FILE, KW_POP, KW_PRIORITY, BRAND_LIST, KW_POP_TOP, KW2POP_SCORE_TOP, \
    KW_PRIORITY_TOP, ASSOCIATED_KEYWORD_FILE_TOP
from lib.db.hive_utils import HiveUnit
from lib.model.linking import get_kw2item
from lib.processing.item_pop import click_pop, purchase_pop
from lib.utils.utils import dump_json, load_json


class KeywordRanking:
    """rank keyword list
    1. select keyword list which contain brand name
    2. for brand, select high priority keyword
    3. rank all keywords according average click count
    """

    def __init__(self):
        """
        brand_kw_df: df, columns: [kw_standard, kw_type, kw]
        brand_priority: df, columns: [kw, Priority]
        kw_pop: df, columns: [kw, no. of product, no. of click]
        """

        self.kw2pop_score, self.kw2pop_score_top = get_kw2pop_score()
        self.kw_priority, self.kw_priority_top = get_kw_priority()

    def ranking(self, kw_lst):
        reserved_kws = {kw: self.kw_priority.get(kw, 1) for kw in kw_lst}
        kw_click_scores = {}
        for kw in reserved_kws.keys():
            kw_click_scores[kw] = self.kw2pop_score.get(kw, 0)
        min_s, max_s = min(kw_click_scores.values()), max(kw_click_scores.values())
        ret = []
        for kw in kw_click_scores:
            ret.append((kw, (kw_click_scores[kw] - min_s + 0.0001) / (max_s + 0.0001) - reserved_kws[kw]))
        ret = sorted(ret, key=lambda x: x[1], reverse=True)
        return [kw[0] for kw in ret]

    def ranking_top(self, kw_lst):
        reserved_kws = {kw: self.kw_priority_top.get(kw, 1) for kw in kw_lst}
        kw_click_scores = {}
        for kw in reserved_kws.keys():
            kw_click_scores[kw] = self.kw2pop_score_top.get(kw, 0)
        min_s, max_s = min(kw_click_scores.values()), max(kw_click_scores.values())
        ret = []
        for kw in kw_click_scores:
            ret.append((kw, (kw_click_scores[kw] - min_s + 0.0001) / (max_s + 0.0001) - reserved_kws[kw]))
        ret = sorted(ret, key=lambda x: x[1], reverse=True)
        return [kw[0] for kw in ret]


def preprocess_query(query):
    if not isinstance(query, str):
        query = str(query)
    if query:
        query = query.strip().replace(' ', '').upper()
    return query


def get_brand_kw_df():
    return pd.read_excel(BRAND_KW_FILE)


def gen_brand_correcting(brand_correcting_file: str = BRAND_LIST,
                         sheet: str = 'stkw_brand'):
    brand_correcting_df = pd.read_excel(brand_correcting_file, sheet_name=sheet)
    brand_correcting = {k: v for k, v in zip(brand_correcting_df['kw'].values, brand_correcting_df['stkw'].values)}
    dump_json(brand_correcting, BRAND_CORRECTING)


def get_brand_correcting():
    return load_json(BRAND_CORRECTING)


def gen_brand_mapping(brand_mapping_file: str = BRAND_LIST,
                      sheet: str = 'stkw_brand_mapping'):
    brand_mapping_df = pd.read_excel(brand_mapping_file, sheet_name=sheet)
    brand_mapping = {k: v for k, v in zip(brand_mapping_df['kw'].values, brand_mapping_df['stkw'].values)}
    dump_json(brand_mapping, BRAND_MAPPING)


def get_brand_mapping():
    return load_json(BRAND_MAPPING)


def gen_kw2pop_score(pop_type: str, hive_unit: HiveUnit, n_day: int):
    kw2item = get_kw2item()
    # item2pop_score
    if pop_type == 'click':
        pop_df = click_pop(hive_unit, n_day)
    elif pop_type == 'purchase':
        pop_df = purchase_pop(hive_unit, n_day)
    else:
        print('Pop Type Not Support!')
        return None
    pop_df.dropna(inplace=True)
    pop_df['op_code'] = pop_df['op_code'].astype(int).astype(str)
    item2pop_score = {k: v for k, v in zip(pop_df['op_code'].values, pop_df['pop_cnt'].values)}
    # kw2item + item2pop_score = kw2pop_score
    kw2pop_score = {}
    for kw, items in kw2item.items():
        pop_scores = []
        for item in items:
            pop_scores.append(item2pop_score.get(item, 0))
        kw2pop_score[kw] = np.mean(pop_scores)
    dump_json(kw2pop_score, KW2POP_SCORE)


def gen_kw2pop_score_old(kw_pop_df):
    kw_pop_df = pd.read_csv(kw_pop_df)
    kw_pop_df = kw_pop_df[kw_pop_df['no. of product'] > 0]
    kw_pop_df['pop_score'] = kw_pop_df['no. of click'] / kw_pop_df['no. of product']
    kw2pop_score = {k: v for k, v in zip(kw_pop_df['kw'].values, kw_pop_df['pop_score'].values)}
    return kw2pop_score


def gen_dif_score():
    # generate total kw pop score
    print(datetime.now(), 'generate top_n op_code score start...')
    dump_json(gen_kw2pop_score_old(KW_POP), KW2POP_SCORE)
    # generate top4 kw pop score
    print(datetime.now(), 'generate top_n op_code score start...')
    dump_json(gen_kw2pop_score_old(KW_POP_TOP), KW2POP_SCORE_TOP)


def get_kw2pop_score():
    kw2pop_score = load_json(KW2POP_SCORE)
    kw2pop_score_top = load_json(KW2POP_SCORE_TOP)
    return kw2pop_score, kw2pop_score_top


def gen_kw_priority(kw_priority_df):
    kw_priority_df = pd.read_excel(kw_priority_df).fillna(1)
    kw_priority_df['kw'] = [str(w).upper() for w in kw_priority_df['Associated Keyword']]
    kw_priority_df['kw_priority'] = kw_priority_df['Kw1Priority'] * kw_priority_df['Kw2Priority']
    kw_priority = {row['kw']: row['kw_priority'] for _, row in kw_priority_df.iterrows()}
    return kw_priority


def gen_dif_priority():
    # generate total kw pop priority
    print(datetime.now(), 'generate total op_code priority start...')
    dump_json(gen_kw_priority(ASSOCIATED_KEYWORD_FILE), KW_PRIORITY)
    # generate top4 kw pop priority
    print(datetime.now(), 'generate top_n op_code priority start...')
    dump_json(gen_kw_priority(ASSOCIATED_KEYWORD_FILE_TOP), KW_PRIORITY_TOP)


def get_kw_priority():
    kw2pop_priority = load_json(KW_PRIORITY)
    kw2pop_priority_top = load_json(KW_PRIORITY_TOP)
    return kw2pop_priority, kw2pop_priority_top

