# -*- coding:utf-8 -*-

import pandas as pd

from lib.utils.utils import dump_json

raw_df = pd.read_excel('./data/词库生成列1.xlsx', sheet_name=2)
# print(raw_df)

ret = {}
for index, row in raw_df.iterrows():
    ret[row['details']] = row['Detail CN']

dump_json(ret, './data/tag_trans_map.json')
