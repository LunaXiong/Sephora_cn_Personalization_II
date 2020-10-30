# -*- coding:utf-8 -*-
import pandas as pd
import json

raw_df = pd.read_csv("./data/user_session.csv")
raw_df = raw_df.sort_values(by=['user_id_session', 'time'])
raw_df['op_code'] = raw_df['op_code'].map(lambda x: int(x))
raw_df = raw_df.groupby('user_id_session')['op_code']. \
    apply(lambda x: x.to_list()).reset_index()
raw_df['item_seqs'] = ""

for i in range(len(raw_df['user_id_session'])):
    #     print(raw_df.iloc[i][1])
    raw_df['item_seqs'][i] = len(raw_df.iloc[i][1])

raw_df = raw_df[~raw_df['item_seqs'].isin([1, 2])]
raw_df = raw_df.rename(columns={0: 'seqs'})
# raw_df['op_code'].to_csv("seq.csv")
print(raw_df['op_code'])

code_list = raw_df['op_code'].to_list()
code_json = json.dumps(code_list)
file = open('user_session_fine_tune.json', 'w')
file.write(code_json)
file.close()
