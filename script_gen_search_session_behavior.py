# -*- coding:utf-8 -*-
import numpy as np
import pandas as pd

from lib.utils.utils import dump_json

raw_df = pd.read_csv('./data/user_session_time.csv').drop(columns='session_id')
raw_df['time'] = pd.to_datetime(raw_df['time'])
raw_df['op_code'] = raw_df['op_code'].apply(lambda x: str(int(x)) if not np.isnan(x) else "S")
raw_df = raw_df.sort_values(by=['user_id', 'time'])


raw_df = raw_df.groupby('user_id')[['time', 'behavior', 'op_code']].\
    apply(lambda x: [(row['time'], row['behavior'], row['op_code']) for index, row in x.iterrows()]).reset_index()

raw_seqs = raw_df[list(raw_df.columns)[1]].to_list()

search_sess_seq = []
d = pd.Timedelta(minutes=30)
for raw_seq in raw_seqs:
    search = False
    search_seq = []
    for triple in raw_seq:

        if triple[1] == "search" and len(search_seq) == 0:
            search = True
            search_seq.append(triple[2])
            search_time = triple[0]
            continue
        if search and (triple[0] - search_time) < d and triple[1] != 'search':
            search_seq.append(triple[2])
            continue
        if search and (triple[0] - search_time) >= d:
            search_sess_seq.append(search_seq)
            search = False
            search_seq = []
            continue
        if search and triple[1] == 'search':
            search_sess_seq.append(search_seq)
            search_seq = [triple[2]]
            continue
    if len(search_seq) > 0:
        search_sess_seq.append(search_seq)

search_sess_seq = [[k for k in x if k != 'S'] for x in search_sess_seq]
search_sess_seq = [x for x in search_sess_seq if len(x) > 1]

dump_json(search_sess_seq, './data/search_sess_behavior_seq.json')

