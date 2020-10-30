import time as t
import pandas as pd
from lib.model.recall import Recall
from lib.processing.for_ranking import gen_posi_items

neg = Recall()


def gen_neg_items():
    df_raw = gen_posi_items()
    # df_raw = pd.read_csv("./data/sample_click_aft_search.csv")
    querys = df_raw['query'].to_list()
    df_raw['query'].apply(str).str.upper()
    df_raw['query'] = df_raw['query'].str.upper()
    for i in range(0, len(querys)):
        try:
            num_list = neg.word_tag_prod(df_raw.loc[i, 'query'])
            num_list_new = map(lambda x: str(x), num_list[0:20])
            df_raw.loc[i, 'neg_items'] = '/'.join(num_list_new)
            # print('now time: ', t.time())
        except:
            continue
    # df_raw.to_csv("./data/search_neg_click.csv", index=None)
    return df_raw


if __name__ == '__main__':
    gen_neg_items()