import timeit
from datetime import datetime, date

import numpy as np
import pandas as pd
from tqdm import tqdm

from lib.datastructure.config import HIVE_CONFIG
from lib.db.hive_utils import HiveUnit
from lib.model.linking import get_user2tag
from lib.model.recall import Recall
from lib.preprocessing.gen_user_historical_behavior import get_query_df
from lib.processing.during_search import get_brand_correcting, get_brand_mapping


class RecallService:
    def __init__(self, recall_obj: Recall):
        self.recall_obj = recall_obj
        self.brand_correct = get_brand_correcting()
        self.brand_mapping = get_brand_mapping()

    def process(self, user: str, query: str):
        """
        Main function to process user and query and generate recall result
        Note that there needs to be a one-to-one correspondence between User and Query
        Parameters
        ----------
        user: str or List[str], open_id
        query: str or List[str], query

        Returns
        -------
        recall_res: DataFrame, [open_id, op_code]
        """
        query = query.lstrip()
        query = self._brand_correcting(query)
        query = self._brand_mapping(query)
        seed_items, rela_items = self.recall_obj.recall(query)
        rela_items = rela_items[:len(seed_items)//2]
        seed_res = pd.DataFrame({'op_code': seed_items})
        rela_res = pd.DataFrame({'op_code': rela_items})
        seed_res['open_id'] = user
        seed_res['query'] = query
        seed_res['time'] = str(date.today())
        rela_res['open_id'] = user
        rela_res['query'] = query
        rela_res['time'] = str(date.today())
        return seed_res, rela_res

    def _brand_correcting(self, word):
        if word in self.brand_correct.keys():
            return self.brand_correct[word]
        else:
            return word

    def _brand_mapping(self, word):
        if word in self.brand_mapping.keys():
            return self.brand_mapping[word]
        else:
            return word


def recall_test():
    # print(datetime.now(), 'read data and construct Trie and Pinyin Trie start...')
    # user2tag = get_user2tag()
    # users = list(user2tag.keys())
    # len_user = len(users)
    # print(len_user)
    # hive_unit = HiveUnit(**HIVE_CONFIG)
    # query_df = get_query_df(hive_unit)
    # hive_unit.release()
    # queries = list(query_df['keyword'].values)
    # len_query = len(queries)
    # print(len_query)
    # if len_user > len_query:
    #     users = users[:len_query]
    # else:
    #     queries = queries[:len_user]
    # print(datetime.now(), 'load recall model...')
    my_recall = Recall()
    recall_service = RecallService(my_recall)
    print(datetime.now(), 'load recall model done, recall service start...')
    users = ['oCOkA5UNBIjameYTHoycT3GQd8wM', 'oCOkA5ZMkOWBmWG5v9CzDld8rEgo']
    queries = ['反转巴黎']
    time_list = []
    for user, query in tqdm(zip(users, queries)):
        start_time = timeit.default_timer()
        seed_items, rela_items = recall_service.process(user, query)
        end_time = timeit.default_timer()
        time_list.append((end_time-start_time))
    print(max(time_list), min(time_list), np.mean(time_list))
    print(seed_items)
    print(rela_items)
    # time_list = pd.DataFrame({'time': time_list})
    # time_list.to_excel('recall_performance_10000.xlsx', index=False)
    # rec_res.to_csv('recall_res_test.csv', index=False)


if __name__ == "__main__":
    recall_test()
