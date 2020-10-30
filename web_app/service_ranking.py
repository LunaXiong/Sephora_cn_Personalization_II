import timeit
from datetime import datetime
import numpy as np
import pandas as pd
from gensim.models import Word2Vec
from tqdm import tqdm
import sys
sys.path.append('./')

from airflow.query_embedding_model import load_query_embedding_model, get_query2query
from lib.datastructure.config import HIVE_CONFIG
from lib.datastructure.constants import USER_FEATURE_COLS
from lib.datastructure.files import LAST_CLICK_FEATURE_FN, LGB_RANKING_FN, USER_ITEM_EMBEDDING_USER, \
    USER_ITEM_EMBEDDING_USER_VEC, USER_ITEM_EMBEDDING_ITEM, USER_ITEM_EMBEDDING_ITEM_VEC, QUERY2QUERY, \
    QUERY_EMBED_MODEL, HISTORY_QUERY_1000
from lib.db.hive_utils import HiveUnit
from lib.model.linking import get_user2tag
from lib.model.ranking import RegressionRanking
from lib.model.recall import Recall
from lib.preprocessing.gen_user_historical_behavior import get_query_df
from lib.processing.during_search import preprocess_query
from lib.processing.for_ranking import LastClick, QueryProcessor
from lib.processing.gen_dataset import gen_batch_dataset
from lib.processing.gen_feature import get_ranking_user_feature, get_ranking_item_feature
from lib.utils.nlp_ops import del_dupe
from lib.utils.utils import load_list
from web_app.service_recall import RecallService


class RankingService:
    def __init__(self,
                 last_click: LastClick,
                 user_feature: pd.DataFrame,
                 item_feature: pd.DataFrame,
                 ranking_model: RegressionRanking,
                 query_processor: QueryProcessor
                 ):

        self.item_feature = item_feature
        self.last_click = last_click
        self.query_processor = query_processor

        self.user_col = 'open_id'
        self.item_col = 'op_code'
        self.time_col = 'time'

        # User feature matrix: 2-D array
        # user-index map: {open_id: index}
        # User feature column index: {col_name: index}
        self.user_feature_mat, self.user_index = self._transfer_user(user_df=user_feature)
        self.user_feature_col_index = {col: idx for idx, col in enumerate(USER_FEATURE_COLS)}
        self.ranking_model = ranking_model

        # self.uie_user_index, self.uie_user_mat, self.uie_item_index, self.uie_item_mat = \
        #     self._load_user_item_embedding()

        self.default_rec = []

    @staticmethod
    def _load_user_item_embedding():
        users = load_list(USER_ITEM_EMBEDDING_USER)
        user_vec = np.load(USER_ITEM_EMBEDDING_USER_VEC)
        items = load_list(USER_ITEM_EMBEDDING_ITEM)
        item_vec = np.load(USER_ITEM_EMBEDDING_ITEM_VEC)

        return {x[1]: x[0] for x in enumerate(users)}, user_vec,\
               {x[1]: x[0] for x in enumerate(items)}, item_vec

    def _transfer_user(self, user_df):
        user_feature_mat = user_df[USER_FEATURE_COLS].values
        user_index = {open_id: idx for idx, open_id in enumerate(user_df['open_id'])}
        return user_feature_mat, user_index

    def process(self, seed_items: pd.DataFrame, rela_items: pd.DataFrame, top_k: int):
        """
        Main process function for ranking in the service
        :param index_df: DataFrame, [open_id, op_code, query, time], res of recall module
        :param top_k: return top-k items according predicted probability
        :return [{"opcode": , "skuid": , "stock":}]
        """
        if seed_items.empty and rela_items.empty:
            flag = 0
            rec_list = self.default_rec
        else:
            flag = 1
            seed_res = self._rank(seed_items)
            if not rela_items.empty:
                rela_res = self._rank(rela_items)
                seed_res.extend(rela_res)
            if type(top_k) == int:
                rec_list = del_dupe(seed_res)[0:top_k]
            elif type(top_k) == str and top_k.isdigit():
                rec_list = del_dupe(seed_res)[0:int(top_k)]
            else:
                rec_list = del_dupe(seed_res)
        return self._process_format(rec_list), flag

    def _rank(self, index_df):
        try:
            pred_data = self._gen_pred_data(index_df)
            pred_res = self.ranking_model.predict(pred_data, reserve_feature_cols=False, treelite=False)
            pred_res.dropna(inplace=True)
            pred_res['op_code'] = pred_res['op_code'].astype(float).astype(int)
            rank_scores = pred_res.sort_values(by='prediction', ascending=False)
            rec_list = list(rank_scores['op_code'].values)
        except:
            rec_list = list(index_df['op_code'].values)
        return rec_list

    def _gen_pred_data(self, index_df):
        return gen_batch_dataset(index_df, self.last_click, self.query_processor,
                                 self.user_feature_mat, self.user_index,
                                 self.user_feature_col_index, self.item_feature)

    def _process_format(self, rec):
        return rec


def ranking_test():
    print(datetime.now(), 'load recall service...')
    recall_model = Recall()
    recall_service = RecallService(recall_model)
    print(datetime.now(), 'load rank service...')
    ranking_model = RegressionRanking.load(LGB_RANKING_FN, load_tl=True)
    last_click = LastClick.load(LAST_CLICK_FEATURE_FN)
    user_feature = get_ranking_user_feature()
    item_feature = get_ranking_item_feature()
    query_embedding_model = load_query_embedding_model()
    query_processor = QueryProcessor(query_embedding_model)
    ranking_service = RankingService(last_click, user_feature, item_feature, ranking_model, query_processor)
    print(datetime.now(), 'load recall service and rank service done')
    users = ['oCOkA5UNHIo7BCBm3aj6-']
    # queries = ['反转巴黎', '精华', '雅诗兰黛', '粉底', '口红', '面膜']
    query = pd.read_csv(HISTORY_QUERY_1000)
    query_list = query['query'].to_list()[0:1000]
    res_all = []

    time_list = []

    for query in query_list:
        query = preprocess_query(query)
        seed_items, rela_items = recall_service.process(users[0], query)
        start_time = timeit.default_timer()
        res, _ = ranking_service.process(seed_items, rela_items, 10)
        end_time = timeit.default_timer()
        time_list.append(end_time-start_time)
        print(query)
        print(end_time-start_time)
        res_all.append(res)

    # for query in query_list:
    #     query = preprocess_query(query)
    #     seed_items, rela_items = recall_service.process(users[0], query)
    #     start_time = timeit.default_timer()
    #     res, _ = ranking_service.process(seed_items, rela_items, top_k=18)
    #     print(query)
    #     print(res)

    time_list = pd.DataFrame({'query': query_list, 'time': time_list, 'result': res_all})
    time_list['open_id'] = 'oCOkA5UNHIo7BCBm3aj6-'
    time_list.to_excel('rank_performance_10000.xlsx', index=False)
    # res.to_csv('recall_res_test.csv', index=False)


if __name__ == "__main__":
    ranking_test()
