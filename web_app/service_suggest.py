import timeit
from datetime import datetime

import jieba
import numpy as np
import pandas as pd
from gensim.models import Word2Vec
from tqdm import tqdm

from airflow.query_embedding_model import load_query_embedding_model
from lib.datastructure.files import QUERY_KW_TEST, HISTORY_QUERY, BRAND_LIST, ASSOCIATED_KEYWORD_FILE, \
    ASSOCIATED_KEYWORD_FILE_TOP
from lib.model.skip_gram import SkipGram
from lib.model.trie import Trie, PinyinTRIE, RTrie
from lib.processing.during_search import preprocess_query, KeywordRanking, get_brand_mapping, get_brand_correcting
from lib.processing.keyword_tag_mapping import get_extended_kw2tag, get_fixed_kw2tag
from lib.utils.nlp_ops import del_dupe
from lib.utils.utils import dump_json


class SuggestService:

    def __init__(self,
                 trie: Trie,
                 pinyin_trie: PinyinTRIE,
                 rtrie: RTrie,
                 # embedding_model: Word2Vec,
                 suggest_rank: KeywordRanking):

        self.trie_model = trie
        self.pinyin_trie_model = pinyin_trie
        self.rtrie_model = rtrie

        # self.kw2tag = get_extended_kw2tag()
        self.kw2tag = get_fixed_kw2tag()
        # self.skip_gram_model = SkipGram(embedding_model)
        self.suggest_rank = suggest_rank
        self.brand_correct = get_brand_correcting()
        self.brand_mapping = get_brand_mapping()
        # self.suggest_rank = suggest_rank

    def process(self, query: str, top_k: int = 10, total_k: int = 5, tag_n: int = 4, kw_n: int = 8):
        """
        Main function to process query and generate suggest keyword and tags
        keywords: query, Trie correct keyword, pinyin Trie correct keyword,
        Trie prefix keyword, PinYin Trie prefix keyword
        Parameters
        ----------
        query: str
        top_k: int, the maximum number of keywords suggested
        total_k: int, the number of total op_code data suggested

        Returns
        -------
        suggest_kw: [{'kw': keyword, 'tags': [tag1, tag2]}, ]
        """
        query = preprocess_query(query)
        keywords = self._gen_keywords(query, top_k, ASSOCIATED_KEYWORD_FILE)
        keywords_top = self._gen_keywords(query, top_k, ASSOCIATED_KEYWORD_FILE_TOP)

        keywords = keywords[:total_k]
        for key in keywords_top:
            if key not in keywords:
                keywords.append(key)
        keywords = keywords[: kw_n]
        res = []
        for kw in keywords:
            kw_res = {'kw': kw}
            if kw in self.kw2tag.keys():
                kw_res['tags'] = self.kw2tag[kw][:tag_n]
            else:
                kw_res['tags'] = []
            res.append(kw_res)
        return res

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

    def _gen_stmap(self, mapping: pd.DataFrame):
        st_dic = {}
        for item in mapping.iterrows():
            for kw in item[1]['kw'].split(','):
                st_dic[kw] = item[1]['stkw']
        return st_dic

    def _gen_associated_kw(self, associated_path):
        word_list = pd.read_excel(associated_path)[['Associated Keyword']]  # keyword set
        word_list = word_list.astype('str')['Associated Keyword']

        self.trie_model.add_words(word_list, pruning=False)
        self.pinyin_trie_model.add_words(word_list, pruning=False)
        self.rtrie_model.add_words(word_list, pruning=False)

    def _gen_keywords(self, query: str, top_k: int, associated_path):
        stw = query.lstrip()
        stw = self._brand_correcting(stw)
        stw_v1 = self._brand_mapping(stw)

        if stw_v1 != stw:
            suggest_kw = [stw_v1]
        else:
            suggest_kw = []
            self._gen_associated_kw(associated_path)
            trie_prefix_kw, trie_kw = self.trie_model.query(stw)
            rtrie_prefix_kw, rtrie_kw = self.rtrie_model.query(stw)
            pinyin_trie_prefix_kw, pinyin_trie_kw = self.pinyin_trie_model.query(stw)
            suggest_kw.extend(trie_prefix_kw)
            if len(suggest_kw) < top_k:
                suggest_kw.extend(rtrie_prefix_kw)
                if len(suggest_kw) < top_k:
                    suggest_kw.extend(pinyin_trie_prefix_kw)
                    if len(suggest_kw) < top_k:
                        suggest_kw.extend(trie_kw)
                        if len(suggest_kw) < top_k:
                            suggest_kw.extend(rtrie_kw)
                            if len(suggest_kw) < top_k:
                                suggest_kw.extend(pinyin_trie_kw)

        suggest_kw = del_dupe(suggest_kw)[:top_k]
        if len(suggest_kw) > 1:
            if associated_path == "ASSOCIATED_KEYWORD_FILE":
                suggest_kw = self.suggest_rank.ranking(suggest_kw)
            else:
                suggest_kw = self.suggest_rank.ranking_top(suggest_kw)
        res = [stw] + suggest_kw[:top_k]
        return res


def suggest_test():
    # Trie
    print(datetime.now(), 'read data and construct Trie and Pinyin Trie start...')
    data_path = "D:/search_test/"
    trie_model = Trie()
    pinyin_trie = PinyinTRIE()
    rtrie_model = RTrie()
    # todo comfirm keyword set using for construct Trie and PinYin Trie

    # load query embedding model
    print(datetime.now(), 'read data and construct Trie and Pinyin Trie done, load item embedding model...')
    # query_embedding_model = load_query_embedding_model()
    # load keyword ranking model
    print(datetime.now(), 'load item embedding model done, load keyword ranking model...')
    keyword_ranking = KeywordRanking()
    # load keyword suggest service
    print(datetime.now(), 'load keyword ranking model done, load suggest service...')
    suggest_service = SuggestService(trie_model, pinyin_trie, rtrie_model, keyword_ranking)
    # suggest service test
    print(datetime.now(), 'load suggest service done, read query data...')
    # historical_query = pd.read_csv(HISTORY_QUERY)
    # historical_query.sort_values(by='search_cnt', ascending=False, inplace=True)
    # queries = historical_query['query'].values
    queries = ['香水', '面膜', '精华']
    # print(datetime.now(), 'read query data done, generate keyword and tag...')
    time_list = []
    for query in tqdm(queries):
        start_time = timeit.default_timer()
        print(query, suggest_service.process(query))
        end_time = timeit.default_timer()
        time_list.append((end_time - start_time))
    # print(max(time_list), min(time_list), np.mean(time_list))
    # historical_query['time'] = time_list
    # historical_query.to_excel('during_search_performance.xlsx', index=False)
    # dump_json(query2kw, QUERY_KW_TEST)


if __name__ == "__main__":
    suggest_test()
