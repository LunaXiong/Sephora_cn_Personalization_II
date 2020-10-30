"""
Recall items according to the user and query
given a query,
1) if query in historical keywords set, using keyword2tag
and tag2item to get relative items;
2) else, split query to words, using query embedding to get relative words,
using cut_tag2item to get relative items;
finally, using similar items from item embedding to get more items.
"""
from collections import Counter

import jieba

from airflow.item_embedding_model import get_item2item
from airflow.query_embedding_model import get_query2query
from lib.model.linking import get_kw2item
from lib.processing.item_tag_mapping import get_extended_tag2item
from lib.utils.utils import jieba_wrap, load_json

jieba = jieba_wrap(jieba)


class Recall:
    def __init__(self):
        self.tag2item = get_extended_tag2item()  # dict[str: set], {cut_tag: {items}}
        self.item2item = get_item2item()  # dict[str: list], {item: [items]}
        self.query2query = get_query2query()  # word2word, cut_tag
        self.kw2item = get_kw2item()  # keyword2item
        self.keyword_set = set(self.kw2item.keys())  # set, {keywords}

    def recall(self, query):
        """recall item according to query
        :returns seed_items:
        :returns rela_items:
        """
        if query in self.keyword_set:
            seed_items = self._kw_tag_prod(query)
        else:
            seed_items = self.word_tag_prod(query)
        rela_items = self._get_relative_items(seed_items)
        return seed_items, rela_items

    def _kw_tag_prod(self, query):
        """query/keyword --kw2tag--> tags --tag2item--> items
        :param query: str
        return rel_items: List[str], relative items"""
        return self.kw2item[query]

    def word_tag_prod(self, query):
        """query -> [w] -> [rel_w in cut_tags] -> [rel_items]"""
        rel_items = []
        for word in jieba.cut(query):
            sim_words = [word]
            if word in self.query2query.keys():
                sim_words.extend(self.query2query[word])
            for sim_word in sim_words:
                rel_items.extend(self.tag2item.get(sim_word, []))
        return [x[0] for x in Counter(rel_items).most_common()]

    def _get_relative_items(self, items):
        rela_items = []
        for item in items:
            rela_items.extend(self.item2item.get(str(item), []))
        rela_items = [item for item in rela_items]
        return list(set(rela_items))
