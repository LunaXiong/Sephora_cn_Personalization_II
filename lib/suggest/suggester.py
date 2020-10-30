"""
用户输入一个query word，将query word分别添加到trie和拼音trie中，
分别得到prefix前缀词和accurate准确词，合并去重
如果数量少于10，取query word的SkipGram
"""
import pandas as pd
from lib.model.trie import Trie, PinyinTRIE
from lib.model.skip_gram import SkipGram


class Suggester:
    def __init__(self, kw_fn, kw_col, item_embedding_model):
        self.kws = self.load_kws(kw_fn, kw_col)
        self.trie = self.build_trie()
        self.pinyin_trie = self.build_py_trie()
        self.sg = SkipGram(item_embedding_model)

    @staticmethod
    def load_kws(kw_fn, kw_col):
        kws = pd.read_csv(kw_fn)[kw_col].astype('str').to_list()
        return list(set(kws))

    def build_trie(self):
        t = Trie()
        t.add_words(self.kws, pruning=False)
        return t

    def build_py_trie(self):
        pyt = PinyinTRIE()
        pyt.add_words(self.kws, pruning=False)
        return pyt

    def suggest(self, word):
        trie_suggest = self.trie.query(word)
        trie_suggest = trie_suggest[0] + trie_suggest[1]
        py_trie_suggest = self.pinyin_trie.query(word)
        py_trie_suggest = py_trie_suggest[0] + py_trie_suggest[1]

        trie_suggest = list(set(trie_suggest + py_trie_suggest))
        if len(trie_suggest) < 10:
            sg_suggest = self.sg.relative_words(word)[:10]
            trie_suggest += [word+x[0] for x in sg_suggest]
        return trie_suggest
