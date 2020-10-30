from collections import Counter, defaultdict
from random import choice

import jieba
import pandas as pd
from tqdm import tqdm

from lib.datastructure.constants import DEFAULT_POP_KEYWORDS, FIXED_TAG2KW, USER_ITEM_SCORE
from lib.datastructure.files import USER2KW, USER2TAG, USER2ITEM, KW2ITEM, KW2ITEM_NEW
from lib.model.skip_gram import SkipGram
from lib.utils.utils import jieba_wrap, dump_json, load_json

jieba = jieba_wrap(jieba)


def gen_user2kw(user2tag, tag2kw, top_k: int = 12):
    """generate default keywords and hot keywords for user
    the first 3 is default keywords, the last 9 is hot keywords
    in hot keywords, the first 3 is fixed hot keywords
    """
    normal_user2kw = _gen_user2normal_kw(user2tag, tag2kw, top_k-3)
    fixed_user2kw = _gen_user2fixed_kw(user2tag)
    user2kw = {}
    for user in set(normal_user2kw.keys()).intersection(fixed_user2kw.keys()):
        normal_kws = normal_user2kw[user]
        fixed_kws = fixed_user2kw[user]
        user2kw[user] = [normal_kws[:1]] + fixed_kws + normal_kws[1:]
    dump_json(user2kw, USER2KW)


def _gen_user2normal_kw(user2tag, tag2kw, top_k: int = 9):
    user2kw = {}
    for user, tags in user2tag.items():
        if tags:
            user_kws = []
            for tag in tags:
                user_kws.extend(tag2kw.get(tag, []))
            user2kw[user] = user_kws
    user2kw = {user: Counter(kw).most_common(top_k) for user, kw in user2kw.items()}
    user2kw = {user: [kw[0] for kw in kws] for user, kws in user2kw.items()}
    return user2kw


def _gen_user2fixed_kw(user2tag):
    """
    generate 3 fixed hot keywords, the first 2 is keep fixed for everyone.
    if ang tag of the user is in fixed tags, choose the corresponding keyword as the last one;
    otherwise choose one keyword from else hot keywords randomly
    """
    user2kw = {user: list(DEFAULT_POP_KEYWORDS) for user in user2tag.keys()}
    for user, tags in user2tag.items():
        tag_flag = False  # whether tags of user in fixed tags
        for tag in tags:
            if tag in FIXED_TAG2KW.keys():
                tag_flag = True
                user2kw[user].append(FIXED_TAG2KW[tag])
                break
        if not tag_flag:
            user2kw[user].append(choice(list(FIXED_TAG2KW.values())))
    return user2kw


def get_user2kw():
    return load_json(USER2KW)


def gen_user2tag(user2item, item2item, item2tag, top_k=10):
    """generate user2tag with user2item, item2item and item2tag
    param user_item: {user_id: [items]}
    param item_sim: {item: [sim_items]}
    param item_tag: {item: [tags]}
    return user2tag: {user: [tags]}
    """
    ret = {}
    for user, items in user2item.items():
        user_tags = []
        for item in items:
            for rel_item in [item] + item2item.get(item, []):
                user_tags.extend(item2tag.get(rel_item, []))
        ret[user] = user_tags
    ret = {u: Counter(t) for u, t in ret.items()}
    ret = {u: sorted(t.keys(), key=lambda x: t[x], reverse=True)[:top_k] for u, t in ret.items()}
    dump_json(ret, USER2TAG)


def get_user2tag():
    return load_json(USER2TAG)


def gen_user2item(behavior_df: pd.DataFrame):
    """generate user-item behavior score,
    the score of all kinds of behavior refer to USER_ITEM_SCORE
    the score of behaviors not in USER_ITEM_SCORE is set to zero
    :param behavior_df: pd.DataFrame, contains ['user_id', 'op_code', 'behavior']
    return user2item, dict, {user: [items]}
    """
    behavior_df = behavior_df.dropna().query('op_code != 0')
    users = behavior_df['user_id'].drop_duplicates()

    user2item = {user: {} for user in users}
    for index, row in behavior_df.iterrows():
        user = row['user_id']
        item = row['op_code']
        beh_score = USER_ITEM_SCORE.get(row['behavior'], 0)
        user2item[user][item] = user2item[user].get(item, 0) + beh_score
    user2item = {u: sorted(i.keys(), key=lambda x: i[x], reverse=True)
                 for u, i in user2item.items()}
    dump_json(user2item, USER2ITEM)


def get_user2item():
    return load_json(USER2ITEM)


def gen_kw2item(kw2tag, tag2item):
    kw2item = {}
    for kw, tags in kw2tag.items():
        items = []
        for tag in tags:
            items.extend(tag2item.get(tag, []))
        kw2item[kw] = items
    res = {k: v for k, v in kw2item.items() if v}
    dump_json(res, KW2ITEM)


def get_kw2item():
    # TODO
    # return load_json(KW2ITEM)
    return load_json(KW2ITEM_NEW)


class KeywordTag:
    def __init__(self, w2v_model, keywords, item_tags, tag_mapping):
        self.w2v_model = w2v_model
        self.kws = {w: jieba.lcut(w) for w in keywords}  # {keyword: [words}
        item_names = item_tags.keys()  # items
        self.item_name_map = self.gen_item_cut_map(item_names)  # {word: [items]}
        self.item_tags = item_tags  # {item: [tags]}
        self.tag_mapping = tag_mapping

    @staticmethod
    def gen_item_cut_map(item_list):
        """gen inverted index from word to items
        param item_list, list
        return: word2item, dict, {word: [items], ...}
        """
        word2item = defaultdict(list)
        for item in item_list:
            for word in jieba.cut(item):
                word2item[word].append(item)
        return word2item

    def gen_kw_rel_tag(self, top_k=5, idf=None):
        """generate keyword2tag, used for recommend relative tags with keywords during search
        keyword --split--> kw2words --word2vec--> kw2words --word2item--> kw2items
        --item2tag--> kw2tags --tag2tag--> kw2tags
        """
        sg = SkipGram(self.w2v_model)
        kw2item = defaultdict(list)
        for kw, cut_kw in tqdm(list(self.kws.items())):
            for w in cut_kw:
                if w not in self.w2v_model.wv:
                    continue
                cut_kw_rel_words = sg.top_rel_words(w, 10)

                for cut_name, item_name in self.item_name_map.items():
                    if cut_name in cut_kw_rel_words:
                        kw2item[kw].extend(item_name)

        kw2tag = defaultdict(list)
        for kw, items in kw2item.items():
            for item in items:
                kw2tag[kw].extend(self.item_tags[item])
        kw2tag = {kw: [self.tag_mapping[x] for x in tags if self.tag_mapping.get(x)]
                  for kw, tags in kw2tag.items()}

        if idf is None:
            kw2tag = {kw: [x[0] for x in Counter(tags).most_common()[:top_k]]
                      for kw, tags in kw2tag.items()}
        else:
            kw2tagscore = defaultdict(dict)
            for kw, tags in kw2tag.items():
                tag_scores = defaultdict(int)
                for tag in tags:
                    tag_scores[tag] += idf[tag]
                kw2tagscore[kw] = tag_scores
            kw2tag = {kw: [x[0] for x in sorted(tag2score.items(), key=lambda z: z[1], reverse=True)][:top_k]
                      for kw, tag2score in kw2tagscore.items()}
        return kw2tag


def gen_tag_kws(kw2tag):
    """generate relative items for tag
    :param kw2tag: dict, {kw: [tags]}
    return: tag2kw: dict, {tag: [kws]}
    """
    tag2kw = defaultdict(list)
    for kw, tags in kw2tag.items():
        for tag in tags:
            tag2kw[tag].append(kw)
    return tag2kw


if __name__ == "__main__":
    pass