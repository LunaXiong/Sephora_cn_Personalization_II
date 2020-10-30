from lib.utils.gen_pinyin import to_pinyin


class Trie:
    def __init__(self):
        # 所有词
        self.words = []
        # 储存节点对应的词
        self.tree_prefix = [[]]
        # 储存字和节点的跳转关系
        self.tree_next = [{}]
        # 储存词语结尾
        self.tree_word = [-1]

    @staticmethod
    def _upper(w: str or list):
        if type(w) == str:
            return w.upper()
        else:
            return [x.upper() for x in w]

    def add_words(self, words, pruning=True):
        index = 0
        max_size = 8000000
        for word in words:
            word = self._upper(word)
            index += 1
            if index > max_size:
                break
            # 当前word在words中的index
            _id = len(self.words)
            self.words.append(word)

            node_index = 0
            node_chars = []
            for c in word:
                if c not in self.tree_next[node_index]:
                    self.tree_next[node_index][c] = len(self.tree_word)
                    self.tree_word.append(-1)
                    self.tree_prefix.append([])
                    self.tree_next.append({})
                node_chars.append(node_index)
                node_index = self.tree_next[node_index][c]
            self.tree_word[node_index] = _id
            for c in node_chars:
                if pruning and len(self.tree_prefix[c]) > 5:
                    continue
                self.tree_prefix[c].append(_id)

    def query_prefix(self, w):
        node_index = 0
        for c in w:
            if node_index < 0:
                break
            node_index = self.tree_next[node_index].get(c, -1)

        if node_index < 0:
            return []
        obj = self.tree_prefix[node_index]
        obj = [self.words[i] for i in obj]
        return obj

    def query_correct(self, w, bad_limit=1, ret_limit=10):
        qh = 0
        q = [(0, 0, 0)]
        obj = []
        while qh < len(q):
            node_index, char_index, bad_cnt = q[qh]
            qh += 1

            if len(obj) > ret_limit:
                break
            if char_index >= len(w):
                if self.tree_word[node_index] >= 0:
                    obj.append(self.tree_word[node_index])
                continue
            c = w[char_index]
            _next = self.tree_next[node_index].get(c, -1)
            if _next >= 0:
                q.append((_next, char_index+1, bad_cnt))
            if bad_cnt < bad_limit:
                for ch, nx in self.tree_next[node_index].items():
                    if c == ch:
                        continue
                    q.append((nx, char_index+1, bad_cnt+1))
        obj = [self.words[i] for i in obj]
        return obj

    def query(self, w):
        w = self._upper(w)
        pre = self.query_prefix(w)
        corr = self.query_correct(w) if len(w) > 1 else []
        return pre, corr


class PinyinTRIE(Trie):

    def __init__(self):
        super(PinyinTRIE, self).__init__()
        self.pinyin2word = {}

    def add_words(self, words, pruning=True):
        pinyin_words = to_pinyin(words)
        for py_lst, word in zip(pinyin_words, words):
            self.pinyin2word[''.join(py_lst).upper()] = word.upper()
        super(PinyinTRIE, self).add_words(pinyin_words, pruning)

    def query(self, w):
        w = self._upper(w)
        w = to_pinyin([w])[0]
        pre, corr = super(PinyinTRIE, self).query(w)
        pre = [self.pinyin2word[''.join(x)] for x in pre]
        corr = [self.pinyin2word[''.join(x)] for x in corr]
        return pre, corr


class RTrie(Trie):
    """
    Suffix Tree
    """
    def add_words(self, words, pruning=True):
        reversed_words = [x[::-1].upper() for x in words]
        super(RTrie, self).add_words(reversed_words, pruning)

    def query(self, w):
        w = self._upper(w)[::-1]
        suf, corr = super(RTrie, self).query(w)
        suf = [x[::-1] for x in suf]
        corr = [x[::-1] for x in corr]
        return suf, corr


if __name__ == '__main__':
    word_lst = ["复旦大学", "复旦医学院", "上海交通大学"]
    # word_lst += ['复旦大学%d' % i for i in range(10)]

    word_lst += ['fudan大学']

    # ptrie = PinyinTRIE()
    # ptrie.add_words(word_lst, pruning=False)
    #
    trie = Trie()
    trie.add_words(word_lst, False)

    x = trie.query('复蛋大学')
    print(x)
    x = trie.query('复旦')
    print(x)
    r_trie = RTrie()
    r_trie.add_words(word_lst)
    # x = r_trie.query('大学')
    # print(x)

