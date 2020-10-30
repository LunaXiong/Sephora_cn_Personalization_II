from gensim.models.word2vec import Word2Vec, Vocab
import numpy as np
from collections import Counter


class SkipGram:
    def __init__(self, model: Word2Vec):
        self.model = model
        self.words, self.owcs, self.owls, self.counts = self.build_matrix()

    def build_matrix(self):
        words, counts = [], []
        for i, j in self.model.wv.vocab.items():
            words.append(i)
            counts.append(j.count)
        o_word_codes, o_word_ls = [], []
        for w in words:
            o_word = self.model.wv.vocab[w]
            o_word_codes.append(o_word.code.astype(np.int8))
            o_word_ls.append(self.model.trainables.syn1[o_word.point].T)
        # padding for matrix calculation
        pad_len = max([x.shape[0] for x in o_word_codes])
        o_word_codes = np.array([np.pad(x, (0, pad_len-x.shape[0]), 'constant') for x in o_word_codes])
        o_word_ls = np.array([np.pad(x, ((0, 0), (0, pad_len-x.shape[1])), 'constant') for x in o_word_ls])
        return words, o_word_codes, o_word_ls, counts

    def pred_prob(self, i_vec):
        dot = np.dot(i_vec, self.owls)
        dot_prod = self.owcs*dot
        log_prob = np.logaddexp(0, -dot)
        ret = -np.sum((log_prob + dot_prod), axis=1) - 0.9 * np.log(self.counts)
        return ret

    def relative_words(self, word):
        try:
            i_vec = self.model[word]
        except KeyError as e:
            raise KeyError("Word %s not in vocabulary" % word)

        scores = self.pred_prob(i_vec)
        r = {w: s for w, s in zip(self.words, scores)}
        return Counter(r).most_common()

    def top_rel_words(self, word, top_k=10):
        rel_words = self.relative_words(word)[:top_k]
        return [x[0] for x in rel_words]

    def batch_relative_words(self, words):
        pass


if __name__ == '__main__':
    pass

