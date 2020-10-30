from functools import reduce
from collections import Counter
import numpy as np


def tf_idf(corpus):
    doc_tf = [dict(Counter(x)) for x in corpus]
    words = list(set(reduce(lambda x, y: x + y, corpus)))
    doc_freq = {}
    for w in words:
        for doc in doc_tf:
            if w in doc:
                doc_freq[w] = doc_freq.get(w, 0) + 1
    doc_num = len(corpus)
    idf = {w: np.log(doc_num / freq) for w, freq in doc_freq.items()}
    tf_idf_val = [{w: tf[w] * idf[w] for w in tf.keys()} for tf in doc_tf]
    return tf_idf_val


if __name__ == '__main__':
    sample_corpus = [['a', 'b', 'c'], ['a', 'g', 'c'], ['d', 'e', 'f']]
    sample_ret = tf_idf(sample_corpus)
    for x in sample_ret:
        print(x)
