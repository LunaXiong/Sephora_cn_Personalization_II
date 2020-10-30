"""
Evaluation code
"""
from tqdm import tqdm

from lib.model.skip_gram import SkipGram
from lib.utils.utils import cosine
import random
import numpy as np

random.seed(1)
np.random.seed(1)


def eval_item_embedding(model, pairs):
    eval_ret = []
    for i1, i2, label in pairs:
        try:
            v1 = model.wv[i1]
            v2 = model.wv[i2]
        except:
            continue
        pred = cosine(v1, v2)
        eval_ret.append((i1, i2, label, pred))
    return eval_ret


def item_embedding_sequence_eval(item_embedding_model, eval_seqs):
    sg = SkipGram(item_embedding_model)
    eval_ret = []
    for seq in tqdm(eval_seqs):
        eval_item = seq.pop(np.random.choice(list(range(len(seq)))))
        rel_items = []
        for item in seq:
            rel_items += [x[0] for x in sg.relative_words(item)[:3]]
        if eval_item in rel_items:
            eval_ret.append(1)
        else:
            eval_ret.append(0)
    return eval_ret
