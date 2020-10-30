import json
import numpy as np
from datetime import date, datetime

from sklearn import preprocessing

from lib.datastructure.files import JIEBA_DICT


def jieba_wrap(j):
    """
    Load user dict for jieba
    """
    j.load_userdict(JIEBA_DICT)
    return j


def encode(lst, same_len=False):
    """
    Encode list elements into ids
    :param same_len: whether the id of elements have same size
    :param lst: list to encode
    :return: {element: id}
    """
    lst = set(lst)
    if not same_len:
        return {x[1]: x[0] for x in enumerate(lst)}
    else:
        pad_num = int('1' + '0' * len(str(max(lst))))
        return {x[1]: pad_num + x[0] for x in enumerate(lst)}


def dump_json(obj, fn, encoding='utf-8'):
    with open(fn, 'w', encoding=encoding) as fout:
        json.dump(obj, fout, ensure_ascii=False, indent=4)


def load_json(fn, encoding='utf-8'):
    with open(fn, 'r', encoding=encoding) as fin:
        return json.load(fin)


def cosine(v1, v2):
    v1, v2 = np.array(v1), np.array(v2)
    return np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2))


def load_list(fn, encoding='utf-8'):
    ret = []
    with open(fn, encoding=encoding) as fin:
        for line in fin:
            ret.append(line.strip())
    return ret


def save_list(lst, fn, encoding='utf-8'):
    with open(fn, 'w', encoding=encoding) as fout:
        for line in lst:
            fout.write(line + '\n')


def datetime2date(dt):
    return date(dt.year, dt.month, dt.day)


def today():
    td = datetime.now()
    return date(td.year, td.month, td.day)


def label_encoding(df, col_list):
    lbl = preprocessing.LabelEncoder()
    for col in col_list:
        df[col] = lbl.fit_transform(df[col].astype(str))


if __name__ == '__main__':
    x = encode([i for i in range(1001)], same_len=True)
    print(x)
