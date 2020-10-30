from pypinyin import lazy_pinyin


def to_pinyin(lst):
    return [lazy_pinyin(x) for x in lst]

