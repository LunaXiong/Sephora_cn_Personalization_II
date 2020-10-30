from lib.datastructure.constants import TOP_SEARCH_CONTENT_LIST


def process_tag(tag: str):
    if tag:
        return tag.strip().replace(' ', '').upper()
    else:
        return ''


def del_dupe(items):
    """drop duplicate elements with order"""
    seen = set()
    res = []
    for item in items:
        if item not in seen:
            res.append(item)
            seen.add(item)
    return res


def test_del_dupe():
    item_1 = ['a', 'b', 'c', 'd']
    item_2 = ['a', 'd', 'c', 'e']
    item_1.extend(item_2)
    res = del_dupe(item_1)
    assert res == ['a', 'b', 'c', 'd', 'e']


def get_related_content(keyword):
    res = []
    keyword = keyword.strip().replace(' ', '').upper()
    for key in TOP_SEARCH_CONTENT_LIST.keys():
        if key in keyword:
            res.append(TOP_SEARCH_CONTENT_LIST[key])
    return res


if __name__ == "__main__":
    test_del_dupe()
