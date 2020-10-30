
from lib.model.recall import Recall
from lib.datastructure.constants import PROD_TAG_COLS

ORIG_KW_TAG = "./data/0918/default_kw.xlsx"
KW_TAG = "./data/kw_tag.json"
PROD_TAG = "./data/0918/product_list.csv"
ITEM_EMBEDDING_SIM = "./data/embedding/item_sim.json"
QUERY_EMBEDDING = "./data/query_w2v_full_new_cut_upper"


def transfer_keyword_tag():
    import pandas as pd
    from lib.utils.utils import dump_json
    orig_kw_tag = pd.read_excel(ORIG_KW_TAG)
    ret = {}
    for index, row in orig_kw_tag.iterrows():
        ret.setdefault(row['Keywords Proposed'], []).append(row['original tag'])
    dump_json(ret, KW_TAG)


def main():
    rc = Recall()
    rc.load_kw_rel_tags(KW_TAG)
    rc.load_prod_tag(PROD_TAG, 'product_id', PROD_TAG_COLS)
    # rc.load_item_embedding(ITEM_EMBEDDING_SIM)
    rc.load_query_embedding(QUERY_EMBEDDING)
    # x = rc.kw_tag_prod("雅诗兰黛")
    # x = rc.word_tag_prod("雅诗兰黛")


if __name__ == '__main__':
    # transfer_keyword_tag()
    main()


