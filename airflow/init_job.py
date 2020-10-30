"""combine some jobs, which contain:
gen_item2tag_job
gen_tag2kw_job
gen_kw2item_job
gen_brand_mapping

"""
from datetime import datetime

from lib.datastructure.config import HIVE_CONFIG
from lib.datastructure.files import STANDARD_TAG_KEYWORD_FILE, EXTENDED_TAG_KEYWORD_FILE, ASSOCIATED_KEYWORD_FILE
from lib.db.hive_utils import HiveUnit
from lib.model.linking import gen_kw2item
from lib.processing.during_search import gen_brand_mapping, gen_brand_correcting, gen_kw2pop_score, \
     gen_dif_priority, gen_dif_score
from lib.processing.item_pop import run_kw_item_pop, gen_top_op
from lib.processing.item_tag_mapping import *
from lib.processing.keyword_tag_mapping import *


# 1. generate item-tag mapping
from script_eval_trie import gen_associated_kw

print(datetime.now(), 'generate item-tag mapping start...')
hive_unit = HiveUnit(**HIVE_CONFIG)
product_profile_basic = hive_unit.get_df_from_db("select * from da_dev.search_prod_tag_mapping_v2")
product_profile_extended = hive_unit.get_df_from_db("select * from da_dev.search_prod_tag_mapping")
product_profile_new = hive_unit.get_df_from_db("select distinct product_id, product_name_cn from oms.dim_sku_profile")
hive_unit.release()
# product-tag mapping, DO contain combined tags, used for during search
gen_extended_tag2item(product_profile_extended)
gen_extended_item2tag()
# product-tag mapping, DO NOT contain combined tags, used for before search
gen_basic_tag2item(product_profile_basic)
gen_basic_item2tag()
# product-tag mapping with NEW product
gen_new_tag2item(product_profile_new)
gen_new_item2tag()
print(datetime.now(), 'generate item-tag mapping done')

# 2. generate keyword-tag mapping
print(datetime.now(), 'generate keyword-tag mapping start...')
# standard tag-keyword mapping, used for keyword suggest in before-search
gen_standard_tag2kw(STANDARD_TAG_KEYWORD_FILE)
gen_standard_kw2tag(STANDARD_TAG_KEYWORD_FILE)
# extended tag-keyword mapping, used for keyword suggest in during search
gen_extended_tag2kw(EXTENDED_TAG_KEYWORD_FILE)
gen_extended_kw2tag(EXTENDED_TAG_KEYWORD_FILE)
print(datetime.now(), 'generate keyword-tag mapping done')


# 3. generate keyword-item mapping, used for recall
print(datetime.now(), 'generate keyword-tag mapping start...')
kw2tag = get_extended_kw2tag()
extended_tag2item = get_extended_tag2item()
new_tag2item = get_new_tag2item()
tag2item = dict(extended_tag2item, **new_tag2item)
gen_kw2item(kw2tag, tag2item)
print(datetime.now(), 'generate keyword-tag mapping done')


# 4. brand correcting and brand mapping
print(datetime.now(), 'generate brand mapping start...')
gen_brand_correcting(ASSOCIATED_KEYWORD_FILE)
gen_brand_mapping(ASSOCIATED_KEYWORD_FILE)
print(datetime.now(), 'generate brand mapping done')
hive_unit = HiveUnit(**HIVE_CONFIG)
# gen_kw2pop_score('click', hive_unit, 90)
gen_associated_kw()
gen_fixed_kw2tag()
run_kw_item_pop(pop_type='click', hive_unit=hive_unit, n_day=90)
gen_top_op(topn=4)
gen_dif_score()
gen_dif_priority()
hive_unit.release()
