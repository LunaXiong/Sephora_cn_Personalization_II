"""
before search
generate keywords list for each user,
used for default keyword recommend and hot keyword recommend.
logic: user --user2item--> items --items2items--> items --item2tag--> tags --tag2kw--> kws
"""
from datetime import datetime

from airflow.item_embedding_model import get_item2item
from lib.model.linking import gen_user2tag, get_user2item, gen_user2kw, get_user2tag
from lib.processing.item_tag_mapping import get_basic_item2tag
from lib.processing.keyword_tag_mapping import get_standard_tag2kw

print(datetime.now(), 'generate user2keyword start...')
user2item = get_user2item()
item2item = get_item2item()
item2tag = get_basic_item2tag()
gen_user2tag(user2item, item2item, item2tag, top_k=20)
user2tag = get_user2tag()
tag2kw = get_standard_tag2kw()
gen_user2kw(user2tag, tag2kw)
print(datetime.now(), 'generate user2keyword done')

