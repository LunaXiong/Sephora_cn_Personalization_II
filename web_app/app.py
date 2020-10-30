import re

import pandas as pd
from flask import Flask, Response, jsonify

from airflow.query_embedding_model import load_query_embedding_model
from lib.datastructure.files import DEFAULT_SKU, EXTENDED_TAG_KEYWORD_FILE, ASSOCIATED_KEYWORD_FILE
from lib.datastructure.files import LGB_RANKING_FN, LAST_CLICK_FEATURE_FN
from lib.model.ranking import RegressionRanking
from lib.model.recall import Recall
from lib.model.trie import Trie, PinyinTRIE, RTrie
from lib.processing.during_search import KeywordRanking
from lib.processing.for_ranking import LastClick, QueryProcessor
from lib.processing.gen_feature import get_ranking_user_feature, get_ranking_item_feature
from lib.processing.get_available_inventory import get_available_inventory
from lib.utils.nlp_ops import get_related_content
from lib.utils.utils import load_json
from web_app.service_ranking import RankingService
from web_app.service_recall import RecallService
from web_app.service_suggest import SuggestService


class MyResponse(Response):
    @classmethod
    def force_type(cls, response, environ=None):
        if isinstance(response, (list, dict)):
            response = jsonify(response)
        return super(Response, cls).force_type(response, environ)


class MyFlask(Flask):
    response_class = MyResponse


app = MyFlask(__name__)

# during search
trie_model = Trie()
pinyin_trie = PinyinTRIE()
rtrie_model = RTrie()
word_list = pd.read_excel(ASSOCIATED_KEYWORD_FILE)[['Associated Keyword']]  # keyword set
word_list = word_list.astype('str')['Associated Keyword']
trie_model.add_words(word_list, pruning=False)
pinyin_trie.add_words(word_list, pruning=False)
rtrie_model.add_words(word_list, pruning=False)
query_embedding_model = load_query_embedding_model()
keyword_ranking = KeywordRanking()
suggest_service = SuggestService(trie_model, pinyin_trie, rtrie_model, keyword_ranking)

# after search
recall_model = Recall()
recall_service = RecallService(recall_model)
ranking_model = RegressionRanking.load(LGB_RANKING_FN, load_tl=True)
last_click = LastClick.load(LAST_CLICK_FEATURE_FN)
user_feature = get_ranking_user_feature()
item_feature = get_ranking_item_feature()
query_processor = QueryProcessor(query_embedding_model)
ranking_service = RankingService(last_click, user_feature, item_feature, ranking_model, query_processor)
opcode2sku = load_json(DEFAULT_SKU)


@app.route("/", methods=["GET"])
def heartbeat():
    return "Hello World!"


@app.route('/v1/bds/recos/suggest/<string:openid>/<string:keyword>', methods=['GET'])
def suggest(openid, keyword):
    keyword = keyword.replace(" ", "").upper()
    keyword = re.sub(u"([^\u4e00-\u9fa5\u0030-\u0039\u0041-\u005a\u0061-\u007a])", "", keyword)
    if not keyword:
        tag_list = []
        content_list = []
    else:
        tag_list = suggest_service.process(keyword)
        content_list = get_related_content(keyword)
    res = {"status": "ok",
           "results": {
               "taglist": tag_list,
               "contentlist": content_list
           }
           }
    return res


@app.route('/v1/bds/recos/search/<string:openid>/<string:keyword>', methods=['GET'])
def search(openid, keyword):
    if openid not in set(user_feature['open_id'].values):
        return {"status": "ok", "results": [], "swap": 0, "info": "no valid open_id"}
    keyword = keyword.replace(' ', '').upper()
    seed_items, rela_items = recall_service.process(openid, keyword)
    item_res, flag = ranking_service.process(seed_items, rela_items)
    filter_items = []
    sku_res = []
    for item in item_res:
        if str(item) in opcode2sku.keys():
            filter_items.append(item)
            sku_res.append(opcode2sku[str(item)])
    stock_res = get_available_inventory(sku_res)
    # stock_res = [0 for _ in range(len(sku_res))]
    results = [{"opcode": str(opcode), "skuid": str(skuid), "stock": str(stock)}
               for opcode, skuid, stock in zip(filter_items, sku_res, stock_res)]
    res = {"status": "ok", "results": results, "swap": flag}
    return res


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8888)
