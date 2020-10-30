"""
String url = "http://10.157.32.75/v1/inventory/front/getAvailableInventoryQuantityList";
   Map<String, String> headers = new HashMap<>();
   headers.put("Content-Type", "application/json");
   JSONArray arry  = new JSONArray();
   arry.add("201893");
   arry.add("127304");
   arry.add("339224");
   JSONObject obj = new JSONObject();
   obj.put("skuCodeList", arry);
   String params =obj.toJSONString();
   String testHtml = cn.hutool.http.HttpRequest.post(url).body(params, "application/json").execute().body();
   logger.info("---------->" + testHtml);
   JSONObject jsonObject = JSON.parseObject(testHtml).getJSONObject("result");
   return jsonObject.toJSONString();
"""
from typing import List

import requests
import json


def get_available_inventory(sku_list: List[str]):
    URL = "http://10.157.32.75/v1/inventory/front/getAvailableInventoryQuantityList"
    headers = {
        'Content-Type': 'application/json',
    }
    data = {
        "skuCodeList": sku_list
    }
    response = requests.post(URL, data=json.dumps(data), headers=headers)
    res = response.json()['results']
    stock_list = [x['quantity'] for x in res]
    return stock_list



