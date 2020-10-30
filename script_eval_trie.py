from datetime import datetime
import time
from tqdm import tqdm

import pandas as pd
import numpy as np
import jieba
from gensim.models import Word2Vec

from airflow.query_embedding_model import load_query_embedding_model
from lib.datastructure.config import HIVE_CONFIG
from lib.datastructure.files import HISTORY_QUERY, QUERY_KW_TEST, SELECT_SKU, ASSOCIATED_KEYWORD_FILE, BRAND_KW_FILE, \
    ITEM_TAG_FILE, BRAND_LIST, PRODUCT_LIST
from lib.db.hive_utils import HiveUnit
from lib.model.skip_gram import SkipGram
from lib.model.trie import PinyinTRIE, Trie, RTrie
from lib.processing.during_search import KeywordRanking, gen_kw2pop_score_old, gen_kw_priority
from lib.utils.utils import dump_json, load_json
from web_app.service_suggest import SuggestService


def gen_stmap(mapping: pd.DataFrame):
    st_dic = {}
    for item in mapping.iterrows():
        for kw in item[1]['kw'].split(','):
            st_dic[kw] = item[1]['stkw']
    return st_dic


def gen_stkw(kw: str, smkw: dict):
    if kw in smkw:
        return smkw[kw]
    else:
        return kw


def add_cut_list(fl: list):
    for fn in fl:
        jieba.load_userdict(fn)


def trie_unit(query: str):
    pass


def trie_test(top_k: int):
    data_path = "D:/search_test/"
    gen_kw2pop_score_old()
    gen_kw_priority()
    ctrie = Trie()
    ptrie = PinyinTRIE()
    rtrie = RTrie()
    word_list = pd.read_excel(ASSOCIATED_KEYWORD_FILE)['Associated Keyword'].astype(
        'str')  # keyword set
    word_list = list(set(word_list))
    ctrie.add_words(word_list, pruning=False)
    ptrie.add_words(word_list, pruning=False)
    rtrie.add_words(word_list, pruning=False)

    jieba.load_userdict("%ssplit_list0917.txt" % data_path)  # cut list
    historical_query = pd.read_csv("%shis_query.csv" % data_path)
    trie_ret_c = {}

    brand_corr = pd.read_excel(BRAND_LIST, sheet_name='stkw_brand')
    brand_corr = gen_stmap(brand_corr)
    brand_map = pd.read_excel(BRAND_LIST, sheet_name='stkw_brand_mapping')
    brand_map = gen_stmap(brand_map)
    for k in brand_corr.keys():
        jieba.add_word(k)
    for k in brand_map.keys():
        jieba.add_word(k)

    # brand_kw_df = pd.read_excel("D:/search_test/brand_kw.xlsx")
    # kw_pop = pd.read_csv("D:/search_test/kw_pop.csv").rename(columns={'0_x': 'no. of product', '0_y': 'no. of click'})
    # kw_pop = kw_pop[kw_pop['no. of product'] > 0]
    # brand_priority = pd.read_excel("D:/search_test/brand_priority.xlsx")

    suggest_rank = KeywordRanking()
    time_cost = []

    for kw in tqdm(historical_query['query'].astype('str')):
        t1 = time.clock()
        time_cost.append(t1)
        stw = gen_stkw(jieba.lcut(kw.lstrip())[0], brand_corr)
        stw_v1 = gen_stkw(stw, brand_map)
        if stw_v1 != stw:
            trie_ret_c[kw] = [stw_v1]
        else:
            trie_ret_c[kw] = []
            trie_prefix, trie_corr = ctrie.query(str(stw))
            trie_p_prefix, trie_p_corr = ptrie.query(stw)
            trie_r_prefix, trie_r_corr = rtrie.query(stw)
            trie_ret_c[kw].extend(trie_prefix)
            if len(trie_ret_c[kw]) < top_k:
                trie_ret_c[kw].extend(trie_r_prefix)
                if len(trie_ret_c[kw]) < top_k:
                    trie_ret_c[kw].extend(trie_p_prefix)
                    if len(trie_ret_c[kw]) < top_k:
                        trie_ret_c[kw].extend(trie_corr)
                        if len(trie_ret_c[kw]) < top_k:
                            trie_ret_c[kw].extend(trie_r_corr)
                            if len(trie_ret_c[kw]) < top_k:
                                trie_ret_c[kw].extend(trie_p_corr)

            if len(trie_ret_c[kw]) > 1:
                trie_ret_c[kw] = suggest_rank.ranking(trie_ret_c[kw])
                trie_ret_c[kw] = [stw] + trie_ret_c[kw][:top_k]

    trie_ret_c = pd.DataFrame.from_dict(trie_ret_c, orient='index').reset_index().rename(columns={'index': 'query'})
    trie_ret_c = pd.merge(trie_ret_c, historical_query, on='query', how='left')
    print(trie_ret_c)

    trie_ret_c.to_csv("%seval_trie_c_V.csv" % data_path, index=False, encoding='utf_8_sig')


def sg_test():
    data_path = "D:/search_test/"
    model = Word2Vec.load('%squery_w2v_full_new_cut_upper' % data_path)
    sg = SkipGram(model)

    # hive_unit = HiveUnit(**HIVE_CONFIG)
    # historical_query = hive_unit.get_df_from_db(r"""
    #     select query,sum(search_cnt) as search_cnt
    #     from da_dev.historical_query_90d
    #     group by query
    #     having sum(search_cnt)>100
    #     """)
    # hive_unit.release()
    # historical_query.to_csv("%shis_query_l.csv" % data_path, index=False, encoding='utf_8_sig')

    historical_query = pd.read_csv("%shis_query.csv" % data_path)

    jieba.load_userdict(r"%sprod_kws.txt" % data_path)
    jieba.load_userdict(r"%sprod_kws_v2.dic" % data_path)
    jieba.load_userdict(r"%sprod_kws_v3.txt" % data_path)

    kwmap = pd.read_excel("%sstkw_brand.xlsx" % data_path)
    kwmap = gen_stmap(kwmap)
    for k, v in kwmap.items():
        jieba.add_word(k)
        jieba.add_word(v)

    sg_ret = {}
    t_record = []
    for kw in historical_query['query'].astype('str'):

        t1 = time.clock()
        try:
            sg_suggest = sg.relative_words(gen_stkw(jieba.lcut(kw)[0], kwmap))[:10]
            sg_ret[kw] = [gen_stkw(jieba.lcut(kw)[0], kwmap) + x[0] for x in sg_suggest]
        except:
            sg_ret[kw] = []
        t2 = time.clock()
        t_record.append(t2 - t1)
    print(np.mean(t_record))
    print(np.max(t_record))
    print(np.min(t_record))

    sg_ret = pd.DataFrame.from_dict(sg_ret, orient='index').reset_index().rename(columns={'index': 'query'})
    sg_ret = pd.merge(sg_ret, historical_query, on='query', how='left')
    sg_ret.to_csv("%seval_sg.csv" % data_path, index=False, encoding='utf_8_sig')


def kw_split(kw_lst: list):
    data_path = "D:/search_test/"
    jieba.load_userdict('%ssplit_list0917.txt' % data_path)

    splite_name = {}
    splite_list = []
    for name in kw_lst:
        splite_name[name] = jieba.lcut(name)
        splite_list = splite_list + jieba.lcut(name)
    splite_ret = pd.DataFrame.from_dict(splite_name, orient='index').reset_index().rename(columns={'index': 'query'})
    splite_list = list(set(splite_list))
    splite_df = pd.DataFrame({'splite_query': splite_list})
    return splite_ret, splite_df


def xlsx2txt(fn: str, ret_fn: str):
    kws = []
    with open(fn, encoding='utf_8') as fin:
        for line in fin:
            kws.append(line[:-1])

    with open(ret_fn, 'w', encoding='utf-8') as fout:
        for w in kws:
            fout.write(w + '\n')


def kw_dic(fn_lst: list, ret_fn: str):
    kw_lst = []
    for fn in fn_lst:
        with open(fn, 'r', encoding='utf-8') as file_to_read:
            while True:
                lines = file_to_read.read().splitlines()
                if not lines:
                    break
                    pass
                kw_lst = kw_lst + lines
                pass
    kw_lst = list(set(kw_lst))
    with open(ret_fn, "w", encoding='utf-8') as ret:
        for kw in kw_lst:
            ret.write(kw + '\n')
    print("SUCCESS!")


def xlsx2list():
    data_path = "D:/search_test/"
    kwmap = pd.read_excel("%sstkw_brand.xlsx" % data_path)
    kwmap = gen_stmap(kwmap)
    kw_lst = []
    for k, v in kwmap.items():
        kw_lst.append(v)
    kw_lst = list(set(kw_lst))
    with open(r"%skw_lst_brand.txt" % data_path, "w", encoding='utf-8') as ret:
        for kw in kw_lst:
            ret.write(kw + '\n')


def word_cut():
    data_path = "D:/search_test/"
    xlsx2list()
    xlsx2txt('%sWords for Split 20200916.csv' % data_path, '%skw_0916.txt' % data_path)
    f1 = r"%sprod_kws.txt" % data_path
    f2 = r"%sprod_kws_v2.dic" % data_path
    f3 = r"%sprod_kws_v3.txt" % data_path
    f4 = r"%skw_lst_brand.txt" % data_path
    f5 = r"%skw_0916.txt" % data_path
    fn_list = [f1, f2, f3, f4, f5]
    print(fn_list)
    kw_dic(fn_list, '%ssplit_list0917.txt' % data_path)


def split_kw_os():
    data_path = "D:/search_test/"
    tb_kw = pd.read_csv('%stb_kw_0915.csv' % data_path).dropna()
    jd_kw = pd.read_csv('%sjd_kw_0915.csv' % data_path).dropna()
    xhs_kw = pd.read_csv('%sxhs_kw_0915.csv' % data_path).dropna()
    kw_lst = tb_kw['keyword_hot'] + jd_kw['keyword_hot'] + xhs_kw['keyword_text']
    kw_lst = list(set(kw_lst))[1:]
    splite_ret, splite_df = kw_split(kw_lst)
    splite_ret.to_csv('%ssplite_query_outside.csv' % data_path, index=False, encoding='utf_8_sig')
    splite_df.to_csv('%ssplite_query_all.outside.csv' % data_path, index=False, encoding='utf_8_sig')


def split_kw_hq():
    data_path = "D:/search_test/"
    hive_unit = HiveUnit(**HIVE_CONFIG)
    his_query = hive_unit.get_df_from_db(r"""
        select query,sum(search_cnt) as search_cnt
        from (
        select query,search_cnt
        from da_dev.historical_query_90d
        union all
        select query,search_cnt
        from da_dev.historical_query_1909_1911 
        union all
        select query,search_cnt
        from da_dev.historical_query_1912_2002 
        union all
        select query,search_cnt
        from da_dev.historical_query_2003_2005 
        )t1
        group by query
        order by search_cnt desc
        """)
    splite_ret, splite_df = kw_split(his_query['query'])
    splite_ret.to_csv('%ssplite_query_hisquery.csv' % data_path, index=False, encoding='utf_8_sig')
    splite_df.to_csv('%ssplite_query_hisquery_all.outside.csv' % data_path, index=False, encoding='utf_8_sig')


def split_kw_pn():
    data_path = "D:/search_test/"
    hive_unit = HiveUnit(**HIVE_CONFIG)
    his_query = hive_unit.get_df_from_db(r"""
            select distinct standardskuname
            from da_dev.search_prod_list 
            """)
    splite_ret, splite_df = kw_split(his_query['standardskuname'])
    splite_ret.to_csv('%ssplite_query_skuname.csv' % data_path, index=False, encoding='utf_8_sig')
    splite_df.to_csv('%ssplite_query_skuname_all.outside.csv' % data_path, index=False, encoding='utf_8_sig')


def default_sku():
    hive_unit = HiveUnit(**HIVE_CONFIG)
    hive_unit.execute(r"""
        drop table if exists da_dev.search_default_sku;
        create table da_dev.search_default_sku
        (
            product_id int,
            sku_id int,
            sku_cd string
        );
        insert into da_dev.search_default_sku
        select product_id,sku_id,sku_cd
        from
        (
            select distinct product_id,sku_cd,sku_id
            from oms.dim_sku_profile
            where to_date(insert_timestamp)=date_sub(current_date,1)
            and product_id<>0)ttt1
        left outer join
        (
        select item_product_id,item_sku
        from(
            select item_product_id,item_sku
            ,row_number() over(partition by item_product_id order by sales desc) as rn
            from(
                select item_product_id,item_sku,sum(apportion_amount) as sales
                from oms. ods_sales_order_item 
                where dt='current'
                and to_date(create_time) between date_sub(current_date,91) and date_sub(current_date,1)
                group by item_product_id,item_sku
            )t1)tt1
        where rn=1)ttt2 where ttt1.product_id=ttt2.item_product_id and ttt1.sku_cd=ttt2.item_sku
    """)
    sku = hive_unit.get_df_from_db(
        r"select distinct product_id as op_code, sku_id from da_dev.search_default_sku where rn=1")
    hive_unit.release()
    sku_dic = {}
    for inx, row in sku.iterrows():
        sku_dic[str(row['op_code'])] = str(row['sku_id'])
    print(sku_dic)
    dump_json(sku_dic, "D:/search_test/default_sku.json")


def gen_associated_kw():
    hive_unit = HiveUnit(**HIVE_CONFIG)
    # product_list = pd.read_excel(PRODUCT_LIST)
    # hive_unit.df2db(product_list, 'da_dev.search_prod_list')
    select_sku = hive_unit.get_df_from_db(r"""
            select product_id,sku_code,rn
            from(
            select tt1.product_id,tt1.sku_code,tt2.sales
            ,row_number() over(partition by tt1.product_id order by tt1.if_bundle desc, tt2.sales desc) as rn
            from
            (
                select distinct product_id,sku_code
                ,case when sku_code like 'V%' then 1 else 0 end as if_bundle
                from da_dev.search_prod_list
                where product_id<>0)tt1
            left outer join
            (
               select t1.sku_code,t2.sales
                from
                (
                    select distinct sku_code,product_id
                    from crm.dim_product
                    where product_id<>0
                )t1 left outer join
                (
                    select product_id,sum(sales) as sales
                    from crm.fact_trans
                    where to_date(trans_time) between date_sub(current_date,181) and date_sub(current_date,1)
                    and account_id<>0 and product_id<>0
                    and sales>0 and qtys>0 and sales/qtys>0
                    group by product_id) t2 on t1.product_id=t2.product_id
            )tt2 on tt1.sku_code=tt2.sku_code)ttt1
            where rn<=5 and sku_code in (select distinct sku_cd
                                        from oms.dim_sku_profile
                                        where to_date(insert_timestamp)=date_sub(current_date,1))
        """)
    select_sku.to_csv(SELECT_SKU, index=False, encoding='utf_8_sig')
    sku_list = select_sku[['sku_code']].drop_duplicates()['sku_code']
    sku_list = '\',\''.join(sku_list)

    associated_kw = pd.read_excel(ASSOCIATED_KEYWORD_FILE)
    rules = associated_kw[['TagType1', 'TagType2']].drop_duplicates().fillna("\'\'")
    associated_kw = associated_kw.fillna('')
    associated_kw['kw'] = associated_kw['Associated Keyword']
    associated_kw['kw_standard'] = associated_kw['Tag1'].astype('str') + associated_kw['Tag2'].astype('str')
    associated_kw['kw_standard'] = [kw.upper().replace(' ', '') for kw in associated_kw['kw_standard']]
    associated_kw['kw_type'] = associated_kw['TagType1'] + associated_kw['TagType2']
    hive_unit.df2db(associated_kw[['kw', 'kw_standard']], 'da_dev.search_kw_tag_mapping')
    print(hive_unit.get_df_from_db('select * from da_dev.search_kw_tag_mapping'))
    brand_kw = associated_kw[['kw', 'kw_standard', 'kw_type']]
    brand_kw.to_excel(BRAND_KW_FILE, index=False)
    rule_code = []
    i = 0
    for inx, row in rules.iterrows():
        code = r"UPPER(REGEXP_REPLACE(concat({col_name1},{col_name2}),' ','')) as {col_name}". \
            format(col_name1=row['TagType1'].lower(),
                   col_name2=row['TagType2'].lower(),
                   col_name='col_name_' + str(i))
        rule_code.append(code)
        i += 1
    rule_code = ','.join(rule_code)
    prod_tag = hive_unit.get_df_from_db(r"""
    select product_id,{rule_code}
    from da_dev.search_prod_list
    where sku_code in ('{sku_list}')
    """.format(rule_code=rule_code, sku_list=sku_list))
    prod_tag = prod_tag.drop_duplicates()
    prod_tag.to_excel(ITEM_TAG_FILE, index=False)


if __name__ == '__main__':
    # default_sku()
    # gen_associated_kw()
    trie_test(top_k=10)
    # hive_unit = HiveUnit(**HIVE_CONFIG)
    # online = hive_unit.get_df_from_db(
    #     "select * from da_dev.search_behavior_offline_open_id where open_id='oCOkA5UQHzItMiljjDnGFcwfp17I'")
    # print(online)
    # hive_unit.release()
    # item_kw = load_json("D:/search_test/item_kw.json")
    # print(item_kw)
    # print(item_kw['香水大吉岭茶'])
