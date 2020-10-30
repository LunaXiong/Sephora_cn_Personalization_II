import json

import pandas as pd
from gensim.models.word2vec import Word2Vec

from lib.datastructure.config import HIVE_CONFIG
from lib.db.hive_utils import HiveUnit
from lib.eval import eval_item_embedding, item_embedding_sequence_eval
from lib.datastructure.files import ITEM_EMBEDDING
from lib.datastructure.files import *
from lib.utils.utils import dump_json


def pair_wise_eval():
    test_data = pd.read_excel('./data/prod_sim_test_data_v2.xlsx')
    feed_data = []
    for index, row in test_data.iterrows():
        feed_data.append((str(row['product_id_A']), str(row['product_id_B']), row['Label']))

    embedding_model = Word2Vec.load(ITEM_EMBEDDING)
    print(len(embedding_model.wv.vocab))

    eval_ret = eval_item_embedding(embedding_model, feed_data)
    eval_ret = pd.DataFrame(eval_ret, columns=['item1', 'item2', 'label', 'pred'])
    print(eval_ret)

    eval_ret.to_csv('./data/item_embedding_eval_ret.csv', index=False)


def seq_wise_eval():
    item_embedding_model = Word2Vec.load(ITEM_EMBEDDING)
    seqs = pd.read_csv('./data/item_seq_v2.csv')['op_code'].to_list()
    seqs = [json.loads(s.replace('\'', '\"')) for s in seqs]
    seqs = [[item for item in seq if item in item_embedding_model.wv] for seq in seqs]
    seqs = [seq for seq in seqs if len(seq) > 5 and len(seq) < 20]
    seqs = seqs[:1000]
    eval_ret = item_embedding_sequence_eval(
        item_embedding_model=item_embedding_model, eval_seqs=seqs)

    dump_json(eval_ret, './data/item_embedding_seq_eval.json')


def item_cluster_eval():
    mod = Word2Vec.load('D:/Git_search/Sephora_cn_Personalization_II/data/item_embedding')
    wv_dic = {}
    for w in mod.wv.vocab.keys():
        wv_dic[w] = mod.wv[w]
    wv_df = pd.DataFrame(wv_dic).T
    new_columns = ['em_var_' + str(column_str) + '_no' for column_str in wv_df.columns]
    wv_df.columns = new_columns
    wv_df = wv_df.reset_index().rename(columns={'index': 'product_id'})
    hive_unit = HiveUnit(**HIVE_CONFIG)

    # product_info = hive_unit.get_df_from_db("""
    # select product_id,product_name
    # from(
    #     select product_id,product_name
    #     ,row_number() over(partition by product_name order by product_id) as rn
    #     from(
    #         select product_id,concat(brand_cn,'+',thirdcategory_cn) as product_name,
    #         row_number() over(partition by product_id order by sku_code) as rn
    #         from da_dev.search_prod_list
    #     )t1
    #     where rn=1)t2
    # where rn=1
    # """)
    # product_info = hive_unit.get_df_from_db("""
    # select product_id,product_name
    # from(
    #     select product_id,product_name
    #     ,row_number() over(partition by product_name order by product_id) as rn
    #     from(
    #         select product_id,concat(brand_cn,'+',thirdcategory_cn) as product_name,
    #         row_number() over(partition by product_id order by sku_code) as rn
    #         from da_dev.search_prod_list
    #         WHERE (brand_cn in ('兰蔻','雅诗兰黛','SK-II','娇兰','娇韵诗','植村秀','纪梵希','资生堂','魅可','阿玛尼','迪奥')
    #         OR category='Fragrances' OR thirdcategory_cn in ('眼霜','清洁','口红','防晒','面膜','眼影','面霜','底妆'))
    #         )t1
    #     where rn=1)t2
    # where rn=1
    # """)
    # hive_unit.release()
    # wv_df = pd.merge(wv_df, product_info, on='product_id', how='left').dropna().drop_duplicates()
    # wv_df.to_csv('D:/test/temp_data/wv.csv', index=False, encoding='utf_8_sig')
    # product_info = hive_unit.get_df_from_db("""
    # select product_id,product_name
    # from(
    #     select product_id,product_name
    #     ,row_number() over(partition by product_name order by product_id) as rn
    #     from(
    #         select product_id,concat(brand_cn,'+',thirdcategory_cn,'+',fragrance_stereotype) as product_name,
    #         row_number() over(partition by product_id order by sku_code) as rn
    #         from da_dev.search_prod_list
    #         where category='Fragrances'
    #         )t1
    #     where rn=1)t2
    # where rn=1
    # """)
    # hive_unit.release()
    # wv_df = pd.merge(wv_df, product_info, on='product_id', how='left').dropna().drop_duplicates()
    # wv_df.to_csv('D:/test/temp_data/wv_fra.csv', index=False, encoding='utf_8_sig')
    # product_info = hive_unit.get_df_from_db("""
    #    select product_id,product_name
    #    from(
    #        select product_id,product_name
    #        ,row_number() over(partition by product_name order by product_id) as rn
    #        from(
    #            select product_id,concat(brand_cn,'+',thirdcategory_cn,'+',skincare_function_basic) as product_name,
    #            row_number() over(partition by product_id order by sku_code) as rn
    #            from da_dev.search_prod_list
    #            where category='Skincare' and skincare_function_basic<>''
    #            )t1
    #        where rn=1)t2
    #    where rn=1
    #    """)
    # hive_unit.release()
    # wv_df = pd.merge(wv_df, product_info, on='product_id', how='left').dropna().drop_duplicates()
    # wv_df.to_csv('D:/test/temp_data/wv_skin.csv', index=False, encoding='utf_8_sig')
    product_info = hive_unit.get_df_from_db("""
           select product_id,product_name
           from(
               select product_id,product_name
               ,row_number() over(partition by product_name order by product_id) as rn
               from(
                   select product_id,concat(brand_cn,'+',thirdcategory_cn,'+',makeup_feature_color_cn) as product_name,
                   row_number() over(partition by product_id order by sku_code) as rn
                   from da_dev.search_prod_list
                   where category='Makeup' and makeup_feature_color_cn<>'' and subcategory_cn='唇部彩妆'
                   )t1
               where rn=1)t2
           where rn=1   
           """)
    hive_unit.release()
    wv_df = pd.merge(wv_df, product_info, on='product_id', how='left').dropna().drop_duplicates()
    wv_df.to_csv('D:/test/temp_data/wv_mku_lip.csv', index=False, encoding='utf_8_sig')


def eval_seq_with_session():
    item_embedding_model = Word2Vec.load(ITEM_EMBEDDING)
    behavior = pd.read_csv('./data/behavior_by_session.csv')
    seqs = behavior.groupby(['user_id', 'sessionid'])['op_code'].apply(lambda x: x.to_list()).reset_index()
    seqs = seqs['op_code'].to_list()
    seqs = [[str(x) for x in s if str(x) in item_embedding_model.wv] for s in seqs]
    seqs = [s for s in seqs if len(s) >= 5]
    print(seqs[0])
    eval_ret = item_embedding_sequence_eval(item_embedding_model, seqs)

    dump_json(eval_ret, './data/item_embedding_seq_eval_with_session.json')


if __name__ == '__main__':
    hive_unit = HiveUnit(**HIVE_CONFIG)
    # item_emb = pd.read_csv("D:/search_test/km.result.final.csv")
    # print(item_emb)
    # hive_unit.df2db(item_emb, 'da_dev.search_item_emb_cluster')
    # hive_unit.execute(r"""
    # drop table if exists da_dev.search_analysis_1y_click;
    # create table da_dev.search_analysis_1y_click
    # (
    #     user_id bigint,
    #     distinct_id string,
    #     vip_card string,
    #     vip_card_type string,
    #     city string,
    #     op_code int,
    #     click_cnt int,
    #     category_cn string,
    #     product_tag string,
    #     cluster int
    # );
    # insert into table da_dev.search_analysis_1y_click
    # select user_id,distinct_id,vip_card,vip_card_type,city,op_code,click_cnt
    # ,category_cn,product_tag,cluster
    # from(
    # select user_id,distinct_id,vip_card,vip_card_type,op_code,city
    # ,count(distinct time) as click_cnt
    # from(
    #     select user_id,distinct_id,vip_card,vip_card_type,op_code,time,do_city as city
    #     from dwd.v_events
    #     where dt between date_sub(current_date,361) and date_sub(current_date,1)
    #     and event = 'viewCommodityDetail'
    #     and op_code rlike '^\\d+$'
    #     and platform_type='MiniProgram'
    # ) t1
    # group by user_id,distinct_id,vip_card,vip_card_type,op_code,city) tt1
    # left outer join
    # (
    # select product_id,category_cn,product_tag
    # from(
    #         select product_id,category_cn
    #         ,concat(brand_cn,'+',thirdcategory_cn,'+',fragrance_stereotype) as product_tag
    #         ,row_number() over(partition by product_id order by sku_code) as rn
    #         from da_dev.search_prod_list
    # )t1
    # where rn=1
    # )tt2 on tt1.op_code=tt2.product_id
    # left outer join
    # da_dev.search_item_emb_cluster tt3
    # on tt2.product_tag=tt3.product_name
    # """)
    # df = hive_unit.get_df_from_db("select * from da_dev.search_analysis_1y_click where cluster is not null limit 100 ")
    # print(df)
    # hive_unit.execute(r"""
    # drop table if exists da_dev.search_analysis_1y_purchase;
    # create table da_dev.search_analysis_1y_purchase
    # (
    #     card_number string,
    #     product_id int,
    #     sales float,
    #     qtys int,
    #     category_cn string
    # );
    # insert into table da_dev.search_analysis_1y_purchase
    # select card_number,ttt1.product_id,sales,qtys,category_cn
    # from(
    #         select card_number,product_id,sum(sales) as sales, sum(qtys) as qtys
    #         from(
    #         select t2.account_number as card_number,t4.product_id,t1.sales,t1.qtys
    #         from
    #         (
    #                 select account_id,product_id,sales,qtys
    #                 from crm.fact_trans
    #                 where to_date(trans_time)  between date_sub(current_date,11) and date_sub(current_date,1)
    #                 and account_id<>0
    #                 and sales>0 and qtys>0
    #                 and sales/qtys<20000
    #                 and product_id is not null
    #         )t1 left outer join
    #         (
    #                 select distinct account_id,account_number
    #                 from crm.dim_account
    #                 where account_id<>0
    #         )t2 on t1.account_id=t2.account_id
    #         left outer join
    #         (
    #                 select distinct product_id,sku_code,category
    #                 from crm.dim_product
    #         )t3 on t1.product_id=t3.product_id
    #         left outer join
    #         (
    #                 select distinct product_id,sku_cd
    #                 from oms.dim_sku_profile
    #                 where to_date(insert_timestamp)=date_sub(current_date,1)
    #         )t4 on t3.sku_code=t4.sku_cd
    #         where t4.sku_cd is not null
    #         and t2.account_id is not null
    #         )tt1
    #         group by card_number,product_id
    # )ttt1 left outer join
    # (
    # select product_id,category_cn
    # from(
    #         select product_id,category_cn
    #         ,row_number() over(partition by product_id order by sku_code) as rn
    #         from da_dev.search_prod_list
    # )t1
    # where rn=1
    # )ttt2 on ttt1.product_id=ttt2.product_id
    # """)
    # df = hive_unit.get_df_from_db("select distinct category_cn from da_dev.search_analysis_1y_purchase limit 100")
    # print(df)
    # df = hive_unit.get_df_from_db(r"""
    # select cluster,vip_card_type,count(distinct user_id) as user_cnt
    # from da_dev.search_analysis_1y_click
    # group by cluster,vip_card_type
    # """)
    # df.to_csv("D:/search_test/fra_card_type.csv", index=False)
    # item_emb = pd.read_csv("D:/search_test/km.result.final_skin.csv")
    # print(item_emb)
    # hive_unit.df2db(item_emb, 'da_dev.search_item_emb_cluster_skin')
    # hive_unit.execute(r"""
    # drop table if exists da_dev.search_analysis_hy_click;
    # create table da_dev.search_analysis_hy_click
    # (
    #     user_id bigint,
    #     distinct_id string,
    #     vip_card string,
    #     vip_card_type string,
    #     op_code int,
    #     click_cnt int,
    #     category_cn string,
    #     product_tag string,
    #     cluster int
    # );
    # insert into table da_dev.search_analysis_hy_click
    # select user_id,distinct_id,vip_card,vip_card_type,op_code,click_cnt
    # ,category_cn,product_tag,cluster
    # from(
    # select user_id,distinct_id,vip_card,vip_card_type,op_code
    # ,count(distinct time) as click_cnt
    # from(
    #     select user_id,distinct_id,vip_card,vip_card_type,op_code,time
    #     from dwd.v_events
    #     where dt between date_sub(current_date,181) and date_sub(current_date,1)
    #     and event = 'viewCommodityDetail'
    #     and op_code rlike '^\\d+$'
    #     and platform_type='MiniProgram'
    # ) t1
    # group by user_id,distinct_id,vip_card,vip_card_type,op_code) tt1
    # left outer join
    # (
    # select product_id,category_cn,product_tag
    # from(
    #         select product_id,category_cn
    #         ,concat(brand_cn,'+',thirdcategory_cn,'+',skincare_function_basic) as product_tag
    #         ,row_number() over(partition by product_id order by sku_code) as rn
    #         from da_dev.search_prod_list
    # )t1
    # where rn=1
    # )tt2 on tt1.op_code=tt2.product_id
    # left outer join
    # da_dev.search_item_emb_cluster_skin tt3
    # on tt2.product_tag=tt3.product_name
    # """)
    # df = hive_unit.get_df_from_db(r"""
    # select cluster, gender, count(distinct user_id) as user_cnt
    # from(
    # select user_id, cluster
    # from da_dev.search_analysis_hy_click
    # where cluster is not null)t1
    # left outer join
    # (select distinct sensor_id, sephora_id
    # from da_dev.tagging_id_mapping
    # where sensor_id is not null)t2
    # on t1.user_id=t2.sensor_id
    # left outer join
    # (
    # select distinct sephora_id,gender
    # from da_dev.user_basic_info_tagging
    # where sephora_id is not null)t3
    # on t2.sephora_id=t3.sephora_id
    # group by cluster, gender
    #  """)
    # print(df)
    # df.to_csv("D:/search_test/skin_gender.csv", index=False)
    df = hive_unit.get_df_from_db(r"""
    select cluster,citytiername,count(distinct user_id) as user_cnt
    from (
    select distinct cluster,user_id,city
    from da_dev.search_analysis_1y_click )t1
    left outer join
    (select  distinct regexp_replace(city,'市','') as city,citytiername
    from da_dev.bds_city_list)t2 on t1.city=t2.city
    group by cluster,citytiername
    """)
    print(df)
    df.to_csv("D:/search_test/fra_citytier.csv", index=False)
    hive_unit.release()

