import pandas as pd
from gensim.models import Word2Vec

from lib.datastructure.config import HIVE_CONFIG
from lib.db.hive_utils import HiveUnit


def load_item_emb():
    # item embedding cluster
    mod = Word2Vec.load('D:/Git_search/Sephora_cn_Personalization_II/data/item_embedding')
    wv_dic = {}
    for w in mod.wv.vocab.keys():
        wv_dic[w] = mod.wv[w]
    wv_df = pd.DataFrame(wv_dic).T
    new_columns = ['em_var_' + str(column_str) + '_no' for column_str in wv_df.columns]
    wv_df.columns = new_columns
    wv_df = wv_df.reset_index().rename(columns={'index': 'product_id'})
    hive_unit = HiveUnit(**HIVE_CONFIG)

    # without filter
    product_info = hive_unit.get_df_from_db("""
    select product_id,product_name
    from(
        select product_id,product_name
        ,row_number() over(partition by product_name order by product_id) as rn
        from(
            select product_id,concat(brand_cn,'+',thirdcategory_cn) as product_name,
            row_number() over(partition by product_id order by sku_code) as rn
            from da_dev.search_prod_list
        )t1
        where rn=1)t2
    where rn=1
    """)

    # filter by hot search keywords
    product_info = hive_unit.get_df_from_db("""
    select product_id,product_name
    from(
        select product_id,product_name
        ,row_number() over(partition by product_name order by product_id) as rn
        from(
            select product_id,concat(brand_cn,'+',thirdcategory_cn) as product_name,
            row_number() over(partition by product_id order by sku_code) as rn
            from da_dev.search_prod_list
            WHERE (brand_cn in ('兰蔻','雅诗兰黛','SK-II','娇兰','娇韵诗','植村秀','纪梵希','资生堂','魅可','阿玛尼','迪奥')
            OR category='Fragrances' OR thirdcategory_cn in ('眼霜','清洁','口红','防晒','面膜','眼影','面霜','底妆'))
            )t1
        where rn=1)t2
    where rn=1
    """)

    # by category
    hive_unit.release()
    wv_df = pd.merge(wv_df, product_info, on='product_id', how='left').dropna().drop_duplicates()
    wv_df.to_csv('D:/test/temp_data/wv.csv', index=False, encoding='utf_8_sig')
    product_info = hive_unit.get_df_from_db("""
    select product_id,product_name
    from(
        select product_id,product_name
        ,row_number() over(partition by product_name order by product_id) as rn
        from(
            select product_id,concat(brand_cn,'+',thirdcategory_cn,'+',fragrance_stereotype) as product_name,
            row_number() over(partition by product_id order by sku_code) as rn
            from da_dev.search_prod_list
            where category='Fragrances'
            )t1
        where rn=1)t2
    where rn=1
    """)
    hive_unit.release()
    wv_df = pd.merge(wv_df, product_info, on='product_id', how='left').dropna().drop_duplicates()
    wv_df.to_csv('D:/test/temp_data/wv_fra.csv', index=False, encoding='utf_8_sig')
    product_info = hive_unit.get_df_from_db("""
       select product_id,product_name
       from(
           select product_id,product_name
           ,row_number() over(partition by product_name order by product_id) as rn
           from(
               select product_id,concat(brand_cn,'+',thirdcategory_cn,'+',skincare_function_basic) as product_name,
               row_number() over(partition by product_id order by sku_code) as rn
               from da_dev.search_prod_list
               where category='Skincare' and skincare_function_basic<>''
               )t1
           where rn=1)t2
       where rn=1
       """)
    hive_unit.release()
    wv_df = pd.merge(wv_df, product_info, on='product_id', how='left').dropna().drop_duplicates()
    wv_df.to_csv('D:/test/temp_data/wv_skin.csv', index=False, encoding='utf_8_sig')
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


def click_by_cardtype():
    hive_unit = HiveUnit(**HIVE_CONFIG)
    df = hive_unit.get_df_from_db(r"""
        select tt1.op_code,vip_card_type,prod_tag,count(distinct user_id) as user_cnt
        from (
            select user_id,op_code,vip_card_type
            from dwd.v_events
            where dt between date_sub(current_date,91) and date_sub(current_date,1)
            and event in ('viewCommondityDetail','$MPViewScreen','PDPClick')
            and platform_type='MiniProgram'
            and op_code rlike '^\\d+$'
            and vip_card_type is not null
        )tt1 left outer join
        (
            select op_code,prod_tag
            from (
            select product_id as op_code, product_name_cn as prod_tag
            ,row_number() over(partition by product_id order by sku_cd) as rn
            from oms.dim_sku_profile)t1
            where rn=1
            )tt2
            on tt1.op_code=tt2.op_code
        group by tt1.op_code,vip_card_type,prod_tag
        order by vip_card_type,user_cnt desc
    """)

    # df = hive_unit.get_df_from_db(r"""
    #      select tt1.op_code,vip_card_type,prod_tag,count(distinct user_id) as user_cnt
    #         from (
    #             select user_id,op_code,vip_card_type
    #             from dwd.v_events
    #             where dt between date_sub(current_date,91) and date_sub(current_date,1)
    #             and event in ('viewCommondityDetail','$MPViewScreen','PDPClick')
    #             and platform_type='MiniProgram'
    #             and op_code rlike '^\\d+$'
    #             and vip_card_type is not null
    #         )tt1 left outer join
    #         (
    #             select product_id as op_code,product_name as prod_tag
    #             from(
    #                 select product_id,product_name
    #                 ,row_number() over(partition by product_name order by product_id) as rn
    #                 from(
    #                     select product_id,concat(brand_cn,'+',thirdcategory_cn) as product_name,
    #                     row_number() over(partition by product_id order by sku_code) as rn
    #                     from da_dev.search_prod_list
    #                     )t1
    #                 where rn=1)t2
    #             where rn=1
    #             )tt2
    #             on tt1.op_code=tt2.op_code
    #         group by tt1.op_code,vip_card_type,prod_tag
    #         order by vip_card_type,user_cnt desc
    #     """)
    # print(df)
    df.to_csv("D:/search_test/proda_by_cardtype.csv", index=False, encoding='utf-8')
    hive_unit.release()
