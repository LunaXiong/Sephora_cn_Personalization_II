# -*- coding:utf-8 -*-
import pandas as pd
import numpy as np

from lib.db.hive_utils import HiveUnit

from lib.model.nn import UserItemEmbedding
from lib.processing.encoder import padding, UserEncoder, cut_name, ItemEncoder
from lib.processing.for_ranking import gen_index


def load_user_item_emb():
    # preparing the data
    hive_unit = HiveUnit(**HIVE_CONFIG)
    item_df = hive_unit.get_df_from_db("select * from da_dev.search_item_profile")
    behavior_df = pd.read_csv("D:/search_test/sample_click_aft_search_neg_10000.csv")
    id_list = behavior_df[['open_id']].drop_duplicates()['open_id']
    id_list = '\',\''.join(id_list)
    user_df = hive_unit.get_df_from_db("""
    select *
    from da_dev.search_user_profile
    where open_id in ('{id_list}')""".format(id_list=id_list))
    hive_unit.release()
    index_df = gen_index(behavior_df)

    user_pad_cols = [x for x in user_df.columns if x != 'open_id']
    user_df = padding(user_df, user_pad_cols, {})

    user_encoder = UserEncoder()
    user_df = user_encoder.gen_for_embedding(user_df)

    item_df = item_df. \
        rename(columns={'product_id': 'op_code'})

    item_df = cut_name(item_df, "standardskuname")
    item_pad_cols = [x for x in item_df.columns if x != 'op_code']
    item_df = padding(item_df, item_pad_cols, {})

    item_encode_cols = item_pad_cols

    item_encoder = ItemEncoder(item_encode_cols)
    item_df = item_encoder.gen_for_embedding(item_df)

    train_data = pd.merge(index_df, user_df, on='open_id', how='inner')
    train_data = pd.merge(train_data, item_df, on='op_code', how='inner').dropna()

    user_cols = user_df.drop(columns=['open_id']).columns
    item_cols = item_df.drop(columns=['op_code']).columns

    user_feature = train_data[list(user_cols)].values
    item_feature = train_data[list(item_cols)].values
    train_labels = train_data['label'].values

    feature_range = max(user_feature.max(), item_feature.max()) + 1

    # train model
    uie = UserItemEmbedding(
        num_user_feature=user_feature.shape[1],
        num_item_feature=item_feature.shape[1],
        max_feature_dim=feature_range
    )
    uie.train((user_feature, item_feature, train_labels))

    # load emb vector
    emb = uie.get_user_embedding(user_feature)
    print(emb[0])
    print(emb[-1])
    emb_df = pd.DataFrame(emb)

    new_columns = ['em_var_' + str(column_str) + '_no' for column_str in emb_df.columns]
    emb_df.columns = new_columns

    emb_df['open_id'] = train_data['open_id']
    emb_df = emb_df.drop_duplicates()

    # combine user info
    hive_unit = HiveUnit(**HIVE_CONFIG)
    user_info = hive_unit.get_df_from_db("""
        select open_id,concat(user_tag,'+',rn) as user_tag
        from(
        select open_id,user_tag,row_number() over(partition by user_tag order by open_id) as rn
        from(
        select open_id,concat(age,'+',member_cardtype,'+',preferred_category,'+',preferred_brand) as user_tag
        from(
            select open_id,gender
            ,case when age<16 then '<16'
            when age<20 then '[16,20)'
            when age<25 then '[20,25)'
            when age<30 then '[25,30)'
            when age<35 then '[30,35)'
            when age<40 then '[35,40)'
            else '>40' end as age
            ,preferred_category
            ,preferred_brand
            ,member_cardtype,city
            from da_dev.search_user_profile
            where open_id in ('{id_list}')
        )t1 left outer join
        da_dev.bds_city_list t2 on regexp_replace(t2.city,'市','')=t1.city)tt1)ttt1
        """.format(id_list=id_list))
    hive_unit.release()
    emb_df = pd.merge(emb_df, user_info, on='open_id', how='left').dropna().drop_duplicates()
    emb_df.to_csv('D:/test/temp_data/user_emb.csv', index=False, encoding='utf_8_sig')


def user_cluster():
    hive_unit = HiveUnit(**HIVE_CONFIG)
    # hive_unit.execute(r"""
    # drop table if exists da_dev.search_user_emb_cluster;
    # create table da_dev.search_user_emb_cluster
    # (
    #     open_id string,
    #     user_tag string,
    #     cluster int
    # );""")
    # df = pd.read_csv("D:/search_test/km.result.final.csv")
    # hive_unit.df2db(df, "da_dev.search_user_emb_cluster")
    # print(df)
    # his_query = hive_unit.get_df_from_db("""
    # select cluster,query,count(distinct sensor_id) as user_cont
    # from(
    # select tt1.cluster,tt1.sensor_id,tt1.user_tag,tt2.query
    # from
    # (
    # select cluster,user_tag,sensor_id
    # from
    # da_dev.search_user_emb_cluster t1
    # left outer join
    # da_dev.tagging_id_mapping t2 on t1.open_id=t2.open_id)tt1
    # left outer join
    # (
    #     select key_words as query,user_id
    #     from dwd.v_events
    #     where dt between date_sub(current_date,181) and date_sub(current_date,1)
    #     and (event= '$MPViewScreen' and page_type_detail='search_list')
    #     and (key_words <>'null' and key_words<>'NULL' and key_words<>'')
    #     and platform_type='MiniProgram'
    # )tt2 on tt1.sensor_id=tt2.user_id)ttt1
    # group by cluster,query
    # """)
    # his_query.to_csv("D:/search_test/user_emb_cluster.csv", index=False, encoding='utf_8_sig')
    his_click = hive_unit.get_df_from_db(r"""
       select cluster,product_tag,count(distinct sensor_id) as user_cont
       from(
       select tt1.cluster,tt1.sensor_id,tt1.user_tag,tt3.product_tag
       from 
       (
       select cluster,user_tag,sensor_id
       from
       da_dev.search_user_emb_cluster t1
       left outer join  
       da_dev.tagging_id_mapping t2 on t1.open_id=t2.open_id)tt1
       left outer join
       (
            select op_code,user_id
            from dwd.v_events 
            where dt between date_sub(current_date,91) and date_sub(current_date,1) 
            and event in ('viewCommondityDetail','$MPViewScreen','PDPClick')
            and platform_type='MiniProgram'
            and op_code rlike '^\\d+$'
       )tt2 on tt1.sensor_id=tt2.user_id
       left outer join 
       (
       select product_id as op_code,product_tag
       from(
           select product_id,concat(brand_cn,'+',thirdcategory_cn) as product_tag,
           row_number() over(partition by product_id order by sku_code) as rn
           from da_dev.search_prod_list
           where brand_cn in ('兰蔻','雅诗兰黛')
           )t1
       where rn=1
       )tt3 on tt3.op_code=tt2.op_code
       where tt3.op_code is not null)ttt1
       group by cluster,product_tag
       """)
    his_click.to_csv("D:/search_test/user_emb_cluster_click.csv", index=False, encoding='utf_8_sig')
    hive_unit.release()


if __name__ == '__main__':
    user_cluster()
