from datetime import datetime

import pandas as pd


from lib.datastructure.config import HIVE_CONFIG
from lib.db.hive_utils import HiveUnit


def gen_user_profile(hive_unit: HiveUnit):
    hive_unit.execute(r"""
    drop table if exists da_dev.search_user_profile;
    create table da_dev.search_user_profile
    (
        open_id string,
        age int,
        gender string,
        city string,
        customer_status_eb string,
        member_tenure_days int,
        member_origin_channel string,
        member_origin_category string,
        member_cardtype string,
        most_visited_category string,
        most_visited_subcategory string,
        most_visited_brand string,
        most_visited_function string,
        preferred_category string,
        preferred_subcategory string,
        preferred_thirdcategory string,
        preferred_brand string,
        skin_type string,
        makeup_maturity string,
        skincare_maturity string,
        makeup_price_range string,
        skincare_price_range string,
        skincare_demand string,
        makeup_demand string,
        fragrance_demand string,
        shopping_driver string,
        purchase_ranking int,
        skincare_item int,
        skincare_revenue float,
        makeup_item int,
        makeup_revenue float,
        fragrance_item int,
        fragrance_revenue float,
        last_90d_purchase int,
        last_90d_purchase_amount float,
        promotion_rate float
    );
    insert into table da_dev.search_user_profile
    select tt1.open_id,tt2.age,tt2.gender,tt2.city,tt2.customer_status_eb,tt2.member_tenure_days
        ,tt2.member_origin_channel,tt2.member_origin_category,tt2.member_cardtype
        ,tt3.most_visited_category,tt3.most_visited_subcategory,tt3.most_visited_brand,tt3.most_visited_function
        ,tt4.preferred_category,tt4.preferred_subcategory,tt4.preferred_thirdcategory,tt4.preferred_brand
        ,tt4.skin_type,tt4.makeup_maturity,tt4.skincare_maturity,tt4.makeup_price_range,tt4.skincare_price_range
        ,tt4.skincare_demand,tt4.makeup_demand,tt4.fragrance_demand,tt4.shopping_driver
        ,tt5.purchase_ranking,tt5.skincare_item,tt5.skincare_revenue,tt5.makeup_item
        ,tt5.makeup_revenue,tt5.fragrance_item,tt5.fragrance_revenue,tt5.last_90d_purchase
        ,tt5.last_90d_purchase_amount,tt5.promotion_rate
    from(
        select sephora_id,card_number,open_id
        from(
            select sephora_id,card_number,open_id
            ,row_number() over(partition by open_id order by sephora_id,card_number) as rn
            from da_dev.tagging_id_mapping 
            where open_id<>''
        )t1
        where rn=1
    )tt1 left outer join 
    (
        select card_number,age,gender,city,customer_status_eb,member_tenure_days
        ,member_origin_channel,member_origin_category,member_cardtype
        from(
            select card_number,age,gender,city,customer_status_eb,member_tenure_days
            ,member_origin_channel,member_origin_category,member_cardtype
            ,row_number() over(partition by card_number order by sephora_id) as rn
            from da_dev.user_basic_info_tagging
            where card_number<>''
        )t1
        where rn=1 
    )tt2 on tt1.card_number=tt2.card_number left outer join
    (
        select sephora_id,most_visited_category,most_visited_subcategory
        ,most_visited_brand,most_visited_function
        from(
            select sephora_id,most_visited_category,most_visited_subcategory
            ,most_visited_brand,most_visited_function
            ,row_number() over(partition by sephora_id order by card_number) as rn
            from da_dev.engagement_tagging
            where sephora_id <>''
        )t1
        where rn=1  
    )tt3 on tt1.sephora_id=tt3.sephora_id left outer join
    (
        select card_number,preferred_category,preferred_subcategory
        ,preferred_thirdcategory,preferred_brand,skin_type,makeup_maturity
        ,skincare_maturity,makeup_price_range,skincare_price_range
        ,skincare_demand,makeup_demand,fragrance_demand,shopping_driver
        from(
            select card_number,preferred_category,preferred_subcategory
            ,preferred_thirdcategory,preferred_brand,skin_type,makeup_maturity
            ,skincare_maturity,makeup_price_range,skincare_price_range
            ,skincare_demand,makeup_demand,fragrance_demand,shopping_driver
            ,row_number() over(partition by card_number order by sephora_id) as rn
            from da_dev.inferred_tagging
            where card_number<>''
        )t1
        where rn=1 
    )tt4 on tt1.card_number=tt4.card_number left outer join
    (
        select card_number,purchase_ranking,skincare_item,skincare_revenue,makeup_item
        ,makeup_revenue,fragrance_item,fragrance_revenue,last_90d_purchase
        ,last_90d_purchase_amount,promotion_rate
        from(
            select card_number,purchase_ranking,skincare_item,skincare_revenue,makeup_item
            ,makeup_revenue,fragrance_item,fragrance_revenue,last_90d_purchase
            ,last_90d_purchase_amount,promotion_rate
            ,row_number() over(partition by card_number order by sephora_id) as rn
            from da_dev.purchase_tagging
            where card_number<>''
        )t1
        where rn=1
    )tt5 on tt1.card_number=tt5.card_number
    """)


def gen_prod_profile(hive_unit: HiveUnit):
    hive_unit.execute(r"""
        drop table if exists da_dev.search_prod_id_mapping;
        create table da_dev.search_prod_id_mapping
        (
            op_code int,
            sku_code string,
            crm_prod_id int
        );
        insert into table da_dev.search_prod_id_mapping
        select t1.op_code,t1.sku_code,t2.product_id as crm_prod_id
        from(
             select sku_cd as sku_code,product_id as op_code
             from oms.dim_sku_profile
             where to_date(insert_timestamp)=current_date
        )t1 left outer join
        crm.dim_product t2 on t1.sku_code=t2.sku_code
    """)
    hive_unit.execute(r"""
        drop table if exists da_dev.search_item_profile;
        create table da_dev.search_item_profile
        (
            product_id int,
            standardskuname string,
            category string,
            subcategory string,
            thirdcategory string,
            brand string,
            brand_origin string,
            detailedcategory string,
            skincare_function_basic string,
            skincare_function_special string,
            makeup_function string,
            makeup_feature_look string,
            makeup_feature_color string,
            target_agegroup string,
            skintype string,
            fragrance_targetgender string,
            fragrance_stereotype string,
            fragrance_impression string,
            if_bundle string
        );
        insert into table da_dev.search_item_profile
        select product_id,standardskuname,category,subcategory,thirdcategory
            ,brand,brand_origin,detailedcategory,skincare_function_basic,skincare_function_special
            ,makeup_function,makeup_feature_look,makeup_feature_color,target_agegroup
            ,skintype,fragrance_targetgender,fragrance_stereotype,fragrance_impression
            ,if_bundle
        from(
                select product_id,standardskuname,category,subcategory,thirdcategory
                ,brand,brand_origin,detailedcategory,skincare_function_basic,skincare_function_special
                ,makeup_function,makeup_feature_look,makeup_feature_color,target_agegroup
                ,skintype,fragrance_targetgender,fragrance_stereotype,fragrance_impression
                ,if_bundle,row_number() over(partition by product_id order by if_bundle desc) as rn
            from(
                    select product_id,standardskuname,category,subcategory,thirdcategory
                    ,brand,brand_origin,detailedcategory,skincare_function_basic,skincare_function_special
                    ,makeup_function,makeup_feature_look,makeup_feature_color,target_agegroup
                    ,skintype,fragrance_targetgender,fragrance_stereotype,fragrance_impression
                    ,case when sku_code like 'V%' then 1 else 0 end as if_bundle
                    from da_dev.search_prod_list 
            )t1)t2
        where rn=1
        """)


def get_prod_profile(hive_unit: HiveUnit):
    query = "select * from da_dev.search_item_profile"
    return hive_unit.get_df_from_db(query)


def gen_behavior(hive_unit: HiveUnit, n_day: int):
    hive_unit.execute(r"""
    drop table if exists da_dev.search_behavior_online ;
    create table da_dev.search_behavior_online
    (
        user_id bigint,
        time string,
        behavior string,
        key_words string,
        op_code string,
        platform string
    );
    insert into table da_dev.search_behavior_online
    
    select distinct t1.user_id,t1.time,t1.behavior,t1.key_words
    ,case when t2.op_code is not null then t2.op_code else t1.op_code end as op_code
    ,platform
    from(
        select distinct user_id,time
        ,case when event='viewCommodityDetail' then 'click'
        when event='AddToShoppingcart' then 'add'
        when event='submitOrder' then 'order'
        else 'search' end as behavior,orderid,key_words,op_code
        ,platform_type as platform
        from dwd.v_events
        where dt between date_sub(current_date,{n_day}+1) and date_sub(current_date,1)
        and platform_type in ('MiniProgram','app')
        and
            (event in ('viewCommodityDetail','AddToShoppingcart','submitOrder')
            or
            (
            event='$MPViewScreen'
            and page_type_detail='search_list'
            and key_words is not null
            and key_words<>'NULL' and key_words<>'')))t1
    left outer join
    (
        select distinct t1.sales_order_number as order_id,t2.item_product_id as op_code
        from
         (
            select sales_order_number,sales_order_sys_id
            from oms.ods_sales_order
            where to_date(order_time) between date_sub(current_date,{n_day}+1) and date_sub(current_date,1)
            and payment_status='1'
            and dt=date_sub(current_date,1))t1
        left outer join
        (
            select distinct sales_order_sys_id,item_product_id
            from oms.ods_sales_order_item
            where dt=date_sub(current_date,1)
            and item_product_id<>'0' and item_product_id<>0)t2
            on t1.sales_order_sys_id=t2.sales_order_sys_id
    )t2 on t1.orderid=t2.order_id
    """.format(n_day=n_day))


def behavior_online(hive_unit: HiveUnit, n_day: int):
    gen_behavior(hive_unit, n_day)
    hive_unit.execute(r"""
    drop table if exists da_dev.search_behavior_online_open_id ;
    create table da_dev.search_behavior_online_open_id
    (
        open_id string,
        time string,
        behavior string,
        op_code string,
        platform string
    );
    insert into table da_dev.search_behavior_online_open_id
    select t1.open_id,t2.time,t2.behavior,t2.op_code,t2.platform
    from 
    (
        select distinct open_id,sensor_id
        from da_dev.tagging_id_mapping 
        where open_id<>'' 
    )t1 left outer join
    (
        select distinct user_id,time,op_code,behavior
        from da_dev.search_behavior_online
        where behavior<>'search' and op_code<>'0' 
    ) t2 on t1.sensor_id=t2.user_id
    """)


def behavior_offline(hive_unit: HiveUnit, n_day: int):
    hive_unit.execute(r"""
    drop table if exists da_dev.search_behavior_offline_open_id ;
    create table da_dev.search_behavior_offline_open_id
    (
        open_id string,
        time string,
        behavior string,
        op_code string
    );
    insert into table da_dev.search_behavior_offline_open_id
    select t1.open_id as user_id,t3.trans_time as time
    ,'order' as behavior
    ,cast(t4.op_code as string) as op_code
    from
    (
        select distinct open_id,card_number
        from da_dev.tagging_id_mapping 
        where open_id<>'' and card_number<>''
    ) t1 left outer join
    (
        select distinct account_number as card_number,account_id
        from crm.dim_account 
        where account_id<>0  
    ) t2 on t1.card_number=t2.card_number
    left outer join
    (
        select account_id,product_id,trans_time
        from crm.fact_trans
        where to_date(trans_time) between date_sub(current_date,{n_day}+1) and date_sub(current_date,1)
        and account_id<>0
        and qtys>0 and sales>0
        and store_id in(select distinct store_id from crm.dim_store where is_eb_store=1)
    )t3 on t2.account_id=t3.account_id 
    left outer join
    (
        select distinct op_code,crm_product_id
        from da_dev. product_id_mapping
    )t4 on t3.product_id=t4.crm_product_id
    where op_code<>0
    """.format(n_day=n_day))


def sample_user():
    user_list = pd.read_csv("D:/search_test/train_users.csv")
    user_list = '\',\''.join(user_list['open_id'])
    print(user_list)
    hive_unit = HiveUnit(**HIVE_CONFIG)
    df = hive_unit.get_df_from_db(r"""
        select t1.open_id,t2.time,t2.op_code,t2.behavior
        from
        (
            select distinct open_id,sensor_id
            from da_dev.tagging_id_mapping
            where open_id<>''
        )t1 left outer join
        (
            select distinct user_id,time,op_code,behavior
            from da_dev.search_behavior_online
            where behavior<>'search' and op_code<>'0'
        ) t2 on t1.sensor_id=t2.user_id
        where t1.open_id in ('{user_list}')
        """.format(user_list=user_list))
    return df


def gen_num_process(hive_unit: HiveUnit):
    num_process_df = hive_unit.get_df_from_db(r"""
        select 
        t1.open_id as open_id,
        t1.gender as gender,
        t1.preferred_category as preferred_category,
        t1.preferred_brand as preferred_brand,
        t1.skincare_price_range as skincare_price_range,
        t1.skincare_demand as skincare_demand,
        t1.member_origin_category as member_origin_category,
        t1.makeup_price_range as makeup_price_range,
        t1.fragrance_demand as fragrance_demand,
        t1.most_visited_brand as most_visited_brand,
        t1.member_origin_channel as member_origin_channel,
        t1.makeup_demand as makeup_demand,
        t1.customer_status_eb as customer_status_eb,
        t1.most_visited_subcategory as most_visited_subcategory,
        t1.skincare_maturity as skincare_maturity,
        t1.makeup_maturity as makeup_maturity,
        t1.promotion_rate as promotion_rate,
        t1.most_visited_function as most_visited_function,
        t1.skin_type as skin_type,
        t1.member_cardtype as member_cardtype,
        t1.shopping_driver as shopping_driver,
        t1.preferred_thirdcategory as preferred_thirdcategory,
        t1.preferred_subcategory as preferred_subcategory,
        t1.most_visited_category as most_visited_category,
        t1.city as city,
        case when age between t2.age_min and t2.age_25 then CONCAT('(',t2.age_min,',',t2.age_25,']')
        when age between t2.age_25 and t2.age_75 then CONCAT('(',t2.age_25,',', t2.age_75,']')
        when age between t2.age_75 and t2.age_max then CONCAT('(',t2.age_75,',', t2.age_max,']')
        else '-1' end as age,
        case when member_tenure_days between t2.member_tenure_days_min and t2.member_tenure_days_25 
        then CONCAT('(',t2.member_tenure_days_min,',',t2.member_tenure_days_25,']')
        when member_tenure_days between t2.member_tenure_days_25 and t2.member_tenure_days_75 
        then CONCAT('(',t2.member_tenure_days_25,',', t2.member_tenure_days_75,']')
        when member_tenure_days between t2.member_tenure_days_75 and t2.member_tenure_days_max 
        then CONCAT('(',t2.member_tenure_days_75,',', t2.member_tenure_days_max,']')
        else '-1' end as member_tenure_days,
        case when purchase_ranking between t2.purchase_ranking_min and t2.purchase_ranking_25 
        then CONCAT('(',t2.purchase_ranking_min,',',t2.purchase_ranking_25,']')
        when purchase_ranking between t2.purchase_ranking_25 and t2.purchase_ranking_75 
        then CONCAT('(',t2.purchase_ranking_25,',', t2.purchase_ranking_75,']')
        when purchase_ranking between t2.purchase_ranking_75 and t2.purchase_ranking_max 
        then CONCAT('(',t2.purchase_ranking_75,',', t2.purchase_ranking_max,']')
        else '-1' end as purchase_ranking,
        case when skincare_item between t2.skincare_item_min and t2.skincare_item_25 
        then CONCAT('(',t2.skincare_item_min,',',t2.skincare_item_25,']')
        when skincare_item between t2.skincare_item_25 and t2.skincare_item_75 
        then CONCAT('(',t2.skincare_item_25,',', t2.skincare_item_75,']')
        when skincare_item between t2.skincare_item_75 and t2.skincare_item_max 
        then CONCAT('(',t2.skincare_item_75,',', t2.skincare_item_max,']')
        else '-1' end as skincare_item,
        case when skincare_revenue between t2.skincare_revenue_min and t2.skincare_revenue_25 
        then CONCAT('(',t2.skincare_revenue_min,',',t2.skincare_revenue_25,']')
        when skincare_revenue between t2.skincare_revenue_25 and t2.skincare_revenue_75 
        then CONCAT('(',t2.skincare_revenue_25,',', t2.skincare_revenue_75,']')
        when skincare_revenue between t2.skincare_revenue_75 and t2.skincare_revenue_max 
        then CONCAT('(',t2.skincare_revenue_75,',', t2.skincare_revenue_max,']')
        else '-1' end as skincare_revenue,
        case when makeup_revenue between t2.makeup_revenue_min and t2.makeup_revenue_25 
        then CONCAT('(',t2.makeup_revenue_min,',',t2.makeup_revenue_25,']')
        when makeup_revenue between t2.makeup_revenue_25 and t2.makeup_revenue_75 
        then CONCAT('(',t2.makeup_revenue_25,',', t2.makeup_revenue_75,']')
        when makeup_revenue between t2.makeup_revenue_75 and t2.makeup_revenue_max 
        then CONCAT('(',t2.makeup_revenue_75,',', t2.makeup_revenue_max,']')
        else '-1' end as makeup_revenue,
        case when makeup_item between t2.makeup_item_min and t2.makeup_item_25 
        then CONCAT('(',t2.makeup_item_min,',',t2.makeup_item_25,']')
        when makeup_item between t2.makeup_item_25 and t2.makeup_item_75 
        then CONCAT('(',t2.makeup_item_25,',', t2.makeup_item_75,']')
        when makeup_item between t2.makeup_item_75 and t2.makeup_item_max 
        then CONCAT('(',t2.makeup_item_75,',', t2.makeup_item_max,']')
        else '-1' end as makeup_item,
        case when fragrance_item between t2.fragrance_item_min and t2.fragrance_item_25 
        then CONCAT('(',t2.fragrance_item_min,',',t2.fragrance_item_25,']')
        when fragrance_item between t2.fragrance_item_25 and t2.fragrance_item_75 
        then CONCAT('(',t2.fragrance_item_25,',', t2.fragrance_item_75,']')
        when fragrance_item between t2.fragrance_item_75 and t2.fragrance_item_max 
        then CONCAT('(',t2.fragrance_item_75,',', t2.fragrance_item_max,']')
        else '-1' end as fragrance_item,
        case when fragrance_revenue between t2.fragrance_revenue_min and t2.fragrance_revenue_25 
        then CONCAT('(',t2.fragrance_revenue_min,',',t2.fragrance_revenue_25,']')
        when fragrance_revenue between t2.fragrance_revenue_25 and t2.fragrance_revenue_75 
        then CONCAT('(',t2.fragrance_revenue_25,',', t2.fragrance_revenue_75,']')
        when fragrance_revenue between t2.fragrance_revenue_75 and t2.fragrance_revenue_max 
        then CONCAT('(',t2.fragrance_revenue_75,',', t2.fragrance_revenue_max,']')
        else '-1' end as fragrance_revenue,
        case when last_90d_purchase between t2.last_90d_purchase_min and t2.last_90d_purchase_25 
        then CONCAT('(',t2.last_90d_purchase_min,',',t2.last_90d_purchase_25,']')
        when last_90d_purchase between t2.last_90d_purchase_25 and t2.last_90d_purchase_75 
        then CONCAT('(',t2.last_90d_purchase_25,',', t2.last_90d_purchase_75,']')
        when last_90d_purchase between t2.last_90d_purchase_75 and t2.last_90d_purchase_max 
        then CONCAT('(',t2.last_90d_purchase_75,',', t2.last_90d_purchase_max,']')
        else '-1' end as last_90d_purchase,
        case when last_90d_purchase_amount between t2.last_90d_purchase_amount_min and t2.last_90d_purchase_amount_25 
        then CONCAT('(',t2.last_90d_purchase_amount_min,',',t2.last_90d_purchase_amount_25,']')
        when last_90d_purchase_amount between t2.last_90d_purchase_amount_25 and t2.last_90d_purchase_amount_75 
        then CONCAT('(',t2.last_90d_purchase_amount_25,',', t2.last_90d_purchase_amount_75,']')
        when last_90d_purchase_amount between t2.last_90d_purchase_amount_75 and t2.last_90d_purchase_amount_max 
        then CONCAT('(',t2.last_90d_purchase_amount_75,',', t2.last_90d_purchase_amount_max,']')
        else '-1' end as last_90d_purchase_amount
        from (
            select 
            CEILING(min(t1.age)) as age_min,
            CEILING(percentile(t1.age, 0.25)) as age_25,
            CEILING(percentile(t1.age, 0.75)) as age_75,
            CEILING(max(t1.age)) as age_max,
            CEILING(min(t1.member_tenure_days)) as member_tenure_days_min,
            CEILING(percentile(t1.member_tenure_days, 0.25)) as member_tenure_days_25,
            CEILING(percentile(t1.member_tenure_days, 0.75)) as member_tenure_days_75,
            CEILING(max(t1.member_tenure_days)) as member_tenure_days_max,
            CEILING(min(t1.purchase_ranking)) as purchase_ranking_min,
            CEILING(percentile(t1.purchase_ranking, 0.25)) as purchase_ranking_25,
            CEILING(percentile(t1.purchase_ranking, 0.75)) as purchase_ranking_75,
            CEILING(max(t1.purchase_ranking)) as purchase_ranking_max,
            CEILING(min(t1.skincare_item)) as skincare_item_min,
            CEILING(percentile(t1.skincare_item, 0.25)) as skincare_item_25,
            CEILING(percentile(t1.skincare_item, 0.75)) as skincare_item_75,
            CEILING(max(t1.skincare_item)) as skincare_item_max,
            CEILING(min(t1.skincare_revenue)) as skincare_revenue_min,
            CEILING(percentile_approx(t1.skincare_revenue, 0.25)) as skincare_revenue_25,
            CEILING(percentile_approx(t1.skincare_revenue, 0.75)) as skincare_revenue_75,
            CEILING(max(t1.skincare_revenue)) as skincare_revenue_max,
            CEILING(min(t1.makeup_revenue)) as makeup_revenue_min,
            CEILING(percentile_approx(t1.makeup_revenue, 0.25)) as makeup_revenue_25,
            CEILING(percentile_approx(t1.makeup_revenue, 0.75)) as makeup_revenue_75,
            CEILING(max(t1.makeup_revenue)) as makeup_revenue_max,
            CEILING(min(t1.makeup_item)) as makeup_item_min,
            CEILING(percentile_approx(t1.makeup_item, 0.25)) as makeup_item_25,
            CEILING(percentile_approx(t1.makeup_item, 0.75)) as makeup_item_75,
            CEILING(max(t1.makeup_item)) as makeup_item_max,
            CEILING(min(t1.fragrance_item)) as fragrance_item_min,
            CEILING(percentile_approx(t1.fragrance_item, 0.25)) as fragrance_item_25,
            CEILING(percentile_approx(t1.fragrance_item, 0.75)) as fragrance_item_75,
            CEILING(max(t1.fragrance_item)) as fragrance_item_max,
            CEILING(min(t1.fragrance_revenue)) as fragrance_revenue_min,
            CEILING(percentile_approx(t1.fragrance_revenue, 0.25)) as fragrance_revenue_25,
            CEILING(percentile_approx(t1.fragrance_revenue, 0.75)) as fragrance_revenue_75,
            CEILING(max(t1.fragrance_revenue)) as fragrance_revenue_max,
            CEILING(min(t1.last_90d_purchase)) as last_90d_purchase_min,
            CEILING(percentile_approx(t1.last_90d_purchase, 0.25)) as last_90d_purchase_25,
            CEILING(percentile_approx(t1.last_90d_purchase, 0.75)) as last_90d_purchase_75,
            CEILING(max(t1.last_90d_purchase)) as last_90d_purchase_max,
            CEILING(min(t1.last_90d_purchase_amount)) as last_90d_purchase_amount_min,
            CEILING(percentile_approx(t1.last_90d_purchase_amount, 0.25)) as last_90d_purchase_amount_25,
            CEILING(percentile_approx(t1.last_90d_purchase_amount, 0.75)) as last_90d_purchase_amount_75,
            CEILING(max(t1.last_90d_purchase_amount)) as last_90d_purchase_amount_max
            from da_dev.search_user_profile t1
        )t2
        cross join da_dev.search_user_profile t1
        """)

    return num_process_df


def run_user_item_profile():
    hive_unit = HiveUnit(**HIVE_CONFIG)
    gen_user_profile(hive_unit)
    hive_unit.release()
    hive_unit = HiveUnit(**HIVE_CONFIG)
    gen_prod_profile(hive_unit)
    hive_unit.release()
    hive_unit = HiveUnit(**HIVE_CONFIG)
    behavior_online(hive_unit, n_day=180)
    hive_unit.release()
    hive_unit = HiveUnit(**HIVE_CONFIG)
    behavior_offline(hive_unit, n_day=180)
    hive_unit.release()
