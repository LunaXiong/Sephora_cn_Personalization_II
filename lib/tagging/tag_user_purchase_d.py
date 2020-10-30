import time

from lib.db.hive_utils import HiveUnit


def user_purchase(hive_unit: HiveUnit):
    t1 = time.clock()
    hive_unit.execute(r"""
       drop table if exists da_dev.product_combine;
        create table da_dev.product_combine
        (
            op_code int,
            product_id int,
            sku_code string,
            category1 string,
            category2 string,
            category3 string,
            skin_type string,
            price float,
            price_level string,
            brand string,
            skincare_function_basic	string,
            skincare_function_special string,
            makeup_function	string,
            makeup_feature_scene string,
            makeup_feature_look string,
            makeup_feature_color string,
            fragrance_stereotype string,
            fragrance_intensity	string,
            fragrance_impression string,
            if_gift int
        );
        insert into table da_dev.product_combine
        select t2.product_id as op_code,t1.product_id,t1.sku_code
        ,case when t2.category is null then t1.category else t2.category end as category1
        ,t2.subcategory as category2,t2.thirdcategory as category3
        ,t2.skintype as skin_type
        ,t1.price
        ,case when t1.price>0 and price<=50 then '(0,50]'
        when t1.price>0 and price<=100 then '(0,100]'
        when t1.price>0 and price<=150 then '(0,150]'
        when t1.price>0 and price<=200 then '(0,200]'
        when t1.price>0 and price<=250 then '(0,250]'
        when t1.price>0 and price<=300 then '(0,300]'
        when t1.price>0 and price<=350 then '(0,350]'
        when t1.price>0 and price<=400 then '(0,400]'
        when t1.price>400 then '>400' end as price_level
        ,case when t2.brand is null then t1.brand else upper(t2.brand) end as brand
        ,t2.skincare_function_basic,t2.skincare_function_special,t2.makeup_function
        ,t2.makeup_feature_scene,t2.makeup_feature_look,t2.makeup_feature_color,t2.fragrance_stereotype,t2.fragrance_intensity
        ,t2.fragrance_impression
        ,case when t1.product_name like '%礼盒%' or t2.bundleproduct_festival is not null then 1 else 0 end as if_gift
        from
        (
            select product_id,sku_code,price,brand,product_name,
            case when category in ('MAKE UP','MAKE UP ACCESSORIES','MAKE UP TESTER','MAKE-UP SAMPLES','MAKEUP TESTER','GIFT MAKE UP') then 'Makeup'
            when category in ('SKINCARE','SKINCARE ACCESSORIES','SKINCARE DEMO','SKINCARE SAMPLES','SKINCARE TESTER','BATH','BATH & GIFT','BATHCARE TESTER','GIFT SKINCARE','HAIR','HAIR ACCESSORIES','HAIR CARE TESTER','HAIR PRODUCT SAMPLES','HAIRCARE') then 'Skincare'
            when category in ('FRAGRANCE','FRAGRANCE ACCESS','FRAGRANCE TESTER','PERFUME SAMPLES','GIFT FRAGANCE') then 'Fragraces'
            when category in ('WELLNESS') then 'Wellness'
            else 'other_offline' end as category
            from crm.dim_product) t1 left outer join
        da_dev.search_prod_list t2 on t1.sku_code=t2.sku_code
    """)
    t2 = time.clock()
    print('Combine Product Done: %f' % (t2 - t1))
    t3 = time.clock()
    hive_unit.execute(r"""
    DROP TABLE IF EXISTS da_dev.purchase_tagging_temp1;
    create table da_dev.purchase_tagging_temp1(
        account_id int,
        last_180D_purchase int,
        last_180D_purchase_amount string,
        purchase_ranking int,
        skincare_item int,
        skincare_revenue string,
        makeup_item int,
        makeup_revenue string,
        fragrance_item int,
        fragrance_revenue string
    ) ;
    insert into da_dev.purchase_tagging_temp1(
        account_id,last_180D_purchase,last_180D_purchase_amount,purchase_ranking
        ,skincare_item,skincare_revenue
        ,makeup_item,makeup_revenue
        ,fragrance_item,fragrance_revenue
    )
    select tt1.account_id,last_180D_purchase,last_180D_purchase_amount,purchase_ranking
    ,tt2.qtys as skincare_item,tt2.sales as skincare_revenue
    ,tt3.qtys as makeup_item ,tt3.sales as makeup_revenue
    ,tt4.qtys as fragrance_item ,tt4.sales as fragrance_revenue
    from(
        select account_id,count(distinct trans_id) as last_180D_purchase,sum(sales) as last_180D_purchase_amount
        ,row_number() over(order by sum(sales) desc) purchase_ranking
        from  crm.fact_trans
        where trans_time between DATE_ADD(CURRENT_DATE,-181) and DATE_ADD(CURRENT_DATE,-1)
        and account_id<>0
        group by account_id
    )tt1
    left join(
        select account_id,t2.category1,sum(qtys) qtys,sum(sales) sales
        from  crm.fact_trans t1
        left join da_dev.product_combine t2
        on t1.product_id=t2.product_id
        where trans_time between DATE_ADD(CURRENT_DATE,-181) and DATE_ADD(CURRENT_DATE,-1)
        and t2.category1 ='Makeup' --美妆
        group by account_id,t2.category1
    )tt2
    on tt1.account_id=tt2.account_id
    left join(
        select account_id,t2.category1,sum(qtys) qtys,sum(sales) sales
        from  crm.fact_trans t1
        left join da_dev.product_combine t2
        on t1.product_id=t2.product_id
        where trans_time between DATE_ADD(CURRENT_DATE,-181) and DATE_ADD(CURRENT_DATE,-1)
        and t2.category1 ='Skincare' --护肤
        group by account_id,t2.category1
    )tt3
    on tt1.account_id=tt3.account_id
    left join(
        select account_id,t2.category1,sum(qtys) qtys,sum(sales) sales
        from  crm.fact_trans t1
        left join da_dev.product_combine t2
        on t1.product_id=t2.product_id
        where trans_time between DATE_ADD(CURRENT_DATE,-181) and DATE_ADD(CURRENT_DATE,-1)
        and t2.category1 ='Fragraces' --香水
        group by account_id,t2.category1
    )tt4
    on tt1.account_id=tt4.account_id
    """)
    t4 = time.clock()
    print('Purchase temp1 Done: %f' % (t4 - t3))
    t5 = time.clock()
    hive_unit.execute(r"""
    DROP TABLE IF EXISTS da_dev.purchase_tagging_temp2;
    create table da_dev.purchase_tagging_temp2(
        account_id int,
        last_30D_purchase int,
        last_30D_purchase_amount string,
        last_90D_purchase int,
        last_90D_purchase_amount string
    ) ;
    insert into da_dev.purchase_tagging_temp2(
        account_id,last_30D_purchase,last_30D_purchase_amount
                  ,last_90D_purchase,last_90D_purchase_amount
    )
    select t1.account_id,t2.last_30D_purchase,t2.last_30D_purchase_amount
                        ,t1.last_90D_purchase,t1.last_90D_purchase_amount
    from(
        select account_id,count(distinct trans_id) as last_90D_purchase,sum(sales) as last_90D_purchase_amount
        from  crm.fact_trans
        where trans_time between DATE_ADD(CURRENT_DATE,-91) and DATE_ADD(CURRENT_DATE,-1)
        group by account_id
    )t1
    left join (
        select account_id,count(distinct trans_id) as last_30D_purchase,sum(sales) as last_30D_purchase_amount
        from  crm.fact_trans
        where trans_time between DATE_ADD(CURRENT_DATE,-31) and DATE_ADD(CURRENT_DATE,-1)
        group by account_id
    )t2
    on t1.account_id=t2.account_id ;
    """)
    t6 = time.clock()
    print('Purchase temp2 Done: %f' % (t6 - t5))
    t7 = time.clock()
    hive_unit.execute(r"""
    DROP TABLE IF EXISTS da_dev.purchase_tagging_temp3;
    create table da_dev.purchase_tagging_temp3(
        account_id int,
        last_180D_purchase int,
        last_180D_purchase_amount string,
        purchase_ranking int,
        skincare_item int,
        skincare_revenue string,
        makeup_item int,
        makeup_revenue string,
        fragrance_item int,
        fragrance_revenue string,
        last_30D_purchase int,
        last_30D_purchase_amount string,
        last_90D_purchase int,
        last_90D_purchase_amount string
    ) ;
    insert into da_dev.purchase_tagging_temp3(
        account_id,last_180D_purchase,last_180D_purchase_amount,purchase_ranking
        ,skincare_item,skincare_revenue
        ,makeup_item,makeup_revenue
        ,fragrance_item,fragrance_revenue
        ,last_30D_purchase,last_30D_purchase_amount
        ,last_90D_purchase,last_90D_purchase_amount
    )
    select t1.account_id,last_180D_purchase,last_180D_purchase_amount,purchase_ranking
        ,skincare_item,skincare_revenue
        ,makeup_item,makeup_revenue
        ,fragrance_item,fragrance_revenue
        ,last_30D_purchase,last_30D_purchase_amount
        ,last_90D_purchase,last_90D_purchase_amount
    from da_dev.purchase_tagging_temp1 t1
    left join da_dev.purchase_tagging_temp2 t2
    on t1.account_id=t2.account_id ;
    """)
    t8 = time.clock()
    print('Purchase temp3 Done: %f' % (t8 - t7))
    t9 = time.clock()
    hive_unit.execute(r"""
    drop table if exists da_dev.purchase_tagging_temp4;
    create table da_dev.purchase_tagging_temp4
    (
    account_id int,
    campaign_participation int,
    last_180D_promotion_amount float
    );
    insert into table da_dev.purchase_tagging_temp4
    select account_id,count(distinct trans_id) as campaign_participation,
    sum(sales) as last_180D_promotion_amount
    from
    (
        select account_id,trans_id,sales,product_id,qtys
        from crm.fact_trans
        where account_id<>0
        and to_date(trans_time) between date_sub(current_date,181) and date_sub(current_date,1)
        and sales>0 and sales/qtys<20000
    ) t1 left outer join
    crm.dim_product t2 on t1.product_id=t2.product_id
    where t1.sales/t1.qtys<t2.price
    group by account_id
    """)
    t10 = time.clock()
    print('Purchase temp4 Done: %f' % (t10 - t9))
    t15 = time.clock()
    hive_unit.execute(r"""
         DROP TABLE IF EXISTS da_dev.purchase_tagging; 
         create table da_dev.purchase_tagging(
            sephora_id string,
            card_number string,
            last_180D_purchase int,
            last_180D_purchase_amount string,
            purchase_ranking int,
            skincare_item int,
            skincare_revenue string,
            makeup_item int,
            makeup_revenue string,
            fragrance_item int,
            fragrance_revenue string,
            last_30D_purchase int,
            last_30D_purchase_amount string,
            last_90D_purchase int,
            last_90D_purchase_amount string,
            channel_preference string,
            promotion_rate float,
            id int,
            union_id string
        ) ;
        insert into da_dev.purchase_tagging
        select case when sephora_id='null' then null when sephora_id ='NULL' then null when sephora_id='' then null else sephora_id end as sephora_id
        ,case when card_number='null' then null when card_number ='NULL' then null when card_number='' then null else card_number end as card_number
        ,case when last_180D_purchase='null' then null when last_180D_purchase ='NULL' then null when last_180D_purchase='' then null else last_180D_purchase end as last_180D_purchase
        ,case when last_180D_purchase_amount='null' then null when last_180D_purchase_amount ='NULL' then null when last_180D_purchase_amount='' then null else last_180D_purchase_amount end as last_180D_purchase_amount
        ,case when purchase_ranking ='null' then null when purchase_ranking  ='NULL' then null when purchase_ranking ='' then null else purchase_ranking  end as purchase_ranking 
        ,case when skincare_item ='null' then null when skincare_item  ='NULL' then null when skincare_item ='' then null else skincare_item  end as skincare_item 
        ,case when skincare_revenue ='null' then null when skincare_revenue  ='NULL' then null when skincare_revenue ='' then null else skincare_revenue  end as skincare_revenue 
        ,case when makeup_item ='null' then null when makeup_item  ='NULL' then null when makeup_item ='' then null else makeup_item  end as makeup_item 
        ,case when makeup_revenue ='null' then null when makeup_revenue  ='NULL' then null when makeup_revenue ='' then null else makeup_revenue  end as makeup_revenue 
        ,case when fragrance_item ='null' then null when fragrance_item  ='NULL' then null when fragrance_item ='' then null else fragrance_item  end as fragrance_item 
        ,case when fragrance_revenue ='null' then null when fragrance_revenue  ='NULL' then null when fragrance_revenue ='' then null else fragrance_revenue  end as fragrance_revenue 
        ,case when last_30D_purchase ='null' then null when last_30D_purchase  ='NULL' then null when last_30D_purchase ='' then null else last_30D_purchase  end as last_30D_purchase 
        ,case when last_30D_purchase_amount ='null' then null when last_30D_purchase_amount  ='NULL' then null when last_30D_purchase_amount ='' then null else last_30D_purchase_amount  end as last_30D_purchase_amount 
        ,case when last_90D_purchase ='null' then null when last_90D_purchase  ='NULL' then null when last_90D_purchase ='' then null else last_90D_purchase  end as last_90D_purchase 
        ,case when last_90D_purchase_amount='null' then null when last_90D_purchase_amount ='NULL' then null when last_90D_purchase_amount='' then null else last_90D_purchase_amount end as last_90D_purchase_amount
        ,case when channel_preference='null' then null when channel_preference ='NULL' then null when channel_preference='' then null else channel_preference end as channel_preference
        ,case when promotion_rate='null' then null when promotion_rate ='NULL' then null when promotion_rate='' then null else promotion_rate end as promotion_rate
        ,row_number() over(order by concat(sephora_id,',',card_number)) as id
        ,case when (case when sephora_id=''  and card_number<>'' then card_number 
        when sephora_id<>'' and card_number='' then sephora_id 
        when sephora_id<>'' and card_number<>'' then concat(sephora_id,',',card_number) 
        else null end)='null' then null 
        when (case when sephora_id=''  and card_number<>'' then card_number 
        when sephora_id<>'' and card_number='' then sephora_id 
        when sephora_id<>'' and card_number<>'' then concat(sephora_id,',',card_number) 
        else null end) ='NULL' then null
        when (case when sephora_id=''  and card_number<>'' then card_number 
        when sephora_id<>'' and card_number='' then sephora_id 
        when sephora_id<>'' and card_number<>'' then concat(sephora_id,',',card_number) 
        else null end)='' then null 
        else (case when sephora_id=''  and card_number<>'' then card_number 
        when sephora_id<>'' and card_number='' then sephora_id 
        when sephora_id<>'' and card_number<>'' then concat(sephora_id,',',card_number) 
        else null end) end as union_id
        from(
        select case when t1.user_id is null or t1.user_id in ('null','NULL',' ','') then '' else t1.user_id end as sephora_id
        ,case when t1.card_no is null or t1.card_no in ('null','NULL',' ','') then '' else t1.card_no end  as card_number
        ,last_180D_purchase,last_180D_purchase_amount,purchase_ranking ,skincare_item 
        ,skincare_revenue ,makeup_item ,makeup_revenue ,fragrance_item ,fragrance_revenue ,last_30D_purchase 
        ,last_30D_purchase_amount ,last_90D_purchase ,last_90D_purchase_amount,channel_preference
        ,last_180D_promotion_amount/last_180D_purchase_amount as promotion_rate
        from 
        (
            select user_id,card_no,mobile
            from oms.ods_user_profile
            where dt=date_sub(current_date,1)
            and  (user_id rlike '^\\d+$' or user_id is null or user_id in ('null','NULL',' ',''))
            and  (card_no rlike '^\\d+$' or card_no is null or card_no in ('null','NULL',' ',''))
        ) t1 left outer join 
        (
            select account_id,account_number,city_name as crm_city
            from crm.dim_account
        )t2 on t1.card_no=t2.account_number left outer join
        da_dev.purchase_tagging_temp3 t3 on t2.account_id=t3.account_id
        left outer join
        (
            select account_id,
            case when t2.store_id=3 and t2.sub_store_id=1231 then 'APP'
            when t2.store_id=3 and t2.sub_store_id=1192 then 'MP'
            when t2.store_id=254 and t2.sub_store_id=254 then 'Tmall'
            when t2.store_id=1017 and t2.sub_store_id=999 then 'JD'
            when t2.store_id=1019 and t2.sub_store_id=1231 then 'APP'
            when t2.store_id=3 and t2.sub_store_id=1232 then 'APP'
            when t2.store_id=1019 and t2.sub_store_id=1192 then 'MP'
            when t2.store_id=1087 and t2.sub_store_id=254 then 'Tmall'
            when t2.store_id=1019 and t2.sub_store_id=1232 then 'APP'
            when t2.store_id=3 and t2.sub_store_id=1202 then 'O2O'
            when t2.store_id=1281 and t2.sub_store_id=1281 then 'O2O'
            when t2.store_id=3 and t2.sub_store_id=1200 then 'Web'
            when t2.store_id=3 and t2.sub_store_id=1199 then 'Web'
            when t2.store_id=1019 and t2.sub_store_id=1202 then 'O2O'
            when t2.store_id=1211 and t2.sub_store_id=999 then 'JD'
            when t2.store_id=1019 and t2.sub_store_id=1199 then 'Web'
            when t2.store_id=1019 and t2.sub_store_id=1200 then 'Web'
            when t2.store_id=1283 and t2.sub_store_id=1283 then 'Others'
            when t2.store_id=3 and t2.sub_store_id is null then 'Others'
            when t2.store_id=3 and t2.sub_store_id=1169 then 'Others'
            when t2.store_id=1019 and t2.sub_store_id is null then 'Others'
            when t2.store_id=1019 and t2.sub_store_id=1169 then 'Others'
            when t2.store_id=254 and t2.sub_store_id is null then 'Others'
            when t2.store_id=1017 and t2.sub_store_id is null then 'Others'
            else 'Others' end as channel_preference
            from(
                select account_id,store_id,sub_store_id
                from(
                select account_id,store_id,sub_store_id
                ,row_number() over(partition by account_id order by tt_sales desc) as rn
                from (
                    select account_id,store_id,sub_store_id,sum(total_sales) as tt_sales
                    from crm.dim_trans
                    where account_id<>0 and total_sales/total_qtys<20000  and total_qtys>0
                    group by account_id,store_id,sub_store_id
                )t1 )tt1 where rn=1)t2 left outer join crm.dim_store t3 on t2.store_id=t3.store_id
        )t4 on t2.account_id=t4.account_id
        left outer join
        da_dev.purchase_tagging_temp4 t5 on t2.account_id=t5.account_id)tt1
        where not (tt1.sephora_id='' and tt1.card_number='')
        """)
    t16 = time.clock()
    print('Purchase Tagging Done: %f' % (t16 - t15))


def run_user_purchase_info():
    hive_unit = HiveUnit(**HIVE_CONFIG)
    user_purchase(hive_unit)
    hive_unit.release()
