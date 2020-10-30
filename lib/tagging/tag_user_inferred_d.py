import time

from lib.datastructure.config import HIVE_CONFIG
from lib.db.hive_utils import HiveUnit


def user_infer(hive_unit: HiveUnit):
    t1 = time.clock()
    hive_unit.execute(r"""
    DROP TABLE IF EXISTS da_dev.inferred_tagging_temp;
    create table da_dev.inferred_tagging_temp(
        account_id int,
        preferred_category string,
        preferred_Subcategory string,
        preferred_thirdcategory string,
        preferred_brand string,
        skin_type string,
        makeup_maturity string,
        skincare_maturity string,
        makeup_price_range string,
        skincare_price_range string
    ) ;
    insert into da_dev.inferred_tagging_temp(
        account_id,preferred_category,preferred_Subcategory,preferred_thirdcategory
        ,preferred_brand,skin_type
        ,makeup_maturity,skincare_maturity
        ,makeup_price_range,skincare_price_range
    )
    select ttt1.account_id,preferred_category,preferred_Subcategory,preferred_thirdcategory
        ,preferred_brand,skin_type
        ,case when makeupsucnt>5 then 'Level1'
         when makeupsucnt>3 then 'Level2'
         when makeupsucnt>=1 then 'Level3'
         end as makeup_maturity
         ,case when skincaresucnt>5 then 'Level1'
         when skincaresucnt>3 then 'Level2'
         when skincaresucnt>=1 then 'Level3'
         end as skincare_maturity
        ,makeup_price_range,skincare_price_range
    from(
        select  distinct account_id
        ,COALESCE(preferred_category,LAST_VALUE(preferred_category,true) over(partition by account_id order by preferred_category desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) preferred_category
        ,COALESCE(preferred_Subcategory,LAST_VALUE(preferred_Subcategory,true) over(partition by account_id order by preferred_Subcategory desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) preferred_Subcategory
        ,COALESCE(preferred_thirdcategory,LAST_VALUE(preferred_thirdcategory,true) over(partition by account_id order by preferred_thirdcategory desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) preferred_thirdcategory
        ,COALESCE(preferred_brand,LAST_VALUE(preferred_brand,true) over(partition by account_id order by preferred_brand desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) preferred_brand
        ,COALESCE(skin_type,LAST_VALUE(skin_type,true) over(partition by account_id order by skin_type desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) skin_type
        from(----更偏好的品类/品牌/肤质
            select account_id
            ,case when casales=max(casales) over(partition by account_id) then category1 else null end preferred_category
            ,case when susales=max(susales) over(partition by account_id) then category2 else null end preferred_Subcategory
            ,case when thsales=max(thsales) over(partition by account_id) then category3 else null end preferred_thirdcategory
            ,case when brandsales=max(brandsales) over(partition by account_id) then brand else null end preferred_brand
            ,case when skinsales=max(skinsales) over(partition by account_id) then skin_type else null end skin_type
            from(
                select account_id,t2.category1,t2.category2,t2.category3,t2.brand,t2.skin_type
                ,case when t2.category1 is null then null else sum(sales) over(partition by account_id,t2.category1) end casales
                ,case when t2.category2 is null then null else sum(sales) over(partition by account_id,t2.category2) end susales
                ,case when t2.category3 is null then null else sum(sales) over(partition by account_id,t2.category3) end thsales
                ,case when t2.brand is null then null else sum(sales) over(partition by account_id,t2.brand) end brandsales
                ,case when t2.skin_type is null then null else sum(sales) over(partition by account_id,t2.skin_type) end skinsales
                from  crm.fact_trans t1
                left join
                ( select op_code,product_id,sku_code,category1,category2,category3,price,price_level,brand,skincare_function_basic
                    ,skincare_function_special,makeup_function,makeup_feature_scene,makeup_feature_look,makeup_feature_color
                    ,fragrance_stereotype,fragrance_intensity,fragrance_impression,if_gift
                    ,case when skin_type='各种肤质' then null else skin_type end as skin_type
                from da_dev.product_combine) t2
                on t1.product_id=t2.product_id
            )tt1
        )temp
    )ttt1
    left join(----makeup下的二级类个数
        select account_id,t2.category1,count(distinct t2.category2) as makeupsucnt
        from  crm.fact_trans t1
        left join da_dev.product_combine t2
        on t1.product_id=t2.product_id
        where t2.category1='Makeup'
        group by account_id,t2.category1
    )ttt2
    on ttt1.account_id=ttt2.account_id
    left join(----Skincare下的二级类个数
        select account_id,t2.category1,count(distinct t2.category2) as skincaresucnt
        from  crm.fact_trans t1
        left join da_dev.product_combine t2
        on t1.product_id=t2.product_id
        where t2.category1='Skincare'
        group by account_id,t2.category1
    )ttt3
    on ttt1.account_id=ttt3.account_id
    left join(
        select account_id,price_level as makeup_price_range
        from(
            select account_id,t2.price_level,count(distinct t2.product_id) as makeupprocnt
            ,row_number() over(partition by account_id order by count(distinct t2.product_id) desc) rn
            from  crm.fact_trans t1
            left join da_dev.product_combine t2
            on t1.product_id=t2.product_id
            where t2.category1='Makeup'
            group by account_id,t2.price_level
        )tt1
        where rn=1
    )ttt4
    on ttt1.account_id=ttt4.account_id
    left join(
        select account_id,price_level as skincare_price_range
        from(
            select account_id,t2.price_level,count(distinct t2.product_id) as skincareprocnt
            ,row_number() over(partition by account_id order by count(distinct t2.product_id) desc) rn
            from  crm.fact_trans t1
            left join da_dev.product_combine t2
            on t1.product_id=t2.product_id
            where t2.category1='Skincare'
            group by account_id,t2.price_level
        )tt1
        where rn=1
    )ttt5
    on ttt1.account_id=ttt5.account_id ;""")
    t2 = time.clock()
    print('Inferr temp Done: %f' % (t2 - t1))
    t3 = time.clock()
    hive_unit.execute(r"""
    DROP TABLE IF EXISTS da_dev.inferred_tagging_temp2;
    create table da_dev.inferred_tagging_temp2(
         account_id int,
         preferred_category string,
         preferred_Subcategory string,
         preferred_thirdcategory string,
         preferred_brand string,
         skin_type string,
         makeup_maturity string,
         skincare_maturity string,
         makeup_price_range string,
         skincare_price_range string,
         skincare_demand string,
         makeup_demand string,
         fragrance_demand string
    ) ;
    insert into da_dev.inferred_tagging_temp2(
     account_id,preferred_category,preferred_Subcategory,preferred_thirdcategory
     ,preferred_brand,skin_type
     ,makeup_maturity,skincare_maturity
     ,makeup_price_range,skincare_price_range
     ,skincare_demand,makeup_demand,fragrance_demand
    )
    select ttt1.account_id,preferred_category,preferred_Subcategory,preferred_thirdcategory
        ,preferred_brand,skin_type
        ,makeup_maturity,skincare_maturity
        ,makeup_price_range,skincare_price_range
        ,skincare_demand,makeup_demand,fragrance_demand
    from da_dev.inferred_tagging_temp ttt1
    left join(
        select account_id,concat(skincare_function_basic,',',skincare_function_special) as skincare_demand
        ,concat(makeup_function,',',makeup_feature_scene,',',makeup_feature_look,',',makeup_feature_color) as makeup_demand
        ,concat(fragrance_stereotype,',',fragrance_intensity,',',fragrance_impression) as fragrance_demand
        from(
            select  distinct account_id
            ,COALESCE(skincare_function_basic,LAST_VALUE(skincare_function_basic,true) over(partition by account_id order by skincare_function_basic desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) skincare_function_basic
            ,COALESCE(skincare_function_special,LAST_VALUE(skincare_function_special,true) over(partition by account_id order by skincare_function_special desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) skincare_function_special
            ,COALESCE(makeup_function,LAST_VALUE(makeup_function,true) over(partition by account_id order by makeup_function desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) makeup_function
            ,COALESCE(makeup_feature_scene,LAST_VALUE(makeup_feature_scene,true) over(partition by account_id order by makeup_feature_scene desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) makeup_feature_scene
            ,COALESCE(makeup_feature_look,LAST_VALUE(makeup_feature_look,true) over(partition by account_id order by makeup_feature_look desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) makeup_feature_look
            ,COALESCE(makeup_feature_color,LAST_VALUE(makeup_feature_color,true) over(partition by account_id order by makeup_feature_color desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) makeup_feature_color
            ,COALESCE(fragrance_stereotype,LAST_VALUE(fragrance_stereotype,true) over(partition by account_id order by fragrance_stereotype desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) fragrance_stereotype
            ,COALESCE(fragrance_intensity,LAST_VALUE(fragrance_intensity,true) over(partition by account_id order by fragrance_intensity desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) fragrance_intensity
            ,COALESCE(fragrance_impression,LAST_VALUE(fragrance_impression,true) over(partition by account_id order by fragrance_impression desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) fragrance_impression
            from(
                select account_id
                ,case when skincarebasicsales=max(skincarebasicsales) over(partition by account_id) then skincare_function_basic else null end skincare_function_basic
                ,case when skincarespecialsales=max(skincarespecialsales) over(partition by account_id) then skincare_function_special else null end skincare_function_special
                ,case when makeupsales=max(makeupsales) over(partition by account_id) then makeup_function else null end makeup_function
                ,case when makeupscenesales=max(makeupscenesales) over(partition by account_id) then makeup_feature_scene else null end makeup_feature_scene
                ,case when makeuplooksales=max(makeuplooksales) over(partition by account_id) then makeup_feature_look else null end makeup_feature_look
                ,case when makeupcolorsales=max(makeupcolorsales) over(partition by account_id) then makeup_feature_color else null end makeup_feature_color
                ,case when fragrancestereotype=max(fragrancestereotype) over(partition by account_id) then fragrance_stereotype else null end fragrance_stereotype
                ,case when fragranceintensitysales=max(fragranceintensitysales) over(partition by account_id) then fragrance_intensity else null end fragrance_intensity
                ,case when fragranceimpressionsales=max(fragranceimpressionsales) over(partition by account_id) then fragrance_impression else null end fragrance_impression
                from(
                    select account_id,t2.skincare_function_basic,t2.skincare_function_special
                    ,t2.makeup_function,t2.makeup_feature_scene,t2.makeup_feature_look,t2.makeup_feature_color
                    ,t2.fragrance_stereotype,t2.fragrance_intensity,t2.fragrance_impression
                    ,case when t2.skincare_function_basic is null then null else sum(sales) over(partition by account_id,t2.skincare_function_basic) end skincarebasicsales
                    ,case when t2.skincare_function_special is null then null else sum(sales) over(partition by account_id,t2.skincare_function_special) end skincarespecialsales
                    ,case when t2.makeup_function is null then null else sum(sales) over(partition by account_id,t2.makeup_function) end makeupsales
                    ,case when t2.makeup_feature_scene is null then null else sum(sales) over(partition by account_id,t2.makeup_feature_scene) end makeupscenesales
                    ,case when t2.makeup_feature_look is null then null else sum(sales) over(partition by account_id,t2.makeup_feature_look) end makeuplooksales
                    ,case when t2.makeup_feature_color is null then null else sum(sales) over(partition by account_id,t2.makeup_feature_color) end makeupcolorsales
                    ,case when t2.fragrance_stereotype is null then null else sum(sales) over(partition by account_id,t2.fragrance_stereotype) end fragrancestereotype
                    ,case when t2.fragrance_intensity is null then null else sum(sales) over(partition by account_id,t2.fragrance_intensity) end fragranceintensitysales
                    ,case when t2.fragrance_impression is null then null else sum(sales) over(partition by account_id,t2.fragrance_impression) end fragranceimpressionsales
                    from  crm.fact_trans t1
                    left join da_dev.product_combine t2
                    on t1.product_id=t2.product_id
                )tt1
            )temp1
        )temp2
    )ttt2
    on ttt1.account_id=ttt2.account_id ;
    """)
    t4 = time.clock()
    print('Inferr temp2 Done: %f' % (t4 - t3))
    t11 = time.clock()
    hive_unit.execute(r"""
            drop table if exists  da_dev.inferred_tagging_temp3;
            create table da_dev.inferred_tagging_temp3
            (
                account_id int,
                seasonal_share float,
                festival_share float,
                promotion_share float,
                gifting_share float,
                exclusive_share float
            );
            insert into table  da_dev.inferred_tagging_temp3
            select t1.account_id
            ,t2.seasonal_sales/t1.last_360d_purchase_amount as seasonal_share
            ,t3.festival_sales/t1.last_360d_purchase_amount as festival_share
            ,t4.promotion_sales/t1.last_360d_purchase_amount as promotion_share
            ,t5.gift_sales/t1.last_360d_purchase_amount as gifting_share
            ,t6.exclusive_sales/t1.last_360d_purchase_amount as exclusive_share
            from (
                select account_id,sum(total_sales) as last_360d_purchase_amount
                from crm.dim_trans
                where to_date(trans_time) between date_sub(current_date,360) and date_sub(current_date,1)
                and total_sales>0 and total_sales/total_qtys<20000 and total_qtys>0
                and account_id<>0
                group by account_id
            )t1 left outer join
            (
                select account_id,sum(total_sales) as seasonal_sales
                from crm.dim_trans
                where to_date(trans_time) between date_sub(current_date,360) and date_sub(current_date,1)
                and total_sales>0 and total_sales/total_qtys<20000 and total_qtys>0
                and account_id<>0
                and month(to_date(trans_time)) in (3,6,9,12)
                group by account_id
            )t2 on t1.account_id=t2.account_id
            left outer join
            (
                select account_id,sum(total_sales) as festival_sales
                from crm.dim_trans
                where to_date(trans_time) between date_sub(current_date,360) and date_sub(current_date,1)
                and total_sales>0 and total_sales/total_qtys<20000 and total_qtys>0
                and account_id<>0
                and (to_date(trans_time) between date_sub('2020-01-01',7) and date_add('2020-01-01',7)--New Year
                or to_date(trans_time) between date_sub('2020-02-14',7) and date_add('2020-02-14',7)--Valentine
                or to_date(trans_time)  between date_sub('2020-08-25',7) and date_add('2020-08-25',7)--Valentine
                or to_date(trans_time) between date_sub('2019-12-25',7) and date_add('2019-12-25',7)--Christmas
                )
                group by account_id
            )t3 on t1.account_id=t3.account_id
            left outer join
            (
                select account_id,
                sum(sales) as promotion_sales
                from
                (
                    select account_id,trans_id,sales,product_id,qtys
                    from crm.fact_trans
                    where account_id<>0
                    and to_date(trans_time) between date_sub(current_date,360) and date_sub(current_date,1)
                    and sales>0 and sales/qtys<20000 and qtys>0
                ) t1 left outer join
                crm.dim_product t2 on t1.product_id=t2.product_id
                where t1.sales/t1.qtys<t2.price
                group by account_id
            )t4 on t1.account_id=t4.account_id
            left outer join
            (
                select
                account_id,
                sum(sales) as gift_sales
                from
                (
                    select account_id,trans_id,sales,product_id,qtys
                    from crm.fact_trans
                    where account_id<>0
                    and to_date(trans_time) between date_sub(current_date,360) and date_sub(current_date,1)
                    and sales>0 and sales/qtys<20000 and qtys>0
                ) t1 left outer join
                da_dev.product_combine t2 on t1.product_id=t2.product_id
                where t2.if_gift=0
                group by account_id
            )t5 on t1.account_id=t5.account_id
            left outer join
            (
                select account_id,
                sum(sales) as exclusive_sales
                from
                (
                    select account_id,trans_id,sales,product_id,qtys
                    from crm.fact_trans
                    where account_id<>0
                    and to_date(trans_time) between date_sub(current_date,360) and date_sub(current_date,1)
                    and sales>0 and sales/qtys<20000 and qtys>0
                ) t1 left outer join
                crm.dim_product t2 on t1.product_id=t2.product_id
                where t2.brand_type='EXCLUSIVE'
                group by account_id
            )t6 on t1.account_id=t6.account_id
            where t1.last_360d_purchase_amount>0
            """)
    t12 = time.clock()
    print('Inferr temp3 Done: %f' % (t12 - t11))
    t13 = time.clock()
    hive_unit.execute(r"""
            drop table if exists da_dev.inferred_tagging_temp4;
            create table da_dev.inferred_tagging_temp4
            (
                account_id int,
                shopping_driver string
            );
            insert into table da_dev.inferred_tagging_temp4
            select t1.account_id
            ,case when t1.gifting_share=t2.max_vol then 'Gifting'
            when t1.festival_share=t2.max_vol then 'Festival'
            when t1.seasonal_share=t2.max_vol then 'Seasonality'
            when t1.exclusive_share=t2.max_vol then 'Exclusivity'
            when t1.promotion_share=t2.max_vol then 'Promotion'
            else 'Regular' end as shopping_driver
            from da_dev.inferred_tagging_temp3 t1
            left outer join
            (
                select account_id,sort_arr[4] as max_vol
                from(
                select account_id,sort_array(array(seasonal_share,festival_share,promotion_share,gifting_share,exclusive_share)) as sort_arr
                from da_dev.inferred_tagging_temp3
            )t1)t2 on t1.account_id=t2.account_id
        """)
    t14 = time.clock()
    print('Inferr temp4 Done: %f' % (t14 - t13))
    t5 = time.clock()
    hive_unit.execute(r"""
        DROP TABLE IF EXISTS da_dev.inferred_tagging;
        create table da_dev.inferred_tagging(
             sephora_id string,
             card_number string,
             preferred_category string,
             preferred_Subcategory string,
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
             id int,
             union_id string
        ) ;
        insert into da_dev.inferred_tagging
        select case when sephora_id='null' then null when sephora_id ='NULL' then null when sephora_id='' then null else sephora_id end as sephora_id
        ,case when card_number='null' then null when card_number ='NULL' then null when card_number='' then null else card_number end as card_number
        ,case when preferred_category='null' then null when preferred_category ='NULL' then null when preferred_category='' then null else preferred_category end as preferred_category
        ,case when preferred_Subcategory='null' then null when preferred_Subcategory ='NULL' then null when preferred_Subcategory='' then null else preferred_Subcategory end as preferred_Subcategory
        ,case when preferred_thirdcategory='null' then null when preferred_thirdcategory ='NULL' then null when preferred_thirdcategory='' then null else preferred_thirdcategory end as preferred_thirdcategory
        ,case when preferred_brand='null' then null when preferred_brand ='NULL' then null when preferred_brand='' then null else preferred_brand end as preferred_brand
        ,case when skin_type='null' then null when skin_type ='NULL' then null when skin_type='' then null else skin_type end as skin_type
        ,case when makeup_maturity='null' then null when makeup_maturity ='NULL' then null when makeup_maturity='' then null else makeup_maturity end as makeup_maturity
        ,case when skincare_maturity='null' then null when skincare_maturity ='NULL' then null when skincare_maturity='' then null else skincare_maturity end as skincare_maturity
        ,case when makeup_price_range='null' then null when makeup_price_range ='NULL' then null when makeup_price_range='' then null else makeup_price_range end as makeup_price_range
        ,case when skincare_price_range='null' then null when skincare_price_range ='NULL' then null when skincare_price_range='' then null else skincare_price_range end as skincare_price_range
        ,case when skincare_demand='null' then null when skincare_demand ='NULL' then null when skincare_demand='' then null else skincare_demand end as skincare_demand
        ,case when makeup_demand='null' then null when makeup_demand ='NULL' then null when makeup_demand='' then null else makeup_demand end as makeup_demand
        ,case when fragrance_demand='null' then null when fragrance_demand ='NULL' then null when fragrance_demand='' then null else fragrance_demand end as fragrance_demand
        ,case when shopping_driver='null' then null when shopping_driver ='NULL' then null when shopping_driver='' then null else shopping_driver end as shopping_driver
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
        select 
          case when t1.user_id is null or t1.user_id in ('null','NULL',' ','') then '' else t1.user_id end as sephora_id
         ,case when t1.card_no is null or t1.card_no in ('null','NULL',' ','') then '' else t1.card_no end  as card_number
         ,preferred_category,preferred_Subcategory,preferred_thirdcategory
         ,preferred_brand,skin_type
         ,makeup_maturity,skincare_maturity
         ,makeup_price_range,skincare_price_range
         ,skincare_demand,makeup_demand,fragrance_demand
         ,t4.shopping_driver
         
        from
        (
            select user_id,card_no,mobile
            from oms.ods_user_profile
            where dt = date_sub(current_date,1)
            and  (user_id rlike '^\\d+$' or user_id is null or user_id in ('null','NULL',' ',''))
            and  (card_no rlike '^\\d+$' or card_no is null or card_no in ('null','NULL',' ',''))
        ) t1 left outer join 
        (
            select account_id,account_number,city_name as crm_city
            from crm.dim_account
        )t2 on t1.card_no=t2.account_number left outer join
        da_dev.inferred_tagging_temp2 t3 on t2.account_id=t3.account_id
        left outer join
        da_dev.inferred_tagging_temp4 t4 on t2.account_id=t4.account_id)tt1
        where not (tt1.sephora_id='' and tt1.card_number='')
    """)
    t6 = time.clock()
    print('Inferr Done: %f' % (t6 - t5))


def run_user_inferred_info():
    hive_unit = HiveUnit(**HIVE_CONFIG)
    user_infer(hive_unit)
    hive_unit.release()
