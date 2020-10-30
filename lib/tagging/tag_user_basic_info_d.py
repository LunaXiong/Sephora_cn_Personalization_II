import time

from lib.datastructure.config import HIVE_CONFIG
from lib.db.hive_utils import HiveUnit


def user_basic_city(hive_unit: HiveUnit):
    t1 = time.clock()
    hive_unit.execute(r"""
    drop table if exists da_dev.user_basic_info_tagging_login_city;
    create table da_dev.user_basic_info_tagging_login_city
    (
        sephora_id string,
        login_city string
    );
    insert into table da_dev.user_basic_info_tagging_login_city
    select sephora_id,login_city
    from (
            select sephora_id,login_city
                ,row_number() over(partition by sephora_id order by dt_cnt desc) as rn
            from (
                    select t1.sephora_id,t2.login_city,sum(dt_cnt) as dt_cnt
                    from  da_dev.iris_app_user_id_mapping t1
                        left outer join 
                        (
                            select user_id,do_city as login_city,count(distinct dt) as dt_cnt
                            from(
                                select distinct user_id,dt,do_city
                                from dwd.v_events
                                where to_date(dt) >= date_sub(current_date,90)
                                    and do_city is not null)t1
                            group by user_id,do_city
                        )t2 on t1.sensor_id=t2.user_id
                    where t2.user_id is not null   
                    group by t1.sephora_id,t2.login_city
                    )tt1
            )tt2
    where rn=1 and sephora_id is not null
    """)
    t2 = time.clock()
    print("Login City Done: %f" % (t2 - t1))
    t3 = time.clock()
    hive_unit.execute(r"""
    drop table if exists da_dev.user_basic_info_tagging_order_city;
    create table da_dev.user_basic_info_tagging_order_city
    (
        member_card string,
        mobile string,
        city string,
        district string
    );
    insert into table da_dev.user_basic_info_tagging_order_city
    select member_card,mobile,city,district
    from(
        select member_card,mobile,city,district
        ,row_number() over(partition by member_card order by sales desc) as rn
        from(
        select t1.member_card,t2.mobile,t2.city,t2.district,sum(t1.sales) as sales
        from (
            select sales_order_sys_id,member_card,payed_amount as sales
            from oms.ods_sales_order 
            where dt='current' and payment_status=1
        )t1 left outer join
        (
            select sales_order_sys_id,mobile,city,district
            from oms.ods_sales_order_address 
            where dt = date_sub(current_date,1)
        )t2 where t1.sales_order_sys_id=t2.sales_order_sys_id
        group by t1.member_card,t2.mobile,t2.city,t2.district
        )tt1)tt2
    where rn=1 and member_card is not null
    """)
    t4 = time.clock()
    print("Order City Done: %f" % (t4 - t3))
    t5 = time.clock()
    hive_unit.execute(r"""
    drop table if exists da_dev.user_basic_info_tagging_city;
    create table da_dev.user_basic_info_tagging_city
    (
        sephora_id int,
        card_number int,
        crm_city string,
        order_city_online string,
        order_residential_area_online string,
        order_city_offline string,
        login_city string
    );
    insert into table  da_dev.user_basic_info_tagging_city
    select t1.user_id as sephora_id,t1.card_no as card_number,t2.crm_city,
    case when t3.city is not null then t3.city else t4.city end as order_city_online,
    case when t3.district is not null then t3.district else t4.district end as order_residential_area_online,
    t5.order_city_offline,t6.login_city
    from 
    (
        select user_id,card_no,mobile
        from oms.ods_user_profile
        where dt = date_sub(current_date,1)
    ) t1 left outer join 
    (-- crm city
        select account_id,account_number,city_name as crm_city
        from crm.dim_account
    )t2 on t1.card_no=t2.account_number left outer join
    da_dev.user_basic_info_tagging_order_city t3 on t1.card_no=t3.member_card and t3.member_card<>'null' left outer join
    da_dev.user_basic_info_tagging_order_city t4 on t1.mobile=t4.mobile and t4.mobile<>'null' left outer join
     (-- sales order address
        -- offline 
         select t1.account_id,t2.city as order_city_offline
         from (
            select account_id,store_id
            from(
                select account_id,store_id
                ,row_number() over(partition by account_id order by sales desc) as rn
                from (
                    select account_id,store_id,sum(total_sales) as sales
                    from crm.dim_trans
                    where account_id<>0 and total_sales<>0
                    group by account_id,store_id
                )t1)t2
            where rn=1)t1 left outer join 
            crm.dim_store t2 on t1.store_id=t2.store_id  
     )t5 on t2.account_id=t5.account_id left outer join 
     da_dev.user_basic_info_tagging_login_city t6 on t1.user_id=t6.sephora_id
     """)
    t6 = time.clock()
    print('Combine City Done :%f' % (t6 - t5))


def user_basic(hive_unit: HiveUnit):
    t7 = time.clock()
    hive_unit.execute(r"""
    drop table if exists da_dev.user_basic_info_tagging_eb_status_new_purchase;
    create table  da_dev.user_basic_info_tagging_eb_status_new_purchase
    (
        card_number string,
        customer_status_eb string,
        dt string
    );
    insert into table da_dev.user_basic_info_tagging_eb_status_new_purchase
    select card_number,customer_status_eb,dt
    from
    (
        select t1.member_card as card_number,
        case when is_placed_flag = 0 then 'NULL'
        when all_order_placed_seq = 1 and member_card_grade in ('WHITE', 'BLACK', 'GOLD') then 'CONVERT_NEW'
        when all_order_placed_seq = 1 and member_card_grade not in ('WHITE', 'BLACK', 'GOLD') then 'BRAND_NEW'
        else 'RETURN' end as customer_status_eb,
        to_date(place_time) as dt
        from
        (
        select member_card
        ,if(type = 8,order_time, if(payment_time<> '1970-01-01 00:00:00', payment_time, order_time)) as place_time
        ,if(
                basic_status not in ('DELETE', 'DELETED', 'TELETED')
                and store_id <> 'TMALL002'
                and type not in (2,9)
                and ((payment_status = 1 and payment_time <> '1970-01-01 00:00:00') or type= 8)
                and payed_amount >1,
                1,
                0
            ) as is_placed_flag
        ,rank() over (partition by member_card,if(
                basic_status not in ('DELETE', 'DELETED', 'TELETED')
                and store_id <> 'TMALL002'
                and type not in (2,9)
                and ((payment_status = 1 and payment_time <> '1970-01-01 00:00:00') or type= 8)
                and payed_amount >1,
                1,
                0
              )  order by if(type = 8,order_time, if(payment_time<> '1970-01-01 00:00:00', payment_time, order_time))) as all_order_placed_seq
        ,member_card_grade
        from oms.ods_sales_order
        where dt='current')t1
    )tt1
    where card_number is not null and card_number<>'null' and card_number<>'NULL'
    and customer_status_eb in ('CONVERT_NEW','BRAND_NEW','RETURN')
    """)
    t8 = time.clock()
    print('EB status 1 Done : %f' % (t8 - t7))
    t9 = time.clock()
    hive_unit.execute(r"""
    drop table if exists da_dev.user_basic_info_tagging_eb_status_register;
    create table  da_dev.user_basic_info_tagging_eb_status_register
    (
        card_number string,
        tenure_days int,
        source string,
        category string,
        level string,
        first_purchase_date string,
        register_date string,
        if_eb_purchase int
    );
    insert into table da_dev.user_basic_info_tagging_eb_status_register
    select t1.card_no as card_number,t1.tenure_days,t1.source
    ,case when t2.trans_date=t1.pink_upgrade_time then t2.category else null end as category
    ,t1.level,t1.pink_upgrade_time as first_purchase_date,t1.join_time as register_date
    ,t2.if_eb_purchase
    from
    (
        select card_no,level,source,store_id
            ,to_date(pink_upgrade_time) as pink_upgrade_time
            ,to_date(join_time) as join_time
            ,datediff(current_date,join_time) as tenure_days
        from oms.dwd_card
        where dt = date_sub(current_date,1)
    )t1
    left outer join
    da_dev.user_basic_info_tagging_eb_status_purchase t2 on t1.card_no=t2.card_number
    where card_number is not null
    """)
    t10 = time.clock()
    print('EB status 2 Done: %f' % (t10 - t9))
    t11 = time.clock()
    hive_unit.execute(r"""
    drop table if exists da_dev.user_basic_info_tagging_eb_status_purchase;
    create table da_dev.user_basic_info_tagging_eb_status_purchase
    (
        card_number string,
        trans_date string,
        category string,
        if_eb_purchase int
    );
    insert into table da_dev.user_basic_info_tagging_eb_status_purchase
    select t1.account_number as card_number,t1.trans_date,t1.category
    ,case when t2.account_id is not null then 1 else 0 end as if_eb_purchase
    from
    (
        select t1.account_number,t1.account_id,t2.trans_date,
        case when t4.category is not null then t4.category else t3.category_1 end as category
        from
        crm.dim_account t1 left outer join
        (
                select account_id,trans_date,product_id
                from
                (
                select account_id,to_date(trans_time) as trans_date,product_id,
                    row_number() over(partition by account_id order by trans_time,sales desc) as rn
                from crm.fact_trans
                where account_id<>0 and qtys>0 and sales>0
                )t1 where rn=1)t2 on t1.account_id=t2.account_id
        left outer join
        (
        select product_id,sku_code,
        case when category in ('MAKE UP','MAKE UP ACCESSORIES','MAKE UP TESTER','MAKE-UP SAMPLES','MAKEUP TESTER','GIFT MAKE UP') then 'Makeup'
        when category in ('SKINCARE','SKINCARE ACCESSORIES','SKINCARE DEMO','SKINCARE SAMPLES','SKINCARE TESTER','BATH','BATH & GIFT','BATHCARE TESTER','GIFT SKINCARE','HAIR','HAIR ACCESSORIES','HAIR CARE TESTER','HAIR PRODUCT SAMPLES','HAIRCARE') then 'Skincare'
        when category in ('FRAGRANCE','FRAGRANCE ACCESS','FRAGRANCE TESTER','PERFUME SAMPLES','GIFT FRAGANCE') then 'Fragraces'
        when category in ('WELLNESS') then 'Wellness'
        else 'other_offline' end as category_1
        from crm.dim_product) t3 on t2.product_id=t3.product_id
        left outer join
        da_dev.search_prod_list t4 on t3.sku_code=t4.sku_code
    )t1 left outer join
    (
        select distinct account_id
        from crm.dim_trans
        where store_id in (
            select store_id
            from crm.dim_store
            where is_eb_store=2
        )
    )t2 on t1.account_id=t2.account_id
    where t1.account_number is not null
    """)
    t12 = time.clock()
    print('First Purchase Done: %f' % (t12 - t11))
    t13 = time.clock()
    hive_unit.execute(r"""
        drop table if exists da_dev.user_basic_info_tagging;
        create table da_dev.user_basic_info_tagging
        (
            sephora_id string,
            card_number string,
            age int,
            age_group string,
            gender string,
            city string,
            residential_area string,
            constellation string,
            customer_status_eb string,
            is_member int,
            member_tenure_days int,
            member_tenure_days_group string,
            member_origin_channel string,
            member_origin_category string,
            member_cardtype string,
            id int,
            union_id string
        );
        insert into table da_dev.user_basic_info_tagging
        select case when sephora_id='null' then null when sephora_id ='NULL' then null when sephora_id='' then null else sephora_id end as sephora_id
        ,case when card_number='null' then null when card_number ='NULL' then null when card_number='' then null else card_number end as card_number
        ,case when age='null' then null when age ='NULL' then null when age='' then null else age end as age
        ,case when age_group='null' then null when age_group ='NULL' then null when age_group='' then null else age_group end as age_group
        ,case when gender='null' then null when gender ='NULL' then null when gender='' then null else gender end as gender
        ,case when city='null' then null when city ='NULL' then null when city='' then null else city end as city
        ,case when residential_area='null' then null when residential_area ='NULL' then null when residential_area='' then null else residential_area end as residential_area
        ,case when constellation='null' then null when constellation ='NULL' then null when constellation='' then null else constellation end as constellation
        ,case when customer_status_eb='null' then null when customer_status_eb ='NULL' then null when customer_status_eb='' then null else customer_status_eb end as customer_status_eb
        ,case when is_member='null' then null when is_member ='NULL' then null when is_member='' then null else is_member end as is_member
        ,case when member_tenure_days='null' then null when member_tenure_days ='NULL' then null when member_tenure_days='' then null else member_tenure_days end as member_tenure_days
        ,case when member_tenure_days_group='null' then null when member_tenure_days_group ='NULL' then null when member_tenure_days_group='' then null else member_tenure_days_group end as member_tenure_days_group
        ,case when member_origin_channel='null' then null when member_origin_channel ='NULL' then null when member_origin_channel='' then null else member_origin_channel end as member_origin_channel
        ,case when member_origin_category='null' then null when member_origin_category ='NULL' then null when member_origin_category='' then null else member_origin_category end as member_origin_category
        ,case when member_cardtype='null' then null when member_cardtype ='NULL' then null when member_cardtype='' then null else member_cardtype end as member_cardtype
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
        select  case when tt1.sephora_id is null or tt1.sephora_id in ('null','NULL',' ','') then '' else tt1.sephora_id end as sephora_id,
            case when tt1.card_number is null or tt1.card_number in ('null','NULL',' ','') then '' else tt1.card_number end as card_number,
            tt1.age,tt1.age_group,tt1.gender,
            tt2.standardcityname as city,tt3.standarddistrictname as residential_area,
            tt1.constellation,tt1.customer_status_eb,tt1.is_member,tt1.member_tenure_days,
            tt1.member_tenure_days_group,tt1.member_origin_channel,
            tt1.member_origin_category,tt1.member_cardtype,
            row_number() over(order by concat(tt1.sephora_id,',',tt1.card_number)) as id,
            concat(tt1.sephora_id,',',tt1.card_number) as union_id
        from(
        select 
        t1.user_id as sephora_id,
        t1.card_no as card_number,
        case when t1.age between 1 and 120 then t1.age else null end as age,
        case when t1.age>0 and t1.age <=18 then '(0,18]'
        when t1.age <=25 then '(18,25]' when t1.age <=30 then '(25,30]' when t1.age <=35 then '(30,35]'
        when t1.age <=40 then '(35,40]' when t1.age <=45 then '(40,45]' when t1.age <=120 then '(45,120]' 
        else null end as age_group,
        case when t1.gender='F' then 'Female' when t1.gender='M' then 'Male' else null end as gender,
        case when t3.crm_city <>'' then t3.crm_city
        when t3.order_city_offline<>'EBUSINESS' then t3.order_city_offline 
        when t3.order_city_online is not null then t3.order_city_online
        else t3.login_city end as city,
        case when t3.order_city_offline='EBUSINESS' then t3.order_residential_area_online else null end as residential_area,
        t2.constellation,
        case when t5.customer_status_eb in ('CONVERT_NEW','BRAND_NEW') then t5.customer_status_eb
        when t4.register_date=date_sub(current_date,1) and t4.first_purchase_date is null then 'NEWLY_REGISTERED'
        when t4.register_date<date_sub(current_date,1) and t4.if_eb_purchase=1 then 'EXISTING_EB'
        else null end as customer_status_eb,
        case when t1.card_no<>0 then 1 else 0  end as is_member,
        case when t4.tenure_days>0 then t4.tenure_days else null end as member_tenure_days,
        case when t4.tenure_days>0 and t4.tenure_days<=360 then '(0,360]' 
        when t4.tenure_days<=720 then '(360,720]'
        when t4.tenure_days<=1080 then '(720,1080]' 
        else '>1080' end as member_tenure_days_group,
        t4.source as member_origin_channel,
        t4.category as  member_origin_category,
        t4.level as member_cardtype
        from 
        (
            select user_id,gender,card_no,dateofbirth,
                year(current_date)-year(dateofbirth) as age,
                month(dateofbirth) as birth_month,
                day(dateofbirth) as birth_date
            from oms.ods_user_profile
            where dt = date_sub(current_date,1)
            and  (user_id rlike '^\\d+$' or user_id is null or user_id in ('null','NULL',' ',''))
            and  (card_no rlike '^\\d+$' or card_no is null or card_no in ('null','NULL',' ',''))
        ) t1 left outer join 
        da_dev.constellation_coding t2 on t1.birth_month=t2.birth_month and t1.birth_date=t2.birth_date
        left outer join
        da_dev.user_basic_info_tagging_city t3 on t1.user_id=t3.sephora_id
        left outer join
        (
            select *
            from da_dev.user_basic_info_tagging_eb_status_register
            where card_number is not null
        ) t4 on t1.card_no=t4.card_number
        left outer join 
        (
            select *
            from da_dev.user_basic_info_tagging_eb_status_new_purchase
            where dt=date_sub(current_date,1)
            and card_number  is not null
        )t5 on t1.card_no=t5.card_number 
         where (t1.user_id is not null or t1.card_no is not null))tt1 left outer join
        (
            select distinct originalcityname,standardcityname
            from da_dev.city_coding_city
            where standardcityname<>'error'
            and originalcityname<>'') tt2 on tt1.city=tt2.originalcityname
            
        left outer join
        (
            select distinct originaldistrictname,standarddistrictname
            from da_dev.city_coding
            where standarddistrictname<>'error' and standarddistrictname is not null
            and originaldistrictname<>''
        )tt3 on tt1.residential_area=tt3.originaldistrictname)ttt1
        where not (ttt1.sephora_id='' and ttt1.card_number='')
    """)
    t14 = time.clock()
    print('Basic Tagging Done: %f' % (t14 - t13))


def run_user_basic_info():
    hive_unit = HiveUnit(**HIVE_CONFIG)
    user_basic_city(hive_unit)
    hive_unit.release()
    hive_unit = HiveUnit(**HIVE_CONFIG)
    user_basic(hive_unit)
    hive_unit.release()
