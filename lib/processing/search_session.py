import datetime
import time

from lib.datastructure.config import HIVE_CONFIG
from lib.db.hive_utils import HiveUnit


def search_session_initial(hive_unit: HiveUnit):
    hive_unit.execute(r"""
        DROP TABLE IF EXISTS da_dev.search_session;
        CREATE TABLE da_dev.search_session(
            user_id string,
            event string,
            time string,
            banner_type string,
            behavior string,
            op_code string,
            platform_type string,
            orderid string,
            sephora_user_id string,
            open_id string,
            dt string,
            seqid int,
            sessionid int,
            key_words string,
            page_type_detail string
        );
    """)


def search_session(hive_unit: HiveUnit):
    t1 = time.clock()
    hive_unit.execute(r"""
     DROP TABLE IF EXISTS da_dev.search_session_temp1;
        CREATE TABLE da_dev.search_session_temp1(
        user_id string,
        event string,
        time string,
        banner_type string,
        behavior string,
        op_code string,
        platform_type string,
        orderid string,
        sephora_user_id string,
        open_id string,
        dt string,
        key_words string,
        page_type_detail string,
        seqid int
    );
    insert into da_dev.search_session_temp1(
        user_id,event,time,banner_type,behavior
        ,op_code,platform_type,orderid,sephora_user_id,open_id,dt,key_words,page_type_detail,seqid
    )
    select user_id,event,time,banner_type,behavior
        ,op_code,platform_type,orderid,sephora_user_id,open_id,dt
        ,key_words,page_type_detail
        ,row_number() over(partition by platform_type,user_id order by cast(time as string)) as seqid
    from(
        select user_id,event,time,banner_type
        ,case when banner_type='search' then banner_type else event end as behavior
        ,op_code,platform_type,orderid,sephora_user_id,open_id,dt,key_words,page_type_detail
        ,row_number() over(partition by user_id,event,time,banner_type,op_code,platform_type,orderid,sephora_user_id
        ,open_id,dt order by cast(time as string)) as rn
        from dwd.v_events
        where platform_type='MiniProgram'
        and dt=date_sub(current_date,1)
    ) as temp
    where rn=1 ;""")
    t2 = time.clock()
    print("Temp1 Done: %f" % (t2 - t1))
    t3 = time.clock()
    hive_unit.execute(r"""
        DROP TABLE IF EXISTS da_dev.search_session_temp2;
        CREATE TABLE da_dev.search_session_temp2(
        user_id string,
        event string,
        time string,
        banner_type string,
        behavior string,
        op_code string,
        platform_type string,
        orderid string,
        sephora_user_id string,
        open_id string,
        dt string,
        seqid int,
        timediff int
        );
        
        insert into da_dev.search_session_temp2(
        user_id,event,time,banner_type,behavior
        ,op_code,platform_type,orderid,sephora_user_id,open_id,dt,seqid,timediff
        )
        select t1.user_id,t1.event,t1.time,t1.banner_type,t1.behavior
        ,t1.op_code,t1.platform_type,t1.orderid,t1.sephora_user_id,t1.open_id,t1.dt,t1.seqid
        ,(unix_timestamp(t1.time, 'yyyy-MM-dd HH:mm') - unix_timestamp(t2.time, 'yyyy-MM-dd HH:mm'))/60 
        from da_dev.search_session_temp1 t1
        left join da_dev.search_session_temp1 t2
        on t1.user_id=t2.user_id and t1.seqid=t2.seqid+1 ;
    """)
    t4 = time.clock()
    print("Temp2 Done: %f" % (t4 - t3))
    t5 = time.clock()
    hive_unit.execute(r"""
        DROP TABLE IF EXISTS da_dev.search_session_temp3;
        CREATE TABLE da_dev.search_session_temp3(
            user_id string,
            behavior string,
            timediff int,
            originalseqid int
        );
        
        with t1 as (
            select user_id,behavior,timediff,seqid as originalseqid
            ,row_number() over(partition by user_id order by seqid) as seqid
            from da_dev.search_session_temp2
            where behavior in('search','$MPHide') or timediff>=30 
        )
        ,t2 as(
            select user_id,behavior,timediff,originalseqid,SeqID
            ,lag(behavior,1) over(partition by user_id order by seqid) lag_behavior
            from t1
        )
        ,t3 as(
            select user_id,behavior,timediff,originalseqid
            ,case when behavior=lag_behavior and timediff < 30 then 0 else 1 end as tag
            from t2
        )
        insert into da_dev.search_session_temp3(user_id,behavior,timediff,originalseqid)
        select user_id,behavior,timediff,originalseqid
        from t3
        where tag=1 ;
        """)
    t6 = time.clock()
    print("Temp3 Done: %f" % (t6 - t5))
    t7 = time.clock()
    hive_unit.execute(r"""
        insert into da_dev.search_session(
        user_id,event,time,banner_type,behavior
        ,op_code,platform_type,orderid,sephora_user_id,open_id,dt,seqid,sessionid,key_words,page_type_detail)
        select tt1.user_id,event,time,banner_type,behavior
        ,op_code,platform_type,orderid,sephora_user_id,open_id,dt,seqid,sessionid+case when maxsessionid is null then 0 
        else maxsessionid end as sessionid
        ,key_words,page_type_detail
        from (
         select t1.user_id,t1.event,t1.time,t1.banner_type,t1.behavior,t1.key_words,t1.page_type_detail
          ,t1.op_code,t1.platform_type,t1.orderid,t1.sephora_user_id,t1.open_id,t1.dt,t1.seqid,t2.rk as sessionid
         from da_dev.search_session_temp1 t1
         left join (
          select user_id,originalseqid as startseqid,endseqid
          ,rank() over(partition by user_id order by originalseqid) rk
          from( 
           select * ,case when behavior='search' and lead_behavior='search' then lead(originalseqid,1) 
           over(partition by user_id order by originalseqid)-1 else lead(originalseqid,1) 
           over(partition by user_id order by originalseqid) end as endseqid
           from( 
            select * ,lead(behavior,1) over(partition by user_id order by originalseqid) lead_behavior
            from da_dev.search_session_temp3
           )a)b
          where behavior='search' 
         ) t2 on t1.user_id=t2.user_id 
         where t1.seqid between t2.startseqid and t2.endseqid 
        )tt1
        left join (
         select user_id,max(sessionid) as maxsessionid
         from da_dev.search_session 
         group by user_id
        )tt2
        on tt1.user_id=tt2.user_id ;
        """)
    t8 = time.clock()
    print("Search session Done: %f" % (t8 - t7))


def initial_session():
    def get_date_by_gap(start_date: str, day_gap: int):
        """
        This function is going to figure out the start date of data time period

        :param start_date:
        :param day_gap:
        :return: start_date: str
        """
        start_date = datetime.datetime.strftime(
            datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(days=day_gap), '%Y-%m-%d')
        return start_date

    hive_unit = HiveUnit(**HIVE_CONFIG)
    search_session_initial(hive_unit)
    hive_unit.release()
    data_str = '2020-03-21'
    while data_str < '2020-09-17':
        print('#' * 20 + 'Engagement Session Start:' + data_str)
        hive_unit = HiveUnit(**HIVE_CONFIG)
        search_session(hive_unit)
        hive_unit.release()
        data_str = get_date_by_gap(start_date=data_str, day_gap=30)


def search_behavior(hive_unit: HiveUnit):
    hive_unit.execute(r"""
        insert into table da_dev.search_session_behavior
        select distinct t1.user_id,t1.time,t1.behavior,t1.key_words
        ,case when t2.op_code is not null then t2.op_code else t1.op_code end as op_code
        ,t1.session_id
        from(
            select distinct user_id,time
            ,case when event='viewCommodityDetail' then 'click' 
            when event='AddToShoppingcart' then 'add'
            when event='submitOrder' then 'order'
            else 'search' end as behavior,orderid,key_words,op_code,sessionid as session_id
            from da_dev.search_session
            where dt=date_sub(current_date,1)            
            and platform_type='MiniProgram' 
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
                where payment_status='1'
                and dt=date_sub(current_date,1))t1
            left outer join
            (
                select distinct sales_order_sys_id,item_product_id
                from oms.ods_sales_order_item 
                where dt=date_sub(current_date,1)
                and item_product_id<>'0' and item_product_id<>0)t2
                on t1.sales_order_sys_id=t2.sales_order_sys_id
        )t2 on t1.orderid=t2.order_id
    """)


def user_session(hive_unit: HiveUnit, n_day: 180):
    hive_unit.execute(r"""
        drop table if exists da_dev.user_sample;
        create table da_dev.user_sample
        (
            user_id string,
            time string,
            behavior string,
            key_words string,
            op_code string,
            sessiong_id string
        );
        
        select distinct t1.user_id ,t1.time ,t1.behavior ,t1.key_words
        ,case when t2.op_code is not null then t2.op_code else t1.op_code end as op_code,SUBSTRING(time,0,10) as sessiong_id
        from(
            select distinct user_id,time
            ,case when event='viewCommodityDetail' then 'click' 
            when event='AddToShoppingcart' then 'add'
            when event='submitOrder' then 'order'
            else 'search' end as behavior,orderid,key_words,op_code
            from dwd.v_events
            where dt between date_sub(current_date,{n_day}+1) and date_sub(current_date,1)
            and platform_type='MiniProgram' 
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
        order by user_id, time""")


def daily_insert():
    print('#' * 20 + 'Search Session Start:' +
          datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(days=-1), '%Y-%m-%d'))
    hive_unit = HiveUnit(**HIVE_CONFIG)
    search_session(hive_unit)
    hive_unit.release()
    print('#' * 20 + 'Search Behavior Start:' +
          datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(days=-1), '%Y-%m-%d'))
    hive_unit = HiveUnit(**HIVE_CONFIG)
    search_behavior(hive_unit)
    hive_unit.release()
    print('#' * 20 + 'user session Start:' +
          datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(days=-1), '%Y-%m-%d'))
    hive_unit = HiveUnit(**HIVE_CONFIG)
    user_session(hive_unit, 180)
    hive_unit.release()




