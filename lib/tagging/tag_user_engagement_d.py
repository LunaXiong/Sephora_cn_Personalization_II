import datetime
import time

from lib.datastructure.config import HIVE_CONFIG
from lib.db.hive_utils import HiveUnit


def engagement_initial(hive_unit: HiveUnit):
    hive_unit.execute(r"""
       DROP TABLE IF EXISTS da_dev.v_events_session;
       CREATE TABLE da_dev.v_events_session(
           event string,
           user_id string,
           `time` string,
           do_city string,
           do_province string,
           do_title string,
           do_element_content string,
           do_url string,
           banner_type string,
           banner_content string,
           banner_current_url string,
           banner_current_page_type string,
           banner_belong_area string,
           banner_to_url string,
           banner_to_page_type string,
           banner_ranking string,
           banner_coding string,
           behavior_type_coding string,
           campaign_code string,
           op_code string,
           platform_type string,
           orderid string,
           beauty_article_title string,
           page_type_detail string,
           page_type string,
           key_words string,
           key_word_type string,
           key_word_type_details string,
           product_id string,
           brand string,
           category string,
           subcategory string,
           sephora_user_id string,
           open_id string,
           dt string,
           sessionid int,
           seqid int,
           sessiontime string
       );
    """)


def engagement_fv_session(hive_unit: HiveUnit, start_date: str):
    t1 = time.clock()
    hive_unit.execute(r"""
        DROP TABLE IF EXISTS da_dev.v_events_lastday;
        CREATE TABLE da_dev.v_events_lastday(
           event string,
           user_id string,
           `time` string,
           do_city string,
           do_province string,
           do_title string,
           do_element_content string,
           do_url string,
           banner_type string,
           banner_content string,
           banner_current_url string,
           banner_current_page_type string,
           banner_belong_area string,
           banner_to_url string,
           banner_to_page_type string,
           banner_ranking string,
           campaign_code string,
           op_code string,
           platform_type string,
           orderid string,
           beauty_article_title string,
           page_type_detail string,
           page_type string,
           key_words string,
           key_word_type string,
           key_word_type_details string,
           product_id string,
           brand string,
           category string,
           subcategory string,
           sephora_user_id string,
           open_id string,
           dt string,
           seqid int
        );
        insert into da_dev.v_events_lastday(
           event,user_id,time,do_city,do_province
           ,do_title,do_element_content,do_url
           ,banner_type,banner_content,banner_current_url,banner_current_page_type
           ,banner_belong_area,banner_to_url,banner_to_page_type,banner_ranking
           ,campaign_code,op_code,platform_type,orderid
           ,beauty_article_title,page_type_detail,page_type
           ,key_words,key_word_type,key_word_type_details
           ,product_id,brand,category,subcategory
           ,sephora_user_id,open_id,dt,seqid
        )
        select event,user_id,time,do_city,do_province
        ,do_title,do_element_content,do_url
        ,banner_type,banner_content,banner_current_url,banner_current_page_type
        ,banner_belong_area,banner_to_url,banner_to_page_type,banner_ranking
        ,campaign_code,op_code,platform_type,orderid
        ,beauty_article_title,page_type_detail,page_type
        ,key_words,key_word_type,key_word_type_details
        ,product_id,brand,category,subcategory
        ,sephora_user_id,open_id,dt
        ,row_number() over(partition by platform_type,user_id order by time,case when platform_type='app'  then
        case event when '$AppStart' then 1
                   when '$AppViewScreen' then 2
                   when '$AppEnd' then 4
        else 3 end
        when platform_type='MiniProgram' then
        case event when '$MPLaunch' then 1
                   when '$MPShow' then 2
                   when '$MPViewScreen' then 3
                   when '$MPHide' then 5
        else 4 end
    end ) as seqid
        from(
           select case when event='$AppStartPassively' then '$AppStart' else event end as event
           ,user_id,time,do_city,do_province
           ,do_title,do_element_content,do_url
           ,banner_type,banner_content,banner_current_url,banner_current_page_type
           ,banner_belong_area,banner_to_url,banner_to_page_type,banner_ranking
           ,campaign_code,op_code,platform_type,orderid
           ,beauty_article_title,page_type_detail,page_type
           ,key_words,key_word_type,key_word_type_details
           ,t2.product_id,t2.brand,t2.category,t2.subcategory
           ,sephora_user_id,open_id,dt
           ,row_number() over(partition by event,user_id,time,do_city,do_province,do_title,do_element_content,do_url
           ,banner_type,banner_content,banner_current_url,banner_current_page_type,banner_belong_area,banner_to_url
           ,banner_to_page_type,banner_ranking,campaign_code,op_code,platform_type,orderid,beauty_article_title
           ,page_type_detail,page_type,key_words,key_word_type,key_word_type_details
           ,sephora_user_id,open_id,dt order by time) as rn
           from dwd.v_events t1
           left join da_dev.search_prod_list t2
           on t1.op_code=t2.product_id
           where dt between '{start_date}' and date_add('{start_date}',29)
           and platform_type in ('MiniProgram','app')
        ) as temp
        where rn=1 ;
    """.format(start_date=start_date))
    t2 = time.clock()
    print('Engagement Sample Done: %f' % (t2 - t1))
    t3 = time.clock()
    hive_unit.execute(r"""
        DROP TABLE IF EXISTS da_dev.v_events_lastday_session_temp1;
        CREATE TABLE da_dev.v_events_lastday_session_temp1(
           event string,
           user_id string,
           `time` string,
           do_city string,
           do_province string,
           do_title string,
           do_element_content string,
           do_url string,
           banner_type string,
           banner_content string,
           banner_current_url string,
           banner_current_page_type string,
           banner_belong_area string,
           banner_to_url string,
           banner_to_page_type string,
           banner_ranking string,
           banner_coding string,
           behavior_type_coding string,
           campaign_code string,
           op_code string,
           platform_type string,
           orderid string,
           beauty_article_title string,
           page_type_detail string,
           page_type string,
           key_words string,
           key_word_type string,
           key_word_type_details string,
           product_id string,
           brand string,
           category string,
           subcategory string,
           sephora_user_id string,
           open_id string,
           dt string,
           seqid int,
           seqidtag int
        );
        insert into da_dev.v_events_lastday_session_temp1(
           event,user_id,time,do_city,do_province
           ,do_title,do_element_content,do_url
           ,banner_type,banner_content,banner_current_url,banner_current_page_type
           ,banner_belong_area,banner_to_url,banner_to_page_type,banner_ranking,banner_coding,behavior_type_coding
           ,campaign_code,op_code,platform_type,orderid
           ,beauty_article_title,page_type_detail,page_type
           ,key_words,key_word_type,key_word_type_details
           ,product_id,brand,category,subcategory
           ,sephora_user_id,open_id,dt,seqid,seqidtag
        )
        select event,user_id,time,do_city,do_province
           ,do_title,do_element_content,do_url
           ,banner_type,banner_content,banner_current_url,banner_current_page_type
           ,banner_belong_area,banner_to_url,banner_to_page_type,banner_ranking,banner_coding,behavior_type_coding
           ,campaign_code,op_code,platform_type,orderid
           ,beauty_article_title,page_type_detail,page_type
           ,key_words,key_word_type,key_word_type_details
           ,product_id,brand,category,subcategory
           ,sephora_user_id,open_id,dt,row_number() over(partition by platform_type,user_id order by seqid) as seqid
           ,1 as seqidtag
        from(
           select *
           ,case when platform_type='app' then
                case when (event='$AppViewScreen' or event='$pageview') and page_type_detail='campaign_page' then '榜单页'
                     when (event='$AppViewScreen' or event='$pageview') and page_type_detail like 'category%' then '分类'
                     when event like'beautyIN_%' or (event ='$pageview' and page_type_detail='beautyCommunity') then '美in'
                     when (event='$AppViewScreen' or event='$pageview') and (page_type_detail='brand_list'
                     or page_type_detail='brand_navigation') then '全部品牌'
                     when (event='$AppViewScreen' or event='$pageview') and (page_type_detail='search'
                     or page_type_detail='search-navigation') then '搜索'
                end
            when platform_type='MiniProgram' then
                case when event='$MPViewScreen' and page_type_detail='miniprogram_campaign' then '榜单页'
                     when event='$MPViewScreen' and page_type_detail like 'category%' then '分类'
                     when event like'beautyIN_%' then '美in'
                     when event='$MPViewScreen' and (page_type_detail='brand_list'
                     or page_type_detail='brand_navigation') then '全部品牌'
                     when event='$MPViewScreen' and page_type_detail='search-navigation' then '搜索'
                 end
           end as banner_coding
           ,case when event like '%Click%' or event like '%click%' then 'Click' else null end as behavior_type_coding
           from da_dev.v_events_lastday
        )temp;

        insert into da_dev.v_events_lastday_session_temp1(
           event,user_id,time,do_city,do_province,platform_type
           ,sephora_user_id,open_id,dt,seqidtag
        )
        select case when platform_type='app' then '$AppStart'
                   when platform_type='MiniProgram' then '$MPLaunch' end as event
        ,user_id,time,do_city,do_province,platform_type
        ,sephora_user_id,open_id,dt, 0 as seqidtag
        from da_dev.v_events_lastday_session_temp1
        where (seqid=1 and event!='$AppStart' and platform_type='app')
          or (seqid=1 and event!='$MPLaunch' and platform_type='MiniProgram');

        insert into da_dev.v_events_lastday_session_temp1(
           event,user_id,time,do_city,do_province,platform_type
           ,sephora_user_id,open_id,dt,seqid,seqidtag)
        select case when platform_type='app' then '$AppEnd'
                   when platform_type='MiniProgram' then '$MPHide' end as event
        ,user_id,time,do_city,do_province,platform_type
        ,sephora_user_id,open_id,dt ,seqid+1 as seqid, 2 as seqidtag
        from(
           select user_id,event,time,do_city,do_province,platform_type,sephora_user_id,open_id,dt,seqid
           ,max(seqid) over(partition by platform_type,USER_ID) mid
           from da_dev.v_events_lastday_session_temp1
        )temp
        where (seqid=mid and event!='$AppEnd' and platform_type='app')
          or (seqid=mid and event!='$MPHide' and platform_type='MiniProgram') ;

        insert into da_dev.v_events_lastday_session_temp1(
        event,user_id,time,do_city,do_province,platform_type
        ,sephora_user_id,open_id,dt,seqidtag)
        select case when t1.platform_type='app' then '$AppEnd'
                   when t1.platform_type='MiniProgram' then '$MPHide' end as event
        ,t1.user_id,t2.time,t2.do_city,t2.do_province,t2.platform_type
        ,t2.sephora_user_id,t2.open_id,t2.dt,2 as seqidtag
        from(
           select event,user_id,time,do_city,do_province,platform_type,sephora_user_id,open_id,dt,seqid
           from da_dev.v_events_lastday_session_temp1
           where event='$AppStart' or event='$MPLaunch'
        )t1
        left join(
           select event,user_id,time,do_city,do_province,platform_type,sephora_user_id,open_id,dt,seqid
           from da_dev.v_events_lastday_session_temp1
        )t2
        on t1.platform_type=t2.platform_type and t1.user_id=t2.user_id and t1.seqid=t2.seqid+1
        where t2.event is not null
        and t2.event!='$AppEnd'
        and t2.event!='$MPHide' ;
    """)
    t4 = time.clock()
    print('Engagement Temp1 Done: %f' % (t4 - t3))
    t5 = time.clock()
    hive_unit.execute(r"""
       DROP TABLE IF EXISTS da_dev.v_events_lastday_session_temp2;
       CREATE TABLE da_dev.v_events_lastday_session_temp2(
           event string,
           user_id string,
           `time` string,
           do_city string,
           do_province string,
           do_title string,
           do_element_content string,
           do_url string,
           banner_type string,
           banner_content string,
           banner_current_url string,
           banner_current_page_type string,
           banner_belong_area string,
           banner_to_url string,
           banner_to_page_type string,
           banner_ranking string,
           banner_coding string,
           behavior_type_coding string,
           campaign_code string,
           op_code string,
           platform_type string,
           orderid string,
           beauty_article_title string,
           page_type_detail string,
           page_type string,
           key_words string,
           key_word_type string,
           key_word_type_details string,
           product_id string,
           brand string,
           category string,
           subcategory string,
           sephora_user_id string,
           open_id string,
           dt string,
           seqid int,
           seqidtag int
       );
       insert into da_dev.v_events_lastday_session_temp2(
           event,user_id,time,do_city,do_province
           ,do_title,do_element_content,do_url
           ,banner_type,banner_content,banner_current_url,banner_current_page_type
           ,banner_belong_area,banner_to_url,banner_to_page_type,banner_ranking,banner_coding,behavior_type_coding
           ,campaign_code,op_code,platform_type,orderid
           ,beauty_article_title,page_type_detail,page_type
           ,key_words,key_word_type,key_word_type_details
           ,product_id,brand,category,subcategory
           ,sephora_user_id,open_id,dt,seqid,seqidtag
       )
       select event,user_id,time,do_city,do_province
           ,do_title,do_element_content,do_url
           ,banner_type,banner_content,banner_current_url,banner_current_page_type
           ,banner_belong_area,banner_to_url,banner_to_page_type,banner_ranking,banner_coding,behavior_type_coding
           ,campaign_code,op_code,platform_type,orderid
           ,beauty_article_title,page_type_detail,page_type
           ,key_words,key_word_type,key_word_type_details
           ,product_id,brand,category,subcategory
           ,sephora_user_id,open_id,dt
           ,row_number() over(partition by platform_type,user_id order by time,seqidtag,seqid) as seqid ,1 as seqidtag
       from da_dev.v_events_lastday_session_temp1 ;
       insert into da_dev.v_events_lastday_session_temp2
       (event,user_id,time,do_city,do_province,platform_type
       ,sephora_user_id,open_id,dt,seqidtag)
       select case when t1.platform_type='app' then '$AppStart'
                   when t1.platform_type='MiniProgram' then '$MPLaunch' end as event
       ,t1.user_id,t2.time,t2.do_city,t2.do_province,t2.platform_type
       ,t2.sephora_user_id,t2.open_id,t2.dt ,0 as seqidtag
       from(
           select event,user_id,time,do_city,do_province,platform_type,sephora_user_id,open_id,dt,seqid
           from da_dev.v_events_lastday_session_temp2
           where event='$AppEnd' or event='$MPHide'
       )t1
       left join(
           select event,user_id,time,do_city,do_province,platform_type,sephora_user_id,open_id,dt,seqid
           from da_dev.v_events_lastday_session_temp2
       )t2
       on t1.platform_type=t2.platform_type and t1.user_id=t2.user_id and t1.seqid=t2.seqid-1
       where t2.event is not null
       and t2.event!='$AppStart'
       and t2.event!='$MPLaunch' ;
    """)
    t6 = time.clock()
    print('Engagement Temp2 Done: %f' % (t6 - t5))
    t7 = time.clock()
    hive_unit.execute(r"""
        DROP TABLE IF EXISTS da_dev.v_events_lastday_session_temp3;
        CREATE TABLE da_dev.v_events_lastday_session_temp3(
           event string,
           user_id string,
           `time` string,
           do_city string,
           do_province string,
           do_title string,
           do_element_content string,
           do_url string,
           banner_type string,
           banner_content string,
           banner_current_url string,
           banner_current_page_type string,
           banner_belong_area string,
           banner_to_url string,
           banner_to_page_type string,
           banner_ranking string,
           banner_coding string,
           behavior_type_coding string,
           campaign_code string,
           op_code string,
           platform_type string,
           orderid string,
           beauty_article_title string,
           page_type_detail string,
           page_type string,
           key_words string,
           key_word_type string,
           key_word_type_details string,
           product_id string,
           brand string,
           category string,
           subcategory string,
           sephora_user_id string,
           open_id string,
           dt string,
           seqid int,
           seqidtag int
        );
        insert into da_dev.v_events_lastday_session_temp3(
           event,user_id,time,do_city,do_province
           ,do_title,do_element_content,do_url
           ,banner_type,banner_content,banner_current_url,banner_current_page_type
           ,banner_belong_area,banner_to_url,banner_to_page_type,banner_ranking,banner_coding,behavior_type_coding
           ,campaign_code,op_code,platform_type,orderid
           ,beauty_article_title,page_type_detail,page_type
           ,key_words,key_word_type,key_word_type_details
           ,product_id,brand,category,subcategory
           ,sephora_user_id,open_id,dt,seqid,seqidtag
        )
        select event,user_id,time,do_city,do_province
           ,do_title,do_element_content,do_url
           ,banner_type,banner_content,banner_current_url,banner_current_page_type
           ,banner_belong_area,banner_to_url,banner_to_page_type,banner_ranking,banner_coding,behavior_type_coding
           ,campaign_code,op_code,platform_type,orderid
           ,beauty_article_title,page_type_detail,page_type
           ,key_words,key_word_type,key_word_type_details
           ,product_id,brand,category,subcategory
           ,sephora_user_id,open_id,dt
           ,row_number() over(partition by platform_type,user_id order by time,seqidtag,seqid) as seqid ,1 as seqidtag
        from da_dev.v_events_lastday_session_temp2 ;
    """)
    t8 = time.clock()
    print('Engagement Temp3 Done: %f' % (t8 - t7))
    t19 = time.clock()
    hive_unit.execute(r"""
    insert into da_dev.v_events_session(
           event,user_id,time,do_city,do_province
           ,do_title,do_element_content,do_url
           ,banner_type,banner_content,banner_current_url,banner_current_page_type
           ,banner_belong_area,banner_to_url,banner_to_page_type,banner_ranking,banner_coding,behavior_type_coding
           ,campaign_code,op_code,platform_type,orderid
           ,beauty_article_title,page_type_detail,page_type
           ,key_words,key_word_type,key_word_type_details
           ,product_id,brand,category,subcategory
           ,sephora_user_id,open_id,dt,sessionid,seqid
           ,sessiontime
        )
        select event,tb1.user_id,time,do_city,do_province
           ,do_title,do_element_content,do_url
           ,banner_type,banner_content,banner_current_url,banner_current_page_type
           ,banner_belong_area,banner_to_url,banner_to_page_type,banner_ranking,banner_coding,behavior_type_coding
           ,campaign_code,op_code,tb1.platform_type,orderid
           ,beauty_article_title,page_type_detail,page_type
           ,key_words,key_word_type,key_word_type_details
           ,product_id,brand,category,subcategory
           ,sephora_user_id,open_id,dt,sessionid+case when maxsessionid is null then 0 else maxsessionid end,seqid
           ,sessiontime
        from (
            select tt1.event,tt1.user_id,tt1.time,tt1.do_city,tt1.do_province
               ,tt1.do_title,tt1.do_element_content,tt1.do_url
               ,tt1.banner_type,tt1.banner_content,tt1.banner_current_url,tt1.banner_current_page_type
               ,tt1.banner_belong_area,tt1.banner_to_url,tt1.banner_to_page_type,tt1.banner_ranking,tt1.banner_coding,tt1.behavior_type_coding
               ,tt1.campaign_code,tt1.op_code,tt1.platform_type,tt1.orderid
               ,tt1.beauty_article_title,tt1.page_type_detail,tt1.page_type
               ,tt1.key_words,tt1.key_word_type,tt1.key_word_type_details
               ,tt1.product_id,tt1.brand,tt1.category,tt1.subcategory
               ,tt1.sephora_user_id,tt1.open_id,tt1.dt
               ,tt2.rk as sessionid,RANK()over(partition by tt1.platform_type,tt1.user_id,rk order by seqid) seqid
               ,cast(tt2.sessiontime as decimal(18,2)) sessiontime
            from da_dev.v_events_lastday_session_temp3 tt1
            join (
               select * ,(unix_timestamp(Htime) - unix_timestamp(Ltime))/60.0 as sessiontime
               ,rank()over(partition by platform_type,user_id order by Lseqid) rk
               from(
                   select  platform_type,user_id, EVENT,RN 
                   ,case when platform_type='app' and lag(event,1) over(partition by platform_type,user_id order by seqid)='$AppStart' then null 
                    when platform_type='MiniProgram' and lag(event,1) over(partition by platform_type,user_id order by seqid)='$MPLaunch' then null 
                    else seqid end as Lseqid
                    ,case when platform_type='app' and event='$AppStart' and LEAD(event,1) over(partition by platform_type,user_id order by seqid)='$AppStart' then LEAD(Hseqid,1) over(partition by platform_type,user_id order by seqid) 
                    when platform_type='MiniProgram' and event='$MPLaunch' and LEAD(event,1) over(partition by platform_type,user_id order by seqid)='$MPLaunch' then LEAD(Hseqid,1) over(partition by platform_type,user_id order by seqid) 
                    else Hseqid end Hseqid
                    ,case when platform_type='app' and lag(event,1) over(partition by platform_type,user_id order by seqid)='$AppStart' then null 
                    when platform_type='MiniProgram' and lag(event,1) over(partition by platform_type,user_id order by seqid)='$MPLaunch' then null 
                    else time end as Ltime
                    ,case when platform_type='app' and event='$AppStart' and LEAD(event,1) over(partition by platform_type,user_id order by seqid)='$AppStart' then LEAD(Htime,1) over(partition by platform_type,user_id order by seqid) 
                    when platform_type='MiniProgram' and event='$MPLaunch' and LEAD(event,1) over(partition by platform_type,user_id order by seqid)='$MPLaunch' then LEAD(Htime,1) over(partition by platform_type,user_id order by seqid) 
                    else Htime end Htime
                   from (
                       select platform_type,user_id,event,seqid,time
                       ,rank()over(partition by platform_type,user_id order by seqid) rn
                       ,LEAD(seqid,1) over(partition by platform_type,user_id order by seqid) as Hseqid
                       ,LEAD(time,1) over(partition by platform_type,user_id order by seqid) as Htime
                       from da_dev.v_events_lastday_session_temp3
                       where event in('$AppStart','$AppEnd','$MPLaunch','$MPHide')
                   )t1
               )t2
               where (event ='$AppStart' and platform_type='app' and Lseqid is not null)
                  or (event ='$MPLaunch' and platform_type='MiniProgram' and Lseqid is not null)
            )tt2
            on tt1.platform_type=tt2.platform_type and tt1.user_id=tt2.user_id	
            where tt1.seqid between Lseqid and Hseqid 
        )tb1
        left join(
            select platform_type,user_id,max(sessionid) as maxsessionid
            from da_dev.v_events_session
            group by platform_type,user_id
        )tb2
        on tb1.user_id=tb2.user_id and tb1.platform_type=tb2.platform_type ;   
    """)
    t20 = time.clock()
    print('Engagement Session Done: %f' % (t20 - t19))


def engagement_daily_session(hive_unit: HiveUnit):
    t9 = time.clock()
    hive_unit.execute(r"""
    DROP TABLE IF EXISTS da_dev.v_events_lastday;
       CREATE TABLE da_dev.v_events_lastday(
           event string,
           user_id string,
           `time` string,
           do_city string,
           do_province string,
           do_title string,
           do_element_content string,
           do_url string,
           banner_type string,
           banner_content string,
           banner_current_url string,
           banner_current_page_type string,
           banner_belong_area string,
           banner_to_url string,
           banner_to_page_type string,
           banner_ranking string,
           campaign_code string,
           op_code string,
           platform_type string,
           orderid string,
           beauty_article_title string,
           page_type_detail string,
           page_type string,
           key_words string,
           key_word_type string,
           key_word_type_details string,
           product_id string,
           brand string,
           category string,
           subcategory string,
           sephora_user_id string,
           open_id string,
           dt string,
           seqid int
       );
       insert into da_dev.v_events_lastday(
           event,user_id,time,do_city,do_province
           ,do_title,do_element_content,do_url
           ,banner_type,banner_content,banner_current_url,banner_current_page_type
           ,banner_belong_area,banner_to_url,banner_to_page_type,banner_ranking
           ,campaign_code,op_code,platform_type,orderid
           ,beauty_article_title,page_type_detail,page_type
           ,key_words,key_word_type,key_word_type_details
           ,product_id,brand,category,subcategory
           ,sephora_user_id,open_id,dt,seqid
       )
       select event,user_id,time,do_city,do_province
       ,do_title,do_element_content,do_url
       ,banner_type,banner_content,banner_current_url,banner_current_page_type
       ,banner_belong_area,banner_to_url,banner_to_page_type,banner_ranking
       ,campaign_code,op_code,platform_type,orderid
       ,beauty_article_title,page_type_detail,page_type
       ,key_words,key_word_type,key_word_type_details
       ,product_id,brand,category,subcategory
       ,sephora_user_id,open_id,dt
       ,row_number() over(partition by platform_type,user_id order by time,case when platform_type='app'  then
        case event when '$AppStart' then 1
                   when '$AppViewScreen' then 2
                   when '$AppEnd' then 4
        else 3 end
        when platform_type='MiniProgram' then
        case event when '$MPLaunch' then 1
                   when '$MPShow' then 2
                   when '$MPViewScreen' then 3
                   when '$MPHide' then 5
        else 4 end
    end ) as seqid
       from(
           select case when event='$AppStartPassively' then '$AppStart' else event end as event,user_id,time,do_city
           ,do_province,do_title,do_element_content,do_url
           ,banner_type,banner_content,banner_current_url,banner_current_page_type
           ,banner_belong_area,banner_to_url,banner_to_page_type,banner_ranking
           ,campaign_code,op_code,platform_type,orderid
           ,beauty_article_title,page_type_detail,page_type
           ,key_words,key_word_type,key_word_type_details
           ,t2.product_id,t2.brand,t2.category,t2.subcategory
           ,sephora_user_id,open_id,dt
           ,row_number() over(partition by event,user_id,time,do_city,do_province,do_title,do_element_content,do_url
           ,banner_type,banner_content,banner_current_url,banner_current_page_type,banner_belong_area,banner_to_url
           ,banner_to_page_type,banner_ranking,campaign_code,op_code,platform_type,orderid,beauty_article_title
           ,page_type_detail,page_type,key_words,key_word_type,key_word_type_details,sephora_user_id,open_id
           ,dt order by time) as rn
           from dwd.v_events t1
           left join da_dev.search_prod_list t2
           on t1.op_code=t2.product_id
           where dt = DATE_ADD(CURRENT_DATE,-1)
           and platform_type in ('MiniProgram','app')
       ) as temp
       where rn=1 ;


       DROP TABLE IF EXISTS da_dev.v_events_lastday_session_temp1;
       CREATE TABLE da_dev.v_events_lastday_session_temp1(
           event string,
           user_id string,
           `time` string,
           do_city string,
           do_province string,
           do_title string,
           do_element_content string,
           do_url string,
           banner_type string,
           banner_content string,
           banner_current_url string,
           banner_current_page_type string,
           banner_belong_area string,
           banner_to_url string,
           banner_to_page_type string,
           banner_ranking string,
           banner_coding string,
           behavior_type_coding string,
           campaign_code string,
           op_code string,
           platform_type string,
           orderid string,
           beauty_article_title string,
           page_type_detail string,
           page_type string,
           key_words string,
           key_word_type string,
           key_word_type_details string,
           product_id string,
           brand string,
           category string,
           subcategory string,
           sephora_user_id string,
           open_id string,
           dt string,
           seqid int,
           seqidtag int
       );
       insert into da_dev.v_events_lastday_session_temp1(
           event,user_id,time,do_city,do_province
           ,do_title,do_element_content,do_url
           ,banner_type,banner_content,banner_current_url,banner_current_page_type
           ,banner_belong_area,banner_to_url,banner_to_page_type,banner_ranking,banner_coding,behavior_type_coding
           ,campaign_code,op_code,platform_type,orderid
           ,beauty_article_title,page_type_detail,page_type
           ,key_words,key_word_type,key_word_type_details
           ,product_id,brand,category,subcategory
           ,sephora_user_id,open_id,dt,seqid,seqidtag
       )
       select event,user_id,time,do_city,do_province
           ,do_title,do_element_content,do_url
           ,banner_type,banner_content,banner_current_url,banner_current_page_type
           ,banner_belong_area,banner_to_url,banner_to_page_type,banner_ranking,banner_coding,behavior_type_coding
           ,campaign_code,op_code,platform_type,orderid
           ,beauty_article_title,page_type_detail,page_type
           ,key_words,key_word_type,key_word_type_details
           ,product_id,brand,category,subcategory
           ,sephora_user_id,open_id,dt,row_number() over(partition by platform_type,user_id order by seqid) as seqid ,1 as seqidtag
       from(
           select *
           ,case when platform_type='app' then
                case when (event='$AppViewScreen' or event='$pageview') and page_type_detail='campaign_page' then '榜单页'
                     when (event='$AppViewScreen' or event='$pageview') and page_type_detail like 'category%' then '分类'
                     when event like'beautyIN_%' or (event ='$pageview' and page_type_detail='beautyCommunity') then '美in'
                     when (event='$AppViewScreen' or event='$pageview') and (page_type_detail='brand_list' or page_type_detail='brand_navigation') then '全部品牌'
                     when (event='$AppViewScreen' or event='$pageview') and (page_type_detail='search' or page_type_detail='search-navigation') then '搜索'
                end
            when platform_type='MiniProgram' then
                case when event='$MPViewScreen' and page_type_detail='miniprogram_campaign' then '榜单页'
                     when event='$MPViewScreen' and page_type_detail like 'category%' then '分类'
                     when event like'beautyIN_%' then '美in'
                     when event='$MPViewScreen' and (page_type_detail='brand_list' or page_type_detail='brand_navigation') then '全部品牌'
                     when event='$MPViewScreen' and page_type_detail='search-navigation' then '搜索'
                 end
           end as banner_coding
           ,case when event like '%Click%' or event like '%click%' then 'Click' else null end as behavior_type_coding
           from da_dev.v_events_lastday
       )temp;
       insert into da_dev.v_events_lastday_session_temp1(
           event,user_id,time,do_city,do_province,platform_type
           ,sephora_user_id,open_id,dt,seqidtag
       )

       select case when platform_type='app' then '$AppStart'
                   when platform_type='MiniProgram' then '$MPLaunch' end as event
       ,user_id,time,do_city,do_province,platform_type
       ,sephora_user_id,open_id,dt, 0 as seqidtag
       from da_dev.v_events_lastday_session_temp1
       where (seqid=1 and event!='$AppStart' and platform_type='app')
          or (seqid=1 and event!='$MPLaunch' and platform_type='MiniProgram');
       insert into da_dev.v_events_lastday_session_temp1(
           event,user_id,time,do_city,do_province,platform_type
           ,sephora_user_id,open_id,dt,seqid,seqidtag)

       select case when platform_type='app' then '$AppEnd'
                   when platform_type='MiniProgram' then '$MPHide' end as event
       ,user_id,time,do_city,do_province,platform_type
       ,sephora_user_id,open_id,dt ,seqid+1 as seqid, 2 as seqidtag
       from(
           select user_id,event,time,do_city,do_province,platform_type,sephora_user_id,open_id,dt,seqid
           ,max(seqid) over(partition by platform_type,USER_ID) mid
           from da_dev.v_events_lastday_session_temp1
       )temp
       where (seqid=mid and event!='$AppEnd' and platform_type='app')
          or (seqid=mid and event!='$MPHide' and platform_type='MiniProgram') ;
       insert into da_dev.v_events_lastday_session_temp1(
       event,user_id,time,do_city,do_province,platform_type
       ,sephora_user_id,open_id,dt,seqidtag)

       select case when t1.platform_type='app' then '$AppEnd'
                   when t1.platform_type='MiniProgram' then '$MPHide' end as event
       ,t1.user_id,t2.time,t2.do_city,t2.do_province,t2.platform_type
       ,t2.sephora_user_id,t2.open_id,t2.dt,2 as seqidtag
       from(
           select event,user_id,time,do_city,do_province,platform_type,sephora_user_id,open_id,dt,seqid
           from da_dev.v_events_lastday_session_temp1
           where event='$AppStart' or event='$MPLaunch'
       )t1
       left join(
           select event,user_id,time,do_city,do_province,platform_type,sephora_user_id,open_id,dt,seqid
           from da_dev.v_events_lastday_session_temp1
       )t2
       on t1.platform_type=t2.platform_type and t1.user_id=t2.user_id and t1.seqid=t2.seqid+1
       where t2.event is not null
       and t2.event!='$AppEnd'
       and t2.event!='$MPHide' ;

       DROP TABLE IF EXISTS da_dev.v_events_lastday_session_temp2;
       CREATE TABLE da_dev.v_events_lastday_session_temp2(
           event string,
           user_id string,
           `time` string,
           do_city string,
           do_province string,
           do_title string,
           do_element_content string,
           do_url string,
           banner_type string,
           banner_content string,
           banner_current_url string,
           banner_current_page_type string,
           banner_belong_area string,
           banner_to_url string,
           banner_to_page_type string,
           banner_ranking string,
           banner_coding string,
           behavior_type_coding string,
           campaign_code string,
           op_code string,
           platform_type string,
           orderid string,
           beauty_article_title string,
           page_type_detail string,
           page_type string,
           key_words string,
           key_word_type string,
           key_word_type_details string,
           product_id string,
           brand string,
           category string,
           subcategory string,
           sephora_user_id string,
           open_id string,
           dt string,
           seqid int,
           seqidtag int
       );
       insert into da_dev.v_events_lastday_session_temp2(
           event,user_id,time,do_city,do_province
           ,do_title,do_element_content,do_url
           ,banner_type,banner_content,banner_current_url,banner_current_page_type
           ,banner_belong_area,banner_to_url,banner_to_page_type,banner_ranking,banner_coding,behavior_type_coding
           ,campaign_code,op_code,platform_type,orderid
           ,beauty_article_title,page_type_detail,page_type
           ,key_words,key_word_type,key_word_type_details
           ,product_id,brand,category,subcategory
           ,sephora_user_id,open_id,dt,seqid,seqidtag
       )
       select event,user_id,time,do_city,do_province
           ,do_title,do_element_content,do_url
           ,banner_type,banner_content,banner_current_url,banner_current_page_type
           ,banner_belong_area,banner_to_url,banner_to_page_type,banner_ranking,banner_coding,behavior_type_coding
           ,campaign_code,op_code,platform_type,orderid
           ,beauty_article_title,page_type_detail,page_type
           ,key_words,key_word_type,key_word_type_details
           ,product_id,brand,category,subcategory
           ,sephora_user_id,open_id,dt
           ,row_number() over(partition by platform_type,user_id order by time,seqidtag,seqid) as seqid ,1 as seqidtag
       from da_dev.v_events_lastday_session_temp1 ;
       insert into da_dev.v_events_lastday_session_temp2
       (event,user_id,time,do_city,do_province,platform_type
       ,sephora_user_id,open_id,dt,seqidtag)
       select case when t1.platform_type='app' then '$AppStart'
                   when t1.platform_type='MiniProgram' then '$MPLaunch' end as event
       ,t1.user_id,t2.time,t2.do_city,t2.do_province,t2.platform_type
       ,t2.sephora_user_id,t2.open_id,t2.dt ,0 as seqidtag
       from(
           select event,user_id,time,do_city,do_province,platform_type,sephora_user_id,open_id,dt,seqid
           from da_dev.v_events_lastday_session_temp2
           where event='$AppEnd' or event='$MPHide'
       )t1
       left join(
           select event,user_id,time,do_city,do_province,platform_type,sephora_user_id,open_id,dt,seqid
           from da_dev.v_events_lastday_session_temp2
       )t2
       on t1.platform_type=t2.platform_type and t1.user_id=t2.user_id and t1.seqid=t2.seqid-1
       where t2.event is not null
       and t2.event!='$AppStart'
       and t2.event!='$MPLaunch' ;

       DROP TABLE IF EXISTS da_dev.v_events_lastday_session_temp3;
       CREATE TABLE da_dev.v_events_lastday_session_temp3(
           event string,
           user_id string,
           `time` string,
           do_city string,
           do_province string,
           do_title string,
           do_element_content string,
           do_url string,
           banner_type string,
           banner_content string,
           banner_current_url string,
           banner_current_page_type string,
           banner_belong_area string,
           banner_to_url string,
           banner_to_page_type string,
           banner_ranking string,
           banner_coding string,
           behavior_type_coding string,
           campaign_code string,
           op_code string,
           platform_type string,
           orderid string,
           beauty_article_title string,
           page_type_detail string,
           page_type string,
           key_words string,
           key_word_type string,
           key_word_type_details string,
           product_id string,
           brand string,
           category string,
           subcategory string,
           sephora_user_id string,
           open_id string,
           dt string,
           seqid int,
           seqidtag int
       );
       insert into da_dev.v_events_lastday_session_temp3(
           event,user_id,time,do_city,do_province
           ,do_title,do_element_content,do_url
           ,banner_type,banner_content,banner_current_url,banner_current_page_type
           ,banner_belong_area,banner_to_url,banner_to_page_type,banner_ranking,banner_coding,behavior_type_coding
           ,campaign_code,op_code,platform_type,orderid
           ,beauty_article_title,page_type_detail,page_type
           ,key_words,key_word_type,key_word_type_details
           ,product_id,brand,category,subcategory
           ,sephora_user_id,open_id,dt,seqid,seqidtag
       )
       select event,user_id,time,do_city,do_province
           ,do_title,do_element_content,do_url
           ,banner_type,banner_content,banner_current_url,banner_current_page_type
           ,banner_belong_area,banner_to_url,banner_to_page_type,banner_ranking,banner_coding,behavior_type_coding
           ,campaign_code,op_code,platform_type,orderid
           ,beauty_article_title,page_type_detail,page_type
           ,key_words,key_word_type,key_word_type_details
           ,product_id,brand,category,subcategory
           ,sephora_user_id,open_id,dt
           ,row_number() over(partition by platform_type,user_id order by time,seqidtag,seqid) as seqid ,1 as seqidtag
       from da_dev.v_events_lastday_session_temp2 ;

       insert into da_dev.v_events_session(
           event,user_id,time,do_city,do_province
           ,do_title,do_element_content,do_url
           ,banner_type,banner_content,banner_current_url,banner_current_page_type
           ,banner_belong_area,banner_to_url,banner_to_page_type,banner_ranking,banner_coding,behavior_type_coding
           ,campaign_code,op_code,platform_type,orderid
           ,beauty_article_title,page_type_detail,page_type
           ,key_words,key_word_type,key_word_type_details
           ,product_id,brand,category,subcategory
           ,sephora_user_id,open_id,dt,sessionid,seqid
           ,sessiontime
       )
	   select event,tb1.user_id,time,do_city,do_province
           ,do_title,do_element_content,do_url
           ,banner_type,banner_content,banner_current_url,banner_current_page_type
           ,banner_belong_area,banner_to_url,banner_to_page_type,banner_ranking,banner_coding,behavior_type_coding
           ,campaign_code,op_code,tb1.platform_type,orderid
           ,beauty_article_title,page_type_detail,page_type
           ,key_words,key_word_type,key_word_type_details
           ,product_id,brand,category,subcategory
           ,sephora_user_id,open_id,dt,sessionid+case when maxsessionid is null then 0 else maxsessionid end,seqid
           ,sessiontime
       from (
           select tt1.event,tt1.user_id,tt1.time,tt1.do_city,tt1.do_province
               ,tt1.do_title,tt1.do_element_content,tt1.do_url
               ,tt1.banner_type,tt1.banner_content,tt1.banner_current_url,tt1.banner_current_page_type
               ,tt1.banner_belong_area,tt1.banner_to_url,tt1.banner_to_page_type,tt1.banner_ranking,tt1.banner_coding,tt1.behavior_type_coding
               ,tt1.campaign_code,tt1.op_code,tt1.platform_type,tt1.orderid
               ,tt1.beauty_article_title,tt1.page_type_detail,tt1.page_type
               ,tt1.key_words,tt1.key_word_type,tt1.key_word_type_details
               ,tt1.product_id,tt1.brand,tt1.category,tt1.subcategory
               ,tt1.sephora_user_id,tt1.open_id,tt1.dt
               ,tt2.rk as sessionid,RANK()over(partition by tt1.platform_type,tt1.user_id,rk order by seqid) seqid
               ,cast(tt2.sessiontime as decimal(18,2)) sessiontime
           from da_dev.v_events_lastday_session_temp3 tt1
           join (
               select * ,(unix_timestamp(Htime) - unix_timestamp(Ltime))/60.0 as sessiontime
               ,rank()over(partition by platform_type,user_id order by Lseqid) rk
               from(
                   select  platform_type,user_id, EVENT,RN 
                    ,case when platform_type='app' and lag(event,1) over(partition by platform_type,user_id order by seqid)='$AppStart' then null 
                    when platform_type='MiniProgram' and lag(event,1) over(partition by platform_type,user_id order by seqid)='$MPLaunch' then null 
                    else seqid end as Lseqid
                    ,case when platform_type='app' and event='$AppStart' and LEAD(event,1) over(partition by platform_type,user_id order by seqid)='$AppStart' then LEAD(Hseqid,1) over(partition by platform_type,user_id order by seqid) 
                    when platform_type='MiniProgram' and event='$MPLaunch' and LEAD(event,1) over(partition by platform_type,user_id order by seqid)='$MPLaunch' then LEAD(Hseqid,1) over(partition by platform_type,user_id order by seqid) 
                    else Hseqid end Hseqid
                    ,case when platform_type='app' and lag(event,1) over(partition by platform_type,user_id order by seqid)='$AppStart' then null 
                    when platform_type='MiniProgram' and lag(event,1) over(partition by platform_type,user_id order by seqid)='$MPLaunch' then null 
                    else time end as Ltime
                    ,case when platform_type='app' and event='$AppStart' and LEAD(event,1) over(partition by platform_type,user_id order by seqid)='$AppStart' then LEAD(Htime,1) over(partition by platform_type,user_id order by seqid) 
                    when platform_type='MiniProgram' and event='$MPLaunch' and LEAD(event,1) over(partition by platform_type,user_id order by seqid)='$MPLaunch' then LEAD(Htime,1) over(partition by platform_type,user_id order by seqid) 
                    else Htime end Htime
                   from (
                       select platform_type,user_id,event,seqid,time
                       ,rank()over(partition by platform_type,user_id order by seqid) rn
                       ,LEAD(seqid,1) over(partition by platform_type,user_id order by seqid) as Hseqid
                       ,LEAD(time,1) over(partition by platform_type,user_id order by seqid) as Htime
                       from da_dev.v_events_lastday_session_temp3
                       where event in('$AppStart','$AppEnd','$MPLaunch','$MPHide')
                   )t1
               )t2
               where (event ='$AppStart' and platform_type='app' and Lseqid is not null)
                  or (event ='$MPLaunch' and platform_type='MiniProgram' and Lseqid is not null)
           )tt2
           on tt1.platform_type=tt2.platform_type and tt1.user_id=tt2.user_id	
           where tt1.seqid between Lseqid and Hseqid 
        )tb1
        left join(
            select platform_type,user_id,max(sessionid) as maxsessionid
            from da_dev.v_events_session
            group by platform_type,user_id
        )tb2
        on tb1.user_id=tb2.user_id and tb1.platform_type=tb2.platform_type ;
    """)
    t10 = time.clock()
    print('Engagement Daliy Session Done: %f' % (t10 - t9))


def engagement_cal(hive_unit: HiveUnit):
    t11 = time.clock()
    hive_unit.execute(r"""
        DROP TABLE IF EXISTS da_dev.engagement_tagging_temp1;
        create table da_dev.engagement_tagging_temp1(
                sephora_id string,
                card_number string,
                union_id string,
                most_visited_channel string
            );
        insert into da_dev.engagement_tagging_temp1(sephora_id,card_number,union_id,most_visited_channel)
        select sephora_id,card_number,union_id,platform_type as most_visited_channel
        from(
            select sephora_id,card_number,union_id,platform_type,row_number() over(partition by sephora_id,card_number,union_id order by platform_cnt desc) rn
            from(
                select sephora_id,card_number,t2.union_id,platform_type,count(distinct dt) platform_cnt
                from dwd.v_events t1
                join da_dev.tagging_id_mapping t2
                on t1.user_id=t2.sensor_id
                where dt between DATE_ADD(CURRENT_DATE,-181) and DATE_ADD(CURRENT_DATE,-1)
                group by sephora_id,card_number,t2.union_id,platform_type
            )t1
        )t2
         where rn=1 ;  """)
    t12 = time.clock()
    print('Engagement Cal Temp1 Done: %f' % (t12 - t11))
    t13 = time.clock()

    hive_unit.execute(r"""
        DROP TABLE IF EXISTS da_dev.engagement_tagging_temp2;
        create table da_dev.engagement_tagging_temp2(
            sephora_id string,
            card_number string,
            union_id string,
            most_visited_channel string,
            last_30D_visit int,
            last_30D_bounce string
        );
        insert into da_dev.engagement_tagging_temp2(sephora_id,card_number,union_id,most_visited_channel,last_30D_visit,last_30D_bounce)
        select tt1.sephora_id,tt1.card_number,tt1.union_id,most_visited_channel,last_30D_visit,cast(last_30D_bounce as decimal(18,4)) as last_30D_bounce
        from da_dev.engagement_tagging_temp1 tt1
        left join (
            select sephora_id,card_number,union_id,sum(distinct maxsessionid) last_30D_visit,(sum(distinct maxsessionid)-sum(sessionid))/cast(sum(distinct maxsessionid) as decimal(18,4))last_30D_bounce
            from(
                select platform_type,be_type,sephora_id,card_number,union_id,sum(distinct maxsessionid)maxsessionid,count(distinct sessionid)sessionid
                from(
                    select case when event like '%click%' then 'click' else null end as be_type,platform_type,sessionid,user_id
                    ,max(sessionid) over(partition by platform_type,user_id) maxsessionid
                    from da_dev.v_events_session
                    where dt between DATE_ADD(CURRENT_DATE,-31) and DATE_ADD(CURRENT_DATE,-1)
                )t1
                join da_dev.tagging_id_mapping t2
                on t1.user_id=t2.sensor_id
                where be_type='click'
                group by platform_type,be_type,sephora_id,card_number,union_id
            )t2
            group by sephora_id,card_number,union_id
        )tt2
        on tt1.union_id=tt2.union_id;""")
    t14 = time.clock()
    print('Engagement Cal Temp2 Done: %f' % (t14 - t13))
    t15 = time.clock()
    hive_unit.execute(r"""
        DROP TABLE IF EXISTS da_dev.engagement_tagging_temp3;
        create table da_dev.engagement_tagging_temp3(
            sephora_id string,
            card_number string,
            union_id string,
            most_visited_channel string,
            last_30D_visit int,
            last_30D_bounce string,
            most_visited_category string,
            most_visited_subcategory string,
            most_visited_brand string,
            most_visited_function string
        );
        insert into da_dev.engagement_tagging_temp3(
        sephora_id,card_number,union_id,most_visited_channel,last_30D_visit,last_30D_bounce,
        most_visited_category,most_visited_subcategory,most_visited_brand,most_visited_function)
        select tt1.sephora_id,tt1.card_number,tt1.union_id,most_visited_channel,last_30D_visit,last_30D_bounce,
        most_visited_category,most_visited_subcategory,most_visited_brand,most_visited_function
        from da_dev.engagement_tagging_temp2 tt1
        left join(
            select  distinct sephora_id,card_number,union_id
            ,COALESCE(maxca,LAST_VALUE(maxca,true) over(partition by sephora_id,card_number,union_id order by maxca desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) most_visited_category
            ,COALESCE(maxsca,LAST_VALUE(maxsca,true) over(partition by sephora_id,card_number,union_id order by maxsca desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) most_visited_subcategory
            ,COALESCE(maxbrand,LAST_VALUE(maxbrand,true) over(partition by sephora_id,card_number,union_id order by maxbrand desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) most_visited_brand
            ,COALESCE(maxbanner,LAST_VALUE(maxbanner,true) over(partition by sephora_id,card_number,union_id order by maxbanner desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) most_visited_function
            from(
                select sephora_id,card_number,union_id
                ,case when cacnt=max(cacnt) over(partition by sephora_id,card_number,union_id) then category else null end maxca
                ,case when scacnt=max(scacnt) over(partition by sephora_id,card_number,union_id) then subcategory else null end maxsca
                ,case when bcnt=max(bcnt) over(partition by sephora_id,card_number,union_id) then brand else null end maxbrand
                ,case when bancnt=max(bancnt) over(partition by sephora_id,card_number,union_id) then banner_coding else null end maxbanner
                from (
                    select sephora_id,card_number,union_id ,category,subcategory,brand,banner_coding
                    ,case when category is null then null else count(0) over(partition by sephora_id,card_number,union_id,category) end cacnt
                    ,case when subcategory is null then null else count(0) over(partition by sephora_id,card_number,union_id,subcategory) end scacnt
                    ,case when brand is null then null else count(0) over(partition by sephora_id,card_number,union_id,brand) end bcnt
                    ,case when banner_coding is null then null else count(0) over(partition by sephora_id,card_number,union_id,banner_coding) end bancnt
                    from(
                        select temp1.* ,temp2.sephora_id,temp2.card_number,temp2.union_id
                        from da_dev.v_events_session temp1
                        join da_dev.tagging_id_mapping temp2
                        on temp1.user_id=temp2.sensor_id
                        where dt between DATE_ADD(CURRENT_DATE,-181) and DATE_ADD(CURRENT_DATE,-1)
                    )temp
                )t1
            )t2
        )tt2
        on tt1.union_id=tt2.union_id;""")
    t16 = time.clock()
    print('Engagement Cal Temp3 Done: %f' % (t16 - t15))
    t17 = time.clock()
    hive_unit.execute(r"""
        DROP TABLE IF EXISTS da_dev.engagement_tagging;
        create table da_dev.engagement_tagging(
            sephora_id string,
            card_number string,
            union_id string,
            id int,
            most_visited_channel string,
            last_30D_visit int,
            last_30D_bounce string,
            most_visited_category string,
            most_visited_subcategory string,
            most_visited_brand string,
            most_visited_function string,
            average_product_visit int,
            average_staytime string
        );
        insert into da_dev.engagement_tagging(
        sephora_id,card_number,union_id,id,most_visited_channel,last_30D_visit,last_30D_bounce
        ,most_visited_category,most_visited_subcategory,most_visited_brand,most_visited_function
        ,average_product_visit,average_staytime)
        select case when tt1.sephora_id='null' then null when tt1.sephora_id ='NULL' then null when tt1.sephora_id='' then null else tt1.sephora_id end as sephora_id
        ,case when tt1.card_number='null' then null when tt1.card_number ='NULL' then null when tt1.card_number='' then null else tt1.card_number end as card_number
        ,case when tt1.union_id='null' then null when tt1.union_id ='NULL' then null when tt1.union_id='' then null else tt1.union_id end as union_id
        ,row_number() over(order by tt1.union_id) as id
        ,most_visited_channel,last_30D_visit,last_30D_bounce
        ,most_visited_category,most_visited_subcategory,most_visited_brand,most_visited_function
        ,average_product_visit,cast(average_staytime as decimal(18,2)) average_staytime
        from da_dev.engagement_tagging_temp3 tt1
        left join 
        (
            select sephora_id,card_number,union_id,avg(procnt) average_product_visit,avg(sessiontime) average_staytime
            from(
                select platform_type,sephora_id,card_number,temp2.union_id,sessionid
                ,count(distinct product_id) procnt
                ,sum(distinct sessiontime) sessiontime
                from da_dev.v_events_session temp1
                join da_dev.tagging_id_mapping temp2
                on temp1.user_id=temp2.sensor_id
                where dt between DATE_ADD(CURRENT_DATE,-181) and DATE_ADD(CURRENT_DATE,-1)
                group by platform_type,sephora_id,card_number,temp2.union_id,sessionid
            )t1
            group by sephora_id,card_number,union_id
        )tt2
        on tt1.union_id=tt2.union_id ; 
    """)
    t18 = time.clock()
    print('Engagement Cal  Done: %f' % (t18 - t17))


def run_user_engagement_info():
    # def get_date_by_gap(start_date: str, day_gap: int):
    #     """
    #     This function is going to figure out the start date of data time period
    #
    #     :param start_date:
    #     :param day_gap:
    #     :return: start_date: str
    #     """
    #     start_date = datetime.datetime.strftime(
    #         datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(days=day_gap), '%Y-%m-%d')
    #     return start_date
    #
    # hive_unit = HiveUnit(**HIVE_CONFIG)
    # engagement_initial(hive_unit)
    # hive_unit.release()
    # data_str = '2020-02-27'
    # while data_str < '2020-09-22':
    #     print('#' * 20 + 'Engagement Session Start:' + data_str)
    #     hive_unit = HiveUnit(**HIVE_CONFIG)
    #     engagement_fv_session(hive_unit, data_str)
    #     hive_unit.release()
    #     data_str = get_date_by_gap(start_date=data_str, day_gap=30)
    hive_unit = HiveUnit(**HIVE_CONFIG)
    engagement_daily_session(hive_unit)
    hive_unit.release()
    hive_unit = HiveUnit(**HIVE_CONFIG)
    engagement_cal(hive_unit)
    hive_unit.release()

