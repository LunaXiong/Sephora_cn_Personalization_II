import time

from lib.datastructure.config import HIVE_CONFIG
from lib.db.hive_utils import HiveUnit


def store_tagging(hive_unit: HiveUnit):
    """
    This function is to generate basic info and sales info for each store
    This part can be refreshed monthly
    :param hive_unit: the connector
    :return:
    """
    t1 = time.clock()
    hive_unit.execute(r"""
    DROP TABLE IF EXISTS da_dev.store_tagging;   
    CREATE TABLE da_dev.store_tagging(
        store_id int,
        online_offline string,
        province string,
        city string,
        city_tier string,
        tradezone_type string,
        monthly_sales string
    ) ;
    insert into da_dev.store_tagging(store_id,online_offline,province,city,city_tier,monthly_sales)  
    select case when t1.store_id='null' then null when t1.store_id ='NULL' then null when t1.store_id='' then null else t1.store_id end as store_id
    ,case when is_eb_store=2 then 'online' else 'offline' end as online_offline
    ,case when t1.province='null' then null when t1.province ='NULL' then null when t1.province='' then null else t1.province end as province
    ,case when t3.standardcityname='null' then null when t3.standardcityname ='NULL' then null when t3.standardcityname='' then null else t3.standardcityname end as city
    ,case when t4.citytiercode='null' then null when t4.citytiercode ='NULL' then null when t4.citytiercode='' then null else t4.citytiercode end as city_tier
    ,round(monthly_sales,2) as monthly_sales
    from crm.dim_store t1
    left join(
        select store_id,sum(total_sales) as monthly_sales
        from  crm.dim_trans 
        where total_sales>0 and total_sales/total_qtys<20000
        and trans_time between trunc(add_months(current_date,-1),'MM') and last_day(add_months(current_date,-1)) 
        group by store_id
    )t2
    on t1.store_id=t2.store_id 
    left outer join
    (
        select distinct originalcityname,standardcityname
        from da_dev.city_coding_city
        where standardcityname<>'error'
        and originalcityname<>'')t3 on t1.city=t3.originalcityname
    left outer join
    da_dev.bds_city_list t4 on t3.standardcityname=regexp_replace(t4.city,'市','');
    """)
    t2 = time.clock()
    print('Store Information Done: %f' % (t2 - t1))


def run_store_info():
    hive_unit = HiveUnit(**HIVE_CONFIG)
    store_tagging(hive_unit)
    hive_unit.release()
