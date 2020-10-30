"""
generate historical query with counts which are searched greater than 20 times in last 360 days.
"""
from lib.db.hive_utils import HiveUnit


def gen_hisquery(hive_unit: HiveUnit):
    hive_unit.execute(r"""
    drop table if exists da_dev.historical_query_360d;
    create table da_dev.historical_query_360d
    (
        query string,
        search_cnt int
    );
    insert into table da_dev.historical_query_360d
    select query,count(distinct user_id) as search_cnt
    from(
    select case when event= '$MPViewScreen' then key_words  else banner_content end as query,user_id
    from dwd.v_events 
    where dt between date_sub(current_date,361) and date_sub(current_date,1) 
    and ((event= '$MPViewScreen' and page_type_detail='search_list') or
        (event= 'clickBanner_MP' and banner_belong_area in ('search','searchview')))
    and ((key_words <>'null' and key_words<>'NULL' and key_words<>'') or
        (banner_content<>'null' and banner_content<>'NULL' and banner_content<>''))
    and platform_type='MiniProgram')t1
    group by query
    having count(distinct user_id)>20
    """)


def run_historical_query():
    hive_unit = HiveUnit(**HIVE_CONFIG)
    gen_hisquery(hive_unit)
    hive_unit.release()