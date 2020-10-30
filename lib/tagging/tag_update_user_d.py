import time

from lib.datastructure.config import HIVE_CONFIG
from lib.db.hive_utils import HiveUnit


def id_mapping(hive_unit: HiveUnit):
    t1 = time.clock()
    hive_unit.execute(r"""
        drop table if exists da_dev.tagging_id_mapping_temp;
        create table da_dev.tagging_id_mapping_temp
        (
            sensor_id bigint,
            sephora_id int,
            open_id string
        );
        set hive.execution.engine = tez;
        set tez.queue.name = sephora_internal;
        insert into table da_dev.tagging_id_mapping_temp
        select t1.id as sensor_id
        ,case when t2.user_id is not null then t2.user_id
        when t3.user_id is not null then t3.user_id
        when t4.user_id is not null then t4.user_id
        else t5.user_id end as sephora_id
        ,case when t2.open_id is not null then t2.open_id
        when t3.open_id is not null then t3.open_id
        when t4.open_id is not null then t4.open_id
        else null end as open_id
        from (
            select id,first_id,second_id
            from dwd.v_users
        )t1 left outer join
        (
            select user_id,open_id,union_id
            from oms.dim_wechat_user_info
        )t2 on t1.first_id = t2.open_id --first_id=open_id
        left outer join
        (
            select user_id,open_id,union_id
            from oms.dim_wechat_user_info
        )t3 on t1.second_id=t3.union_id --second_id=union_id
        left outer join
        (
            select user_id,open_id,union_id
            from oms.dim_wechat_user_info
        )t4 on t1.second_id=t4.user_id -- second_id=sephora_id with open_id
        left outer join
        (
            select distinct user_id
            from oms.ods_user_profile
            where dt=date_sub(current_date,1)
        )t5 on t1.second_id=t5.user_id -- second_id=sephora_id without open_id
    """)
    t2 = time.clock()
    print('ID Mapping Temp Done: %f' % (t2 - t1))
    t3 = time.clock()
    hive_unit.execute(r"""
        drop table if exists da_dev.tagging_id_mapping;
        create table da_dev.tagging_id_mapping
        (
            sephora_id string,
            card_number string,
            union_id string,
            account_id string,
            sensor_id bigint,
            open_id string
        );
        set hive.execution.engine = tez;
        set tez.queue.name = sephora_internal;
        insert into table da_dev.tagging_id_mapping
        select sephora_id,card_number
        ,case when sephora_id=''  and card_number<>'' then card_number 
        when sephora_id<>'' and card_number='' then sephora_id 
        when sephora_id<>'' and card_number<>'' then concat(sephora_id,',',card_number) 
        else null end as union_id
        ,account_id,sensor_id,open_id
        from(
        select case when t1.user_id is null or t1.user_id in ('null','NULL',' ','') then '' else t1.user_id end as sephora_id
         ,case when t1.card_no is null or t1.card_no in ('null','NULL',' ','') then '' else t1.card_no end  as card_number
         ,t2.account_id,t3.open_id,t3.sensor_id
        from 
        (
            select user_id,card_no
            from oms.ods_user_profile
            where dt=date_sub(current_date,1)
            and  (user_id rlike '^\\d+$' or user_id is null or user_id in ('null','NULL',' ',''))
            and  (card_no rlike '^\\d+$' or card_no is null or card_no in ('null','NULL',' ',''))
        ) t1
        left outer join
        crm.dim_account t2 on t1.card_no=t2.account_number 
        left outer join
        da_dev.tagging_id_mapping_temp t3 on t1.user_id=t3.sephora_id)tt1
        where not (tt1.sephora_id='' and tt1.card_number='')
    """)
    t4 = time.clock()
    print('ID Mapping Done: %f' % (t4 - t3))


def user_tracking(hive_unit: HiveUnit):
    t5 = time.clock()
    hive_unit.execute(r"""
        DROP TABLE IF EXISTS da_dev.v_events_lastday_user;
        CREATE TABLE da_dev.v_events_lastday_user(user_id string); 
        insert into da_dev.v_events_lastday_user(user_id)
        select distinct user_id
        from dwd.v_events
        where dt=DATE_ADD(CURRENT_DATE,-1);
    """)
    t6 = time.clock()
    print('Tracking User Done: %f' % (t6 - t5))


def user_purchase(hive_unit: HiveUnit):
    t7 = time.clock()
    hive_unit.execute(r"""
        DROP TABLE IF EXISTS da_dev.fact_trans_lastday_user;
        CREATE TABLE da_dev.fact_trans_lastday_user(
        account_id string,
        card_number string);
        insert into da_dev.fact_trans_lastday_user(account_id,card_number)
        select distinct t1.account_id,t2.account_number as card_number
        from 
        (select distinct account_id
        from crm.fact_trans
        where to_date(trans_time)=DATE_ADD(CURRENT_DATE,-1)) t1 left outer join
        crm.dim_account t2 on t1.account_id=t2.account_id;
    """)
    t8 = time.clock()
    print('Trans User Done: %f' % (t8 - t7))


def user_register(hive_unit: HiveUnit):
    t9 = time.clock()
    hive_unit.execute(r"""
         drop table if exists da_dev.new_register;
         create table da_dev.new_register
         (
             sephora_id string,
             card_number string,
             union_id string
         );
         set hive.execution.engine = tez;
         set tez.queue.name = sephora_internal;
         insert into table da_dev.new_register
         select tt1.sephora_id,tt1.card_number,tt1.union_id
         from(
             select sephora_id,card_number,concat(sephora_id,',',card_number) as union_id
             from(
                select case when user_id is null then ''  else user_id end as sephora_id,
                    case when card_no is null then '' else card_no end as card_number
                from oms.ods_user_profile
                where dt=date_sub(current_date,1)
             )t1
        )tt1 left outer join
        (
            select sephora_id,card_number,concat(sephora_id,',',card_number) as union_id
            from(
                select case when user_id is null then ''  else user_id end as sephora_id,
                    case when card_no is null then '' else card_no end as card_number
                from oms.ods_user_profile
                where dt=date_sub(current_date,2)
             )t1
        )tt2 on tt1.union_id=tt2.union_id
        where tt2.union_id is null
    """)
    t10 = time.clock()
    print('New Register Done: %f' % (t10 - t9))


def user_combine(hive_unit: HiveUnit):
    t11 = time.clock()
    hive_unit.execute(r"""
    drop table if exists da_dev.update_user;
    create table da_dev.update_user
    (
        sephora_id string,
        card_number string,
        union_id string,
        create_date string
    );
    insert into table da_dev.update_user
    select sephora_id,card_number,union_id,current_date as create_date
    from(
    select sephora_id,card_number,union_id
    from  da_dev.new_register
    union
    select t1.sephora_id,t1.card_number,t1.union_id
    from
    da_dev.tagging_id_mapping t1 left outer join
    da_dev.fact_trans_lastday_user t2 on t1.card_number=t2.card_number
    where t2.card_number is not null
    union
    select t1.sephora_id,t1.card_number,t1.union_id
    from
    da_dev.tagging_id_mapping t1 left outer join
    da_dev.v_events_lastday_user t2 on t1.sensor_id=t2.user_id
    where t2.user_id is not null)tt1
    """)
    t12 = time.clock()
    print('Update User Done: %f' % (t12 - t11))


def run_update_user():
    # hive_unit = HiveUnit(**HIVE_CONFIG)
    # id_mapping(hive_unit)
    # hive_unit.release()
    # hive_unit = HiveUnit(**HIVE_CONFIG)
    # user_register(hive_unit)
    # hive_unit.release()
    # hive_unit = HiveUnit(**HIVE_CONFIG)
    # user_purchase(hive_unit)
    # hive_unit.release()
    # hive_unit = HiveUnit(**HIVE_CONFIG)
    # user_tracking(hive_unit)
    # hive_unit.release()
    hive_unit = HiveUnit(**HIVE_CONFIG)
    user_combine(hive_unit)
    hive_unit.release()


if __name__ == '__main__':
    run_update_user()
