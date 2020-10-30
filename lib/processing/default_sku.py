from lib.datastructure.config import HIVE_CONFIG
from lib.datastructure.files import DEFAULT_SKU
from lib.db.hive_utils import HiveUnit
from lib.utils.utils import dump_json


def gen_default_sku():
    hive_unit = HiveUnit(**HIVE_CONFIG)
    hive_unit.execute(r"""
        drop table if exists da_dev.search_default_sku;
        create table da_dev.search_default_sku
        (
            product_id int,
            sku_id int,
            sku_cd string,
            rn int
        );
        insert into da_dev.search_default_sku
        select product_id,sku_id,sku_cd,rn
        from(
        select tt1.product_id,tt1.sku_id,tt1.sku_cd,tt2.sales
        ,row_number() over(partition by tt1.product_id order by tt1.if_bundle desc, tt2.sales desc) as rn
        from
        (
            select distinct product_id,sku_cd,sku_id
            ,case when sku_cd like 'V%' then 1 else 0 end as if_bundle
            from oms.dim_sku_profile
            where to_date(insert_timestamp)=date_sub(current_date,1)
            and product_id<>0)tt1
        left outer join
        (
           select t1.sku_code,t2.sales
            from
            (
                select distinct sku_code,product_id
                from crm.dim_product
                where product_id<>0
            )t1 left outer join
            (
                select product_id,sum(sales) as sales
                from crm.fact_trans
                where to_date(trans_time) between date_sub(current_date,181) and date_sub(current_date,1)
                and account_id<>0 and product_id<>0
                and sales>0 and qtys>0 and sales/qtys>0
                group by product_id) t2 on t1.product_id=t2.product_id
        )tt2 on tt1.sku_cd=tt2.sku_code)ttt1
        where rn<=5
    """)
    sku = hive_unit.get_df_from_db(
        r"select distinct product_id as op_code, sku_id from da_dev.search_default_sku where rn=1")
    hive_unit.release()
    sku_dic = {}
    for inx, row in sku.iterrows():
        sku_dic[str(row['op_code'])] = str(row['sku_id'])
    print(sku_dic)
    dump_json(sku_dic, DEFAULT_SKU)