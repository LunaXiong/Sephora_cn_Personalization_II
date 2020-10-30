import time

from lib.datastructure.config import HIVE_CONFIG
from lib.db.hive_utils import HiveUnit


def product_tagging(hive_unit: HiveUnit):
    hive_unit.execute(r"""
    drop table if exists da_dev.product_tagging_temp;
    create table da_dev.product_tagging_temp
    (
        product_id int,
        customer_status_EB string,
        pennetration float
    );
    insert into table da_dev.product_tagging_temp
    select tt1.product_id,tt1.customer_status_EB,tt1.user_cnt/tt2.user_cnt as pennetration
    from(
        select t1.product_id,t3.customer_status_EB,count(distinct t3.card_number) as user_cnt
        from crm.fact_trans t1
        left outer join
        crm.dim_account t2 on t1.account_id=t2.account_id
        left outer join
        da_dev.user_basic_info_tagging t3 on t2.account_number=t3.card_number
        group by t1.product_id,t3.customer_status_EB
    )tt1 left outer join
    (
        select customer_status_EB,count(distinct card_number) as user_cnt
        from da_dev.user_basic_info_tagging 
        group by customer_status_EB
    )tt2 on tt1.customer_status_EB=tt2.customer_status_EB
    """)
    print('Temp1 Done')
    hive_unit.execute(r"""
    drop table if exists da_dev.product_tagging_temp2;
    create table da_dev.product_tagging_temp2
    (
        customer_status_EB string,
        quantile float
    );
    insert into da_dev.product_tagging_temp2
    select customer_status_EB,percentile_approx(pennetration, 0.9) as quantile
    from da_dev.product_tagging_temp
    where customer_status_EB <>''
    group by customer_status_EB;
    
    drop table if exists da_dev.product_tagging_temp3;
    create table da_dev.product_tagging_temp3
    (
        product_id int,
        customer_segment string
    );
    insert into table da_dev.product_tagging_temp3
    select tt1.product_id,concat_ws(',',collect_set(tt1.customer_status_EB)) as customer_segment
    from (
        select t1.product_id,t1.customer_status_EB
        from da_dev.product_tagging_temp t1
        left outer join 
        da_dev.product_tagging_temp2 t2
        on t1.customer_status_EB=t2.customer_status_EB
        where t1.pennetration>t2.quantile
    )tt1
    group by tt1.product_id;
    """)
    print('Temp2&Temp3 Done')
    hive_unit.execute(r"""
    drop table if exists da_dev.product_tagging_ret;
    create table da_dev.product_tagging_ret
    (
        product_id int,
        sku_code string,
        sku_name string,
        category string,
        subcategory string, 
        thirdcategory string,
        brand string,	
        product_line string,	
        nickname string,	
        brand_origin string,	
        skincare_function_basic string,	
        skincare_function_special string,	
        skincare_ingredients string,	
        makeup_function string,	
        makeup_feature_look string,	
        makeup_feature_color string,	
        makeup_feature_scene string,	
        target_agegroup string,	
        function_segmented string,	
        skintype string,	
        fragrance_targetgender string,	
        fragrance_stereotype string,	
        fragrance_intensity string,	
        fragrance_impression string,	
        fragrance_type string,	
        bundleproduct_festival string,	
        bundleproduct_main_sku string,	
        bundleproduct_main_sku_function string,	
        bundleproduct_opmix string,
        customer_segment string
    );
    insert into table da_dev.product_tagging_ret
    select t1.product_id,t1.sku_code,sku_name,t1.category,subcategory,thirdcategory,t1.brand,product_line,nickname,brand_origin
    ,skincare_function_basic,skincare_function_special,skincare_ingredients,makeup_function,makeup_feature_look
    ,makeup_feature_color,makeup_feature_scene,target_agegroup,function_segmented,skintype,fragrance_targetgender
    ,fragrance_stereotype,fragrance_intensity,fragrance_impression,fragrance_type,bundleproduct_festival
    ,bundleproduct_main_sku,bundleproduct_main_sku_function,bundleproduct_opmix,t3.customer_segment
    from da_dev.search_prod_list t1 left outer join
    (select distinct product_id,sku_code
    from crm.dim_product) t2 on  t1.sku_code=t2.sku_code left outer join
    da_dev.product_tagging_temp3 t3 on t2.product_id=t3.product_id
    """)
    print('Product info Done')


def run_prod_info():
    hive_unit = HiveUnit(**HIVE_CONFIG)
    product_tagging(hive_unit)
    hive_unit.release()


if __name__ == '__main__':
    run_prod_info()
