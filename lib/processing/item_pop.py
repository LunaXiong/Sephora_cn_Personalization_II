import pandas as pd

from lib.datastructure.config import HIVE_CONFIG
from lib.datastructure.files import ITEM_TAG_FILE, KW2ITEM_NEW, KW_POP, KW_POP_TOP
from lib.db.db_unit import MySQLUnit
from lib.db.hive_utils import HiveUnit
from lib.utils.utils import dump_json, load_json


def click_pop(hive_unit: HiveUnit, n_day: int):
    click_cnt = hive_unit.get_df_from_db(r"""
    select op_code,count(distinct user_id) as pop_cnt
    from dwd.v_events
    where event in ('viewCommodityDetai','ListProductClick','$MPViewScreen','PDPClick')
    and dt between date_sub(current_date,{n_day}+1) and date_sub(current_date,1)
    and op_code rlike '^\\d+$'
    group by op_code
    """.format(n_day=n_day))
    return click_cnt


def add_pop(hive_unit: HiveUnit, n_day: int):
    add_cnt = hive_unit.get_df_from_db(r"""
    select op_code,count(distinct user_id) as pop_cnt
    from dwd.v_events
    where event in ('startAddToShoppingcart','addToShoppingcart','startBuyNow','buyNow')
    and dt between date_sub(current_date,{n_day}+1) and date_sub(current_date,1)
    group by op_code
    """.format(n_day=n_day))
    return add_cnt


def purchase_pop(hive_unit: HiveUnit, n_day: int):
    purchase_cnt = hive_unit.get_df_from_db(r"""
    select t1.op_code,count(distinct t2.account_id) as pop_cnt
    from da_dev.search_prod_id_mapping t1
    left outer join
    (select account_id, product_id
    from crm.fact_trans
     where account_id<>0
     and sales>0 and qtys>0 and sales/qtys<20000
     and to_date(trans_time) between date_sub(current_date,{n_day}+1) and date_sub(current_date,1))t2 on t1.crm_prod_id=t2.product_id
    group by t2.account_id
    """.format(n_day=n_day))
    return purchase_cnt


def online_prod(mysql_unit: MySQLUnit):
    prod_online = mysql_unit.get_df("select distinct sku_code from product.prod_sku where status=1")
    prod_online = "\',\'".join(prod_online['sku_code'])
    return prod_online


def all_tag():
    hive_unit = HiveUnit(**HIVE_CONFIG)
    cols = hive_unit.get_df_from_db("show columns from da_dev.search_prod_list")['field']
    prod_tag_all = []
    for col in cols:
        prod_tags = hive_unit.get_df_from_db(r"""
        select distinct product_id,{col} as tag
        from da_dev.search_prod_list
        where {col} <>''
        """.format(col=col))
        prod_tag = pd.DataFrame({'kw': [], 'kw_type': []})
        prod_tag['kw'] = list(set(prod_tags['tag']))
        prod_tag['kw_type'] = col
        prod_tag_all.append(prod_tag)
    prod_tag_all = pd.concat(prod_tag_all)
    prod_tag_all.to_csv('D:/search_test/prod_tag_all.csv', index=False, encoding='utf_8_sig')
    hive_unit.release()


def reset_mapping(hive_unit: HiveUnit, fn: str):
    """
    Run if product tag or keyword-tag mapping have been changed
    :param hive_unit:
    :param fn: initial 'D:/search_test/ProductTag 20201014.xlsx'
    :return:
    """
    hive_unit.execute("drop table if exists da_dev.search_kw_tag_mapping")
    hive_unit.df2db(pd.read_excel(fn), 'da_dev.search_kw_tag_mapping')
    hive_unit.execute(r"""
                drop table if exists da_dev.search_prod_tag_mapping_v2;
                create table da_dev.search_prod_tag_mapping_v2
                (
                    product_id int,
                    brand string,
                    thirdcategory string,
                    nickname string,
                    bundleproduct_main_sku_function string,
                    skincare_function_special string,
                    makeup_feature_color string,
                    skincare_function_basic string,
                    makeup_function string,
                    product_line string,
                    makeup_feature_scene string,
                    skintype string,
                    fragrance_impression string,
                    bundleproduct_opmix string,
                    fragrance_stereotype string,
                    fragrance_targetgender string,
                    bundleproduct_festival string,
                    makeup_feature_look string,
                    fragrance_type string,
                    fragrance_intensity string,
                    target_agegroup string,
                    brand_thirdcategory string
                );
                insert into table da_dev.search_prod_tag_mapping_v2
                select  product_id,brand,thirdcategory,nickname
                ,bundleproduct_main_sku_function,skincare_function_special,makeup_feature_color
                ,skincare_function_basic,makeup_function,product_line,makeup_feature_scene,skintype
                ,fragrance_impression,bundleproduct_opmix,fragrance_stereotype,fragrance_targetgender
                ,bundleproduct_festival,makeup_feature_look,fragrance_type,fragrance_intensity,target_agegroup
                ,brand_thirdCategory
                from(
                        select product_id,thirdcategory,nickname
                            ,brand,bundleproduct_main_sku_function,skincare_function_special,makeup_feature_color
                            ,skincare_function_basic,makeup_function ,product_line ,makeup_feature_scene ,skintype
                            ,fragrance_impression,bundleproduct_opmix ,fragrance_stereotype,fragrance_targetgender
                            ,bundleproduct_festival ,makeup_feature_look,fragrance_type ,fragrance_intensity
                            ,target_agegroup
                            ,case when nickname rlike '[a-zA-Z]' then concat(brand,' ',nickname) else concat(brand,nickname) end as brand_nickname
                            ,case when product_line rlike '[a-zA-Z]' then concat(brand,' ',product_line) else concat(brand,product_line) end as brand_productline
                            ,case when thirdcategory rlike '[a-zA-Z]' then concat(brand,' ',thirdcategory)
                            else concat(brand,thirdcategory) end as brand_thirdcategory
                            ,concat(brand,detailedcategory) as brand_detailedcategory
                            ,concat(detailedcategory,brand) as detailedcategory_brand
                            ,case when thirdcategory rlike '[a-zA-Z]' then concat(thirdcategory,' ',brand)
                                else concat(thirdcategory,brand) end as thirdcategory_brand
                            ,case when nickname rlike '[a-zA-Z]' then concat(nickname,' ',brand) else concat(nickname,brand) end as nickname_brand
                            ,case when product_line rlike '[a-zA-Z]' then concat(product_line,' ',brand) else concat(product_line,brand) end as productline_brand
                            ,case when category rlike '[a-zA-Z]' then concat(category,' ',thirdcategory) else concat(category,thirdcategory) end as category_thirdcategory
                            ,case when subcategory rlike '[a-zA-Z]' then concat(subcategory,' ',thirdcategory) else concat(subcategory,thirdcategory) end  as subcategory_thirdcategory
                            ,concat(thirdcategory,detailedcategory) as thirdcategory_detailedcategory
                            ,row_number() over(partition by product_id order by case when sku_code like 'V%' then 1 else 0 end desc) as rn
                        from da_dev.search_prod_list
                    )t1
                where rn=1
            """)
    hive_unit.execute(r"""
            drop table if exists da_dev.search_prod_tag_mapping;
            create table da_dev.search_prod_tag_mapping
            (
                product_id int,
                brand string,
                thirdcategory string,
                nickname string,
                bundleproduct_main_sku_function string,
                skincare_function_special string,
                makeup_feature_color string,
                skincare_function_basic string,
                makeup_function string,
                product_line string,
                makeup_feature_scene string,
                skintype string,
                fragrance_impression string,
                bundleproduct_opmix string,
                fragrance_stereotype string,
                fragrance_targetgender string,
                bundleproduct_festival string,
                makeup_feature_look string,
                fragrance_type string,
                fragrance_intensity string,
                target_agegroup string,
                brand_thirdcategory string,
                brand_nickname string,
                brand_productline string,
                brand_detailedcategory string,
                thirdcategory_brand string,
                nickname_brand string,
                productline_brand string,
                detailedcategory_brand string,
                category_thirdcategory string,
                subcategory_thirdcategory string,
                thirdcategory_detailedcategory string
            );
            insert into table da_dev.search_prod_tag_mapping
            select  product_id,brand,thirdcategory,nickname
            ,bundleproduct_main_sku_function,skincare_function_special,makeup_feature_color
            ,skincare_function_basic,makeup_function,product_line,makeup_feature_scene,skintype
            ,fragrance_impression,bundleproduct_opmix,fragrance_stereotype,fragrance_targetgender
            ,bundleproduct_festival,makeup_feature_look,fragrance_type,fragrance_intensity,target_agegroup
            ,brand_thirdCategory,brand_nickname ,brand_productline,brand_detailedcategory
            ,thirdcategory_brand,nickname_brand,productline_brand,detailedcategory_brand,category_thirdcategory
            ,subcategory_thirdcategory ,thirdcategory_detailedcategory 
            from(
                    select product_id,thirdcategory,nickname
                        ,brand,bundleproduct_main_sku_function,skincare_function_special,makeup_feature_color
                        ,skincare_function_basic,makeup_function ,product_line ,makeup_feature_scene ,skintype
                        ,fragrance_impression,bundleproduct_opmix ,fragrance_stereotype,fragrance_targetgender
                        ,bundleproduct_festival ,makeup_feature_look,fragrance_type ,fragrance_intensity
                        ,target_agegroup
                        ,case when nickname rlike '[a-zA-Z]' then concat(brand,' ',nickname) else concat(brand,nickname) end as brand_nickname
                        ,case when product_line rlike '[a-zA-Z]' then concat(brand,' ',product_line) else concat(brand,product_line) end as brand_productline
                        ,case when thirdcategory rlike '[a-zA-Z]' then concat(brand,' ',thirdcategory)
                        else concat(brand,thirdcategory) end as brand_thirdcategory
                        ,concat(brand,detailedcategory) as brand_detailedcategory
                        ,concat(detailedcategory,brand) as detailedcategory_brand
                        ,case when thirdcategory rlike '[a-zA-Z]' then concat(thirdcategory,' ',brand)
                            else concat(thirdcategory,brand) end as thirdcategory_brand
                        ,case when nickname rlike '[a-zA-Z]' then concat(nickname,' ',brand) else concat(nickname,brand) end as nickname_brand
                        ,case when product_line rlike '[a-zA-Z]' then concat(product_line,' ',brand) else concat(product_line,brand) end as productline_brand
                        ,case when category rlike '[a-zA-Z]' then concat(category,' ',thirdcategory) else concat(category,thirdcategory) end as category_thirdcategory
                        ,case when subcategory rlike '[a-zA-Z]' then concat(subcategory,' ',thirdcategory) else concat(subcategory,thirdcategory) end  as subcategory_thirdcategory
                        ,concat(thirdcategory,detailedcategory) as thirdcategory_detailedcategory
                        ,row_number() over(partition by product_id order by case when sku_code like 'V%' then 1 else 0 end desc) as rn
                    from da_dev.search_prod_list
                )t1
            where rn=1
        """)


def run_kw_item_pop(pop_type: str, hive_unit: HiveUnit, n_day: int):
    # tags-products mapping
    prod_tags = pd.read_excel(ITEM_TAG_FILE)
    cols = prod_tags.columns.to_list()
    cols.remove('product_id')
    prod_tag_dic = {}
    for col in cols:
        tag_df = prod_tags[['product_id', col]].groupby(col)['product_id'].apply(lambda x: x.to_list()).reset_index()
        for inx, row in tag_df.iterrows():
            prod_tag_dic[row[col]] = list(set(row['product_id']))

    # keyword-tags mapping
    tag_kw_df = hive_unit.get_df_from_db("select kw, kw_standard from da_dev.search_kw_tag_mapping")
    tag_kw_dic = {}
    for inx, row in tag_kw_df.iterrows():
        tag_kw_dic[row['kw']] = row['kw_standard']

    # keywords-products mapping
    prod_kw_dic = {}
    for k, v in tag_kw_dic.items():
        prod_kw_dic[k] = prod_tag_dic.get(v)
    dump_json(prod_kw_dic, KW2ITEM_NEW)

    # product popularity
    if pop_type == 'click':
        pop_df = click_pop(hive_unit, n_day)
    elif pop_type == 'purchase':
        pop_df = purchase_pop(hive_unit, n_day)
    else:
        print('Pop Type Not Support!')
        return None

    pop_dic = {}
    pop_df = pop_df.dropna()
    print(pop_df)
    for inx, row in pop_df.iterrows():
        pop_dic[str(int(row['op_code']))] = row['pop_cnt']
    dump_json(pop_dic, 'D:/search_test/item_click.json')
    pop_dic = load_json('D:/search_test/item_click.json')
    prod_kw_dic = load_json(KW2ITEM_NEW)
    kw_pop = {}
    kw_prod = {}
    kw_avg_pop = {}
    for k, v in prod_kw_dic.items():
        pop_cnt = 0
        prod_cnt = 0
        if v:
            for prod in v:
                pop_cnt += pop_dic.get(str(int(prod)), 0)
                prod_cnt += 1
        kw_pop[k.upper()] = pop_cnt
        kw_prod[k.upper()] = prod_cnt
        if prod_cnt > 0:
            kw_avg_pop[k.upper()] = pop_cnt / prod_cnt
    kw_pop = pd.DataFrame.from_dict(kw_pop, orient='index').reset_index().rename(
        columns={'index': 'kw', '0': 'click_cnt'})
    kw_prod = pd.DataFrame.from_dict(kw_prod, orient='index').reset_index().rename(
        columns={'index': 'kw', '0': 'prod_cnt'})
    kw_pop = pd.merge(kw_prod, kw_pop, on='kw', how='left').rename(
        columns={'0_x': 'no. of product', '0_y': 'no. of click'})
    kw_pop.to_csv(KW_POP, index=False, encoding='utf_8_sig')


def gen_top_op(topn: int):
    pop_dic = load_json('D:/search_test/item_click.json')
    prod_kw_dic = load_json(KW2ITEM_NEW)
    kw_pop = {}
    kw_prod = {}
    kw_avg_pop = {}
    for k, v in prod_kw_dic.items():
        pop_cnt = 0
        prod_cnt = 0
        if v:
            v.sort(key=lambda prod: pop_dic.get(str(int(prod)), 0), reverse=True)
            v = v[: topn]
            for prod in v:
                pop_cnt += pop_dic.get(str(int(prod)), 0)
                prod_cnt += 1
        kw_pop[k.upper()] = pop_cnt
        kw_prod[k.upper()] = prod_cnt
        if prod_cnt > 0:
            kw_avg_pop[k.upper()] = pop_cnt / prod_cnt
    kw_pop = pd.DataFrame.from_dict(kw_pop, orient='index').reset_index().rename(
        columns={'index': 'kw', '0': 'click_cnt'})
    kw_prod = pd.DataFrame.from_dict(kw_prod, orient='index').reset_index().rename(
        columns={'index': 'kw', '0': 'prod_cnt'})
    kw_pop = pd.merge(kw_prod, kw_pop, on='kw', how='left').rename(
        columns={'0_x': 'no. of product', '0_y': 'no. of click'})
    kw_pop.to_csv(KW_POP_TOP, index=False, encoding='utf_8_sig')


if __name__ == '__main__':
    hive_unit = HiveUnit(**HIVE_CONFIG)
    # # reset_mapping(hive_unit=hive_unit, fn='D:/search_test/ProductTag 20201016.xlsx')
    run_kw_item_pop(pop_type='click', hive_unit=hive_unit, n_day=90)
    # # df = hive_unit.get_df_from_db("select * from da_dev.inffered_tagging where card_number=100772")
    # # print(df)
    hive_unit.release()
    # top_op(topn=4)
    # all_tag()