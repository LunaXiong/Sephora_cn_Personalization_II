from lib.processing.default_sku import gen_default_sku
from lib.processing.user_item_profile import run_user_item_profile
from lib.processing.search_session import daily_insert
from lib.processing.for_ranking import run_click_aft_search


def run_hive_table_update():
    print("#"*20 + 'Refresh hive data tables ...')
    print("#"*20 + 'Refresh user item profile...')
    run_user_item_profile()
    print("#"*20 + 'Refresh search session and search behavior...')
    daily_insert()
    print("#"*20 + 'Refresh click after search...')
    run_click_aft_search()
    print("#"*20 + 'Refresh default sku...')
    gen_default_sku()
