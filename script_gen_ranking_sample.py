import pandas as pd

from lib.datastructure.config import HIVE_CONFIG
from lib.db.hive_utils import HiveUnit

hive_unit = HiveUnit(**HIVE_CONFIG)
raw_click = hive_unit.get_df_from_db("""select * from da_dev.search_click_aft_search""")
print(raw_click.info())
raw_click = raw_click.dropna()
raw_click_sample = raw_click.head(10000)
print(raw_click_sample)
raw_click_sample.to_csv("sample_click_aft_search.csv", index=None)

hive_unit.release()

