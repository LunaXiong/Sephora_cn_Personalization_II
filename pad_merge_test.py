import pandas as pd
from datetime import datetime

from lib.model.ranking import RegressionRanking
from lib.processing.for_ranking import gen_index, LastClick
from lib.utils.utils import today
from web_app.service_ranking import RankingService

BEHAVIOR_WITH_NEG = "./data/sample_click_aft_search_neg_10000.csv"
TODAY = today()
INDEX_COLS = ['open_id', 'op_code', 'time']
USER_FN = "./data/search_user_profile.csv"
ITEM_FN = "./data/product_list.csv"

# Load index
raw_df = pd.read_csv(BEHAVIOR_WITH_NEG)
raw_df['time'] = pd.to_datetime(raw_df['time'])

# Extract negative samples
index_df = gen_index(raw_df)

# Generate features
# Last click delta
last_click = LastClick()
last_click.gen_click_map(raw_df)
last_click_feature = last_click.gen_last_click(index_df, TODAY)
last_click_feature = last_click_feature[INDEX_COLS + ['last_click']]

user_df = pd.read_csv(USER_FN)
item_df = pd.read_csv(ITEM_FN)

ranking_model = RegressionRanking()

rs = RankingService(last_click, user_df, item_df, ranking_model)

sample_user_item_pairs = pd.DataFrame({'open_id': ['oCOkA5euKFYbnRuqeG6AZJv4R_qw',
                                                   'oCOkA5euKFYbnRuqeG6AZJv4R_qw',
                                                   'oCOkA5euKFYbnRuqeG6AZJv4R_qw',
                                                   'oCOkA5euKFYbnRuqeG6AZJv4R_qw',
                                                   'oCOkA5euKFYbnRuqeG6AZJv4R_qw',
                                                   'oCOkA5euKFYbnRuqeG6AZJv4R_qw',
                                                   'oCOkA5euKFYbnRuqeG6AZJv4R_qw',
                                                   'oCOkA5euKFYbnRuqeG6AZJv4R_qw',
                                                   'oCOkA5euKFYbnRuqeG6AZJv4R_qw',
                                                   'oCOkA5euKFYbnRuqeG6AZJv4R_qw'],
                                       'op_code': ['987065', '982974',
                                                   '464004', '464004',
                                                   '1819', '987065',
                                                   '981987', '959527',
                                                   '969553', '982772']})
tc = []
for i in range(10):
    t1 = datetime.now()
    rs._rank(sample_user_item_pairs)
    t2 = datetime.now()
    tc.append(t2 - t1)

print("Min: %s" % min(tc))
print("Max: %s" % max(tc))