import pandas as pd
import lightgbm as lgb
from sklearn import preprocessing

df_lable = pd.read_csv("./data/index_df.csv")
df_item_profile = pd.read_csv("./data/item_profile.csv")
df_item_profile.rename(columns={'product_id': 'op_code'}, inplace=True)
df_user_profile = pd.read_csv("./data/search_user_profile.csv")

df_item_lable = pd.merge(df_lable, df_item_profile, how='left', on='op_code')
df_lable_item_user = pd.merge(df_item_lable, df_user_profile, how='left', on='open_id')
df_lable_item_user = df_lable_item_user.drop(['time', 'query', 'open_id', 'op_code'], axis=1)


def col_process(df, col_list):
    lbl = preprocessing.LabelEncoder()
    for col in col_list:
        df[col] = lbl.fit_transform(df[col].astype(str))


col_list = ['standardskuname', 'category', 'subcategory', 'thirdcategory', 'brand', 'brand_origin',
            'skincare_function_basic', 'skincare_function_special', 'makeup_function', 'makeup_feature_look',
            'makeup_feature_color', 'target_agegroup', 'skintype', 'fragrance_targetgender', 'fragrance_stereotype',
            'fragrance_impression', 'gender', 'city', 'customer_status_eb', 'member_origin_channel',
            'member_origin_category', 'member_cardtype', 'most_visited_category', 'most_visited_subcategory',
            'most_visited_brand', 'most_visited_function', 'preferred_category', 'preferred_subcategory',
            'preferred_thirdcategory', 'preferred_brand', 'skin_type', 'makeup_maturity', 'skincare_maturity',
            'makeup_price_range', 'skincare_price_range', 'skincare_demand', 'makeup_demand', 'fragrance_demand',
            'shopping_driver']

col_process(df_lable_item_user, col_list)
train = df_lable_item_user
target = 'label'
train_data = lgb.Dataset(train.drop(columns=[target]), label=train[target])
validation_data = lgb.Dataset(train.drop(columns=[target]), label=train[target])
params = {
    'objective': 'binary',
    'learning_rate': 0.1,
    'min_gain_to_split': 0.04,
    'num_round': 1000,
    'min_data_in_leaf': 25,
    'metric': {'binary_logloss', 'auc'},
}

clf = lgb.train(params, train_data, valid_sets=[validation_data])
clf.save_model('./data/model.txt')

gbm = lgb.Booster(model_file='./data/model.txt')
y_pred = gbm.predict(None, num_iteration=gbm.best_iteration)

