import pandas as pd


def load_feature_data(ver='prod'):
    if ver == 'prod':
        # Encoded user feature
        user_feature_df = pd.read_csv("../data/ranking_user_df.csv")

        # processed and encoded item feature
        item_feature_df = pd.read_csv("../data/ranking_item_df.csv")

        return user_feature_df, item_feature_df

    else:
        pass

