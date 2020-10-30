import pickle

import lightgbm
import numpy as np
import pandas as pd


# class DataAssembler:
#     def __init__(self, query_emb_model, user_encoder=None, item_encoder=None):
#         self.user_encoder = UserEncoder() if not user_encoder else user_encoder
#         self.item_encoder = ItemEncoder() if not item_encoder else item_encoder
#         self.query_processor = QueryProcessor(query_emb_model)
#
#     def process_train_users(self, raw_df):
#         rank_df = self.user_encoder.gen_for_ranking(raw_df)
#         return rank_df
#
#     def process_train_items(self, raw_df):
#         rank_df = self.item_encoder.gen_for_ranking(raw_df)
#         return rank_df
#
#     def process_train_queries(self):
#         pass


def _sample_weight(raw_score: np.ndarray, alpha=40, epsilon=10 ** (-6)):
    """
    calculate the weight of instance (user_id, op_code)
    raw score, r_ui = n_clicks + 3 * n_addCart + 5 * n_purchase
    sample weight, c_ui = 1 + alpha * log(1 + r_ui/epsilon)
    :param raw_score:
    :param alpha:
    :param epsilon:
    :return:
    """
    if epsilon == 0:
        epsilon = 10 ** (-6)
    sample_weight = 1 + alpha * np.log(1 + raw_score / epsilon)
    return sample_weight


def _cal_raw_score(df: pd.DataFrame, w_click: int = 1, w_cart: int = 3, w_purchase: int = 5):
    """
    calculate raw score for pair(user_id, op_code) with history click, addToCart, purchase behaviors
    raw score, r_ui = n_clicks + 3 * n_addCart + 5 * n_purchase
    :param df: pd.DataFrame, must has 'history_click', 'history_add', 'history_purchase' columns
    :param w_click: int, weight of click behavior, default is 1
    :param w_cart: int, weight of add to cart behavior, default is 3
    :param w_purchase: int, weight of purchase behavior, default is 5
    :return: ndarray, [n_samples, ]
    """

    raw_score = w_click * df['history_click'] + w_cart * df['history_add'] + w_purchase * df['history_purchase']
    return raw_score.values


class RegressionRanking:
    def __init__(self, index_cols=('open_id', 'op_code'), y_col='label', model=None, feature_cols=None):
        # Different API of lightgbm: https://lightgbm.readthedocs.io/en/latest/Python-API.html
        # parameters for training API: train and cv
        self.default_param = {
            'task': 'train',
            'application': 'regression',
            'boosting': 'gbdt',
            'learning_rate': 0.1,
            'num_leaves': 63,
            'tree_learner': 'serial',
            'metric': ['auc', 'binary_logloss'],
            'max_bin': 63,
            'max_depth': 7,
        }

        self.params = None
        self.default_sample_weight_params = {
            'alpha': 40, 'epsilon': 10 ** (-6),
            'w_click': 1, 'w_cart': 3, 'w_purchase': 5
        }
        self.index_cols = list(index_cols)
        self.y_col = y_col
        self.feature_cols = feature_cols

        self.model = model
        # tree-lite
        self.tl_predictor = None

    def incremental_training(self, data, params=None, return_feature_importance=False, sample_weight=True,
                             sample_weight_params=None):
        if params:
            self.default_param.update(params)
        params = self.default_param

        if sample_weight:
            if sample_weight_params:
                self.default_sample_weight_params.update(sample_weight_params)

        if sample_weight:
            if sample_weight_params:
                self.default_sample_weight_params.update(sample_weight_params)
            raw_score_params = {'w_click': self.default_sample_weight_params['w_click'],
                                'w_cart': self.default_sample_weight_params['w_cart'],
                                'w_purchase': self.default_sample_weight_params['w_purchase']}
            weight_params = {
                'alpha': self.default_sample_weight_params['alpha'],
                'epsilon': self.default_sample_weight_params['epsilon'],
            }
            raw_score = _cal_raw_score(data, **raw_score_params)
            weight = _sample_weight(raw_score, **weight_params)
        else:
            weight = None

        x_data, y_data = self._x_y_split(data)
        train_data = lightgbm.Dataset(x_data, y_data, weight=weight)
        model = self.model
        lgbm = lightgbm.train(params,
                              train_data,
                              num_boost_round=100,
                              valid_sets=[train_data],
                              valid_names=['train'],
                              init_model=model,
                              early_stopping_rounds=10,
                              keep_training_booster=True,
                              )
        self.model = lgbm

        if return_feature_importance:
            return self.get_feature_importance()

    def predict(self, X: pd.DataFrame, reserve_feature_cols=True, treelite=True, with_leaf=False):
        """
        predict with given X
        :param X:
        :param reserve_feature_cols:
        :param treelite: whether to user treelite to accelerate
        :param with_leaf: whether to predict left index in every tree, set with True when for LGBM2LR
        :return:
        """
        if not self.model:
            raise RuntimeError('Empty model, use self.fit to generate a model or self.load to load a model')
        feed_data = X[self.feature_cols].values

        if with_leaf:
            return self.model.predict(feed_data, pred_leaf=True)

        if not treelite:
            pred_ret = self.model.predict(feed_data)
        else:
            if not self.tl_predictor:
                from lib.model.treelite_pred import TreeLitePredictor
                self.tl_predictor = TreeLitePredictor(self)
            pred_ret = self.tl_predictor.predict(feed_data)

        # X.sort_values("prediction", inplace=True, ascending=False)
        if reserve_feature_cols:
            X['prediction'] = pred_ret
            return X
        else:
            return pd.concat((X[self.index_cols], pd.DataFrame({'prediction': pred_ret})), axis=1)

    def save(self, filepath):
        """
        save the model
        :param filepath:
        :return:
        """
        try:
            with open(filepath, 'wb') as p:
                pickle.dump([self.model, self.feature_cols, self.y_col], p)
        except:
            raise IOError("Cannot open file %s" % filepath)

    @classmethod
    def load(cls, filepath: str, load_tl=False):
        """
        Load the model
        :param filepath: the model filepath
        :param load_tl: whether load a tree-lite predictor
        :return:
        """
        try:
            with open(filepath, 'rb') as r:
                model, feature_cols, y_col = pickle.load(r)
        except:
            raise IOError("Cannot open file %s" % filepath)

        obj = cls(model=model, feature_cols=feature_cols, y_col=y_col)
        if load_tl:
            from lib.model.treelite_pred import TreeLitePredictor
            obj.tl_predictor = TreeLitePredictor(obj)
        return obj
    
    def _x_y_split(self, train_df: pd.DataFrame):
        """
        split train df into features and target/label
        :param train_df: pandas.DataFrame, the raw data to fit model.
        :return:
            X: ndarray, [n_samples, n_feature_column]
            y: ndarray, [n_samples, n_y_column]
        """
        feature_cols = [x for x in train_df.columns if (x not in self.index_cols and x != self.y_col)]
        self.feature_cols = feature_cols
        print('feature columns: ', feature_cols)

        return train_df[feature_cols].values, train_df[self.y_col].values

    def get_feature_importance(self):
        """
        get sorted feature importance of model
        :return: {col_1: importance_1, col_2: importance_2, ...}
        """
        if not self.model:
            raise RuntimeError('Empty model, use self.fit to generate a model or self.load to load a model')

        model = self.model
        feature_importance = model.feature_importance()
        feature_importance = {col: imp for col, imp in zip(self.feature_cols, feature_importance)}
        feature_importance = {col: imp / max(feature_importance.values()) for col, imp in
                              feature_importance.items()}
        return dict(sorted(feature_importance.items(), key=lambda x: x[1], reverse=True))
