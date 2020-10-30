import jieba
import pickle
import pandas as pd
import numpy as np
import lightgbm


class LGBM:
    def __init__(self, index_cols=('user_id', 'op_code'), y_col='label'):
        # Different API of lightgbm: https://lightgbm.readthedocs.io/en/latest/Python-API.html
        # parameters for training API: train and cv
        self.default_param = {
            'task': 'train',
            'application': 'classification',
            'boosting': 'gbdt',
            'learning_rate': 0.1,
            'num_leaves': 63,
            'tree_learner': 'serial',
            'metric': ['auc', 'binary_logloss'],
            'max_bin': 63,
            'max_depth': 7,
        }

        self.params = None
        self.index_cols = list(index_cols)
        self.y_col = y_col

        self.model = None
        # tree-lite
        self.tl_predictor = None

    def incremental_training(self, data, params=None):
        if params:
            self.default_param.update(params)
        params = self.default_param
        x_data, y_data = self._x_y_split(data)
        train_data = lightgbm.Dataset(x_data, y_data)
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

    def predict(self, X: pd.DataFrame, treelite=True):
        """
        predict with given X
        :param X:
        :param treelite: whether to user treelite to accelerate, only enable in linux env.
        :return:
        """
        if not self.model:
            raise RuntimeError('Empty model, use self.fit to generate a model or self.load to load a model')

        feature_cols = [col for col in X.columns if (col not in self.index_cols and col != self.y_col)]
        feed_data = X[feature_cols].values

        if not treelite:
            pred_ret = self.model.predict(feed_data)
        else:
            if not self.tl_predictor:
                from lib.model.treelite_pred import TreeLitePredictor
                self.tl_predictor = TreeLitePredictor(self)
            pred_ret = self.tl_predictor.predict(feed_data)

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
    def load(cls, filepath: str, load_treelite=False):
        """
        Load the model
        :param filepath: the model filepath
        :param load_treelite: whether build treelite model
        :return:
        """
        try:
            with open(filepath, 'rb') as r:
                model, feature_cols, y_col = pickle.load(r)
        except:
            raise IOError("Cannot open file %s" % filepath)

        obj = cls(y_col=y_col)
        obj.feature_cols = feature_cols
        obj.model = model
        if load_treelite:
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
