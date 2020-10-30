import os

import treelite
import pandas as pd
from tqdm import tqdm
import treelite.runtime
import numpy as np
from tempfile import TemporaryDirectory


class TreeLitePredictor:
    def __init__(self, lgbm, plat_form='linux'):
        """
        Faster prediction with treelite for lightgbm
        :param lgbm: lgbm object
        :param plat_form: linux or windows
        """
        self.model = lgbm.model
        self.feature_cols = lgbm.feature_cols
        self.tmp_lgbm_model_fn = 'lgbm.txt'
        if plat_form == 'linux':
            self.toolchain = 'gcc'
            self.tl_model_name = 'tl_model.so'
        elif plat_form == 'windows':
            self.toolchain = 'msvc'
            self.tl_model_name = 'tl_model.dll'
        self.treelite_export_param = {'parallel_comp': 32}

        self.predictor = None

    def gen_predictor(self):
        with TemporaryDirectory() as temp_dir:
            lgbm_export = os.path.join(temp_dir, self.tmp_lgbm_model_fn)
            self.model.save_model(lgbm_export)
            model = treelite.Model.load(lgbm_export, model_format='lightgbm')

            treelite_export = os.path.join(temp_dir, self.tl_model_name)
            model.export_lib(
                toolchain=self.toolchain,
                libpath=treelite_export,
                params=self.treelite_export_param,
                verbose=True
            )
            predictor = treelite.runtime.Predictor(treelite_export, verbose=True)
        return predictor

    def predict(self, pred_data: np.ndarray or pd.DataFrame):
        if not self.predictor:
            self.predictor = self.gen_predictor()
        if isinstance(pred_data, pd.DataFrame):
            pred_data = pred_data[self.feature_cols]
        batch = treelite.runtime.Batch.from_npy2d(pred_data)
        pred_ret = self.predictor.predict(batch)
        return pred_ret

