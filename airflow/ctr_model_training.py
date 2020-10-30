from datetime import datetime

from sklearn.metrics import precision_recall_fscore_support, roc_auc_score, accuracy_score
from tqdm import tqdm


from lib.datastructure.config import HIVE_CONFIG
from lib.datastructure.constants import PARAMS
from lib.datastructure.files import LGB_RANKING_FN
from lib.db.hive_utils import HiveUnit
from lib.model.ranking import RegressionRanking
from lib.processing.gen_dataset import generate_dataset


def model_train(hive_unit: HiveUnit, params: dict = PARAMS, batch_size: int = 5000000):
    lgbm = RegressionRanking(index_cols=('query', 'open_id', 'op_code'), y_col='label')
    for train_data in tqdm(generate_dataset(hive_unit, batch_size)):
        lgbm.incremental_training(train_data, params=params, sample_weight=False)
    lgbm.save(LGB_RANKING_FN)


def model_predict(pred_x, treelite=True):
    lgbm = RegressionRanking.load(LGB_RANKING_FN)
    pred_y = lgbm.predict(pred_x, treelite=treelite)
    return pred_y


def model_evaluation(hive_unit: HiveUnit, batch_size: int = 5000000):
    lgbm = RegressionRanking.load(LGB_RANKING_FN)
    y_true = []  # {0, 1}
    y_score = []  # [0, 1]
    for train_data in tqdm(generate_dataset(hive_unit, batch_size)):
        y_true.extend(train_data[lgbm.y_col].values)
        y_score.extend(lgbm.predict(train_data, reserve_feature_cols=False, treelite=False)['prediction'].values)
    precision, recall, f1, _ = precision_recall_fscore_support()
    print('precision: %.4f recall: %.4f f1: %.4f' % (precision, recall, f1))
    y_pred = [1 if x >= 0.5 else 0 for x in y_score]
    accuracy = accuracy_score(y_true, y_pred)
    print('accuracy: %.4f' % accuracy)
    auc = roc_auc_score(y_true, y_pred)
    print('auc: %.4f' % auc)


if __name__ == "__main__":
    hive_unit = HiveUnit(**HIVE_CONFIG)
    print(datetime.now(), 'train model start...')
    model_train(hive_unit)
    print(datetime.now(), 'train model done')

