# -*- coding:utf-8 -*-

from sklearn import metrics
from matplotlib import pyplot as plt

import pandas as pd


def roc(fn, pred_col, label_col):
    df = pd.read_csv(fn)
    y_prob = df[pred_col]
    Y = df[label_col]
    fpr, tpr, thresholds = metrics.roc_curve(Y, y_prob, pos_label=1)
    for f, t ,th in zip(fpr, tpr, thresholds):
        print(f, t, th)
    auc = metrics.auc(fpr, tpr)
    print(auc)

    plt.figure()
    lw = 2
    plt.plot(fpr, tpr, color='darkorange',
             lw=lw)
    plt.plot([0, 1], [0, 1], color='navy', lw=lw, linestyle='--')
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('Receiver operating characteristic')
    plt.legend(loc="lower right")
    plt.show()
