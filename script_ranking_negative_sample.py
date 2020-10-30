import numpy as np
import collections
import jieba
import time as t
import pickle
import pandas as pd
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer


results = pd.read_csv('./data/df_op_code.csv')
results['op_code'] = results['op_code'].apply(str)
results.dropna()

def read_all():
    code = []
    name = []
    for i in range(len(results)):
        code.append(results['op_code'][i])
        name.append(results['sku_name'][i])
    return code, name

TfIdfTransformer = TfidfTransformer()
test_cipin = pickle.load(open('./data/test_cipin2.pkl', "rb"))
vectorizer = pickle.load(open('./data/vectorizer2.pkl', "rb"))
test_tfidf = TfIdfTransformer.fit_transform(test_cipin)

def Tf_idf(input,except_op):
    input_text_jieba = jieba.cut(input)
    coll = collections.Counter(input_text_jieba)
    new_vectorizer = []
    for word in vectorizer.get_feature_names():
        new_vectorizer.append(coll[word])
    new_tfidf = np.array(test_tfidf.toarray()).T
    new_vectorizer = np.array(new_vectorizer).reshape(1, len(new_vectorizer))
    scores = np.dot(new_vectorizer, new_tfidf)
    new_scores = list(scores[0])
    max_location = sorted(enumerate(new_scores), key=lambda x: x[1])
    max_location.reverse()
    fin_code = []
    fin_name = []
    count = 0
    i = 0
    code = read_all()[0]
    name = read_all()[1]
    while True:
        if count > 20:
            break
        if not except_op in code[max_location[i][0]]:
            count = count + 1
            fin_code.append(code[max_location[i][0]])
            fin_name.append(name[max_location[i][0]])
        i = i + 1
    outputs = []
    for i in range(20):
        outputs.append(fin_code[i])
#         output = {
#             'sku_name': fin_name[i],
#             'code': fin_code[i].replace("::", "\n"),
#         }
#         outputs.append(output)
#     print(outputs)
    return outputs


df_click_all = pd.read_csv("./data/sample_click_aft_search.csv")

df_click_all['op_code_not_click'] = ''
df_click_all['op_code'] = df_click_all['op_code'].apply(str)
for i in range(0, len(df_click_all['time'])+1):
    print(i + 1)
    print('ues time: ', t.time())
    df_click_all['op_code_not_click'][i] = '/'.join(Tf_idf(df_click_all['query'][i], df_click_all['op_code'][i]))

df_click_all.to_csv("./data/sample_null_click_aft_search.csv", index=None)
print(df_click_all)

