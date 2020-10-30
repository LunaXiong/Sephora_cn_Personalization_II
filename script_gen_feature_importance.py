import random
import pandas as pd
import numpy as np
import matplotlib.pylab as plt
import lightgbm as lgb

from gensim.models.word2vec import Word2Vec
from tqdm import tqdm

random.seed(100)
procduct_tag = "product_tagging_ret.csv"
model_file_path = './data/item_embedding'
nonum_list = ['category', 'subcategory', 'thirdcategory', 'bundleproduct_main_sku_function',
              'produce_line', 'nickname', 'brand_origin', 'skincare_function_basic', 'customer_segment',
              'makeup_feature_look', 'skincare_ingredients', 'makeup_function',
              'makeup_feature_color', 'makeup_feature_scene', 'target_agegroup', 'function_segmented',
              'skintype', 'fragrance_targetgender', 'fragrance_stereotype', 'fragrance_intensity',
              'fragrance_impression', 'fragrance_type', 'bundleproduct_festival', 'brand',
              'bundleproduct_opmix', 'skincare_function_special']


def load_model():
    """
    loading item-embedding by Word2Vec return model and all item list
    """
    model = Word2Vec.load(model_file_path)
    items = []
    for w in model.wv.vocab.keys():
        items.append(w)
    return model, items


def gen_random_list(num_a, num_b, df):
    """
    generate random list_a / list_b
    """
    df_list = df['product_id'].to_list()
    random_list_a = random.sample(df_list, num_a)
    random_list_b = random.sample([item for item in df_list if item not in random_list_a], num_b)
    print(random_list_a)
    print(random_list_b)
    return random_list_a, random_list_b


def gen_cosine(num1, num2):
    """
    Calculate the cosine distance
    """
    num1, num2 = np.array(num1), np.array(num2)
    cosine = np.dot(num1, num2) / (np.linalg.norm(num1) * np.linalg.norm(num2))
    return cosine


def gen_item_couple(list_a, list_b, model):
    """
    generate dict {item-item:list , cosine:list}
    """
    couple = {}
    couple_id = []
    data = []
    i = 0
    while i in range(len(list_a)):
        for vo1 in list_a:
            k = list_a[i]
            k_ = model.wv.get_vector(str(k))
            couple[k] = list_b
            for v in couple[k]:
                couple_id_ = str(k) + "_" + str(v)
                v_ = model.wv.get_vector(str(v))
                couple_id.append(couple_id_)
                data.append(gen_cosine(k_, v_))
            i += 1
    return couple_id, data


def gen_df_cosine(couple_id, data):
    """"
    generate item-item-cosine Dataframe
    """
    df_cosine = pd.DataFrame({'couple_id': couple_id,
                              'cosine': data})
    df_cosine = pd.concat([df_cosine['cosine'], df_cosine['couple_id'].str.split('_', expand=True)], axis=1)
    df_cosine = df_cosine.rename(columns={0: 'product_a', 1: 'product_b'}, inplace=True)
    df_cosine['product_a'] = df_cosine['product_a'].apply(int)
    df_cosine['product_b'] = df_cosine['product_b'].apply(int)
    return df_cosine


def gen_df_brand(feature, brand):
    """
    for Different levels: brand / categroy / ThirdCategroy
    """
    items = load_model()[1]
    df_tag = pd.read_csv(procduct_tag)
    df_Brand = df_tag[(df_tag['product_id'].isin([x for x in items])) & (df_tag[feature].isin([brand]))] \
        .drop(columns=['sku_name', 'sku_code', 'bundleproduct_main_sku'])
    product_list = df_Brand['product_id'].to_list()
    return df_Brand, product_list


def gen_df_tag():
    df_tag = pd.read_csv(procduct_tag).drop(columns=['sku_name', 'sku_code', 'bundleproduct_main_sku'])
    product_list = df_tag['product_id'].to_list()
    return df_tag, product_list


def gen_df_fillna(df_tag):
    """
    fillna for tag DataFrame
    """
    for col in df_tag.columns:
        if col in nonum_list:
            df_tag[col].fillna('</UNK>', inplace=True)
        else:
            df_tag[col].fillna(0, inplace=True)
    return df_tag


def gen_vector(df: pd.DataFrame, encode_rows, return_row_coding=False):
    index_col = "product_id"
    encode_row_val = {}
    for row in encode_rows:
        row_val = df[row].drop_duplicates().tolist()
        encode_row_val[row] = {x[1]: x[0] for x in enumerate(row_val)}
    cols = df.columns
    ret = {}
    for index, row in tqdm(df.iterrows()):
        row = dict(row)
        index = row.pop(index_col)
        row_val = []
        for col in cols:
            if col == index_col:
                continue
            if encode_row_val.get(col):
                row_val.append(encode_row_val[col][row[col]])
            else:
                row_val.append(row[col])
        ret[index] = row_val
    if not return_row_coding:
        return ret
    else:
        # print(ret, encode_row_val)
        return ret, encode_row_val


def gen_df_vector_tag(num_a, num_b, df, col):
    df_tag = gen_df_tag()
    random_list_a = gen_random_list(num_a, num_b, df)[0]
    random_list_b = gen_random_list(num_a, num_b, df)[1]
    tag_df_a = gen_df_tag()[df_tag[col].isin(random_list_a)]
    tag_df_b = gen_df_tag()[df_tag[col].isin(random_list_b)]
    tag_df_a = pd.DataFrame.from_dict(gen_vector(tag_df_a, nonum_list)).T
    tag_df_b = pd.DataFrame.from_dict(gen_vector(tag_df_b, nonum_list)).T
    tag_df_a.columns = nonum_list
    tag_df_b.columns = nonum_list
    tag_df_a.to_csv("tag_df_a.csv")
    tag_df_b.to_csv("tag_df_b.csv")
    return tag_df_a, tag_df_b


def gen_df_tag2tag(df_cosine):
    tag_df_a = pd.read_csv("tag_df_a.csv")
    tag_df_b = pd.read_csv("tag_df_b.csv")
    tag_df_a.rename(
        columns={'Unnamed: 0': 'product_a', 'category': 'category1', 'subcategory': 'subcategory1',
                 'thirdcategory': 'thirdcategory1', 'brand': 'brand1', 'produce_line': 'produce_line1',
                 'nickname': 'nickname1', 'skincare_ingredients': 'skincare_ingredients1',
                 'brand_origin': 'brand_origin1', 'skincare_function_basic': 'skincare_function_basic1',
                 'skincare_function_special': 'skincare_function_special1',
                 'makeup_function': 'makeup_function1', 'makeup_feature_look': 'makeup_feature_look1',
                 'makeup_feature_color': 'makeup_feature_color1', 'makeup_feature_scene': 'makeup_feature_scene1',
                 'target_agegroup': 'target_agegroup1', 'function_segmented': 'function_segmented1',
                 'skintype': 'skintype1', 'bundleproduct_main_sku_function': 'bundleproduct_main_sku_function1',
                 'fragrance_stereotype': 'fragrance_stereotype1', 'fragrance_intensity': 'fragrance_intensity1',
                 'fragrance_impression': 'fragrance_impression1', 'fragrance_type': 'fragrance_type1',
                 'bundleproduct_festival': 'bundleproduct_festival1', 'fragrance_targetgender': 'fragrance_targetgender1',
                 'bundleproduct_opmix': 'bundleproduct_opmix1', 'customer_segment': 'customer_segment1'},
        inplace=True)

    tag_df_b.rename(
        columns={'Unnamed: 0': 'product_b', 'category': 'category2', 'subcategory': 'subcategory2',
                 'thirdcategory': 'thirdcategory2', 'brand': 'brand2', 'produce_line': 'produce_line2',
                 'nickname': 'nickname2', 'brand_origin': 'brand_origin2', 'skintype': 'skintype2',
                 'skincare_function_basic': 'skincare_function_basic2', 'function_segmented': 'function_segmented2',
                 'skincare_function_special': 'skincare_function_special2',
                 'skincare_ingredients': 'skincare_ingredients2', 'makeup_function': 'makeup_function2',
                 'makeup_feature_look': 'makeup_feature_look2', 'makeup_feature_color': 'makeup_feature_color2',
                 'makeup_feature_scene': 'makeup_feature_scene2', 'target_agegroup': 'target_agegroup2',
                 'fragrance_targetgender': 'fragrance_targetgender2', 'fragrance_stereotype': 'fragrance_stereotype2',
                 'fragrance_intensity': 'fragrance_intensity2', 'fragrance_impression': 'fragrance_impression2',
                 'fragrance_type': 'fragrance_type2', 'bundleproduct_festival': 'bundleproduct_festival2',
                 'bundleproduct_main_sku_function': 'bundleproduct_main_sku_function2',
                 'bundleproduct_opmix': 'bundleproduct_opmix2', 'customer_segment': 'customer_segment2', },
        inplace=True)
    tag_df_a['product_a'] = tag_df_a['product_a'].apply(int)
    tag_df_b['product_b'] = tag_df_b['product_b'].apply(int)
    df_cosine_tag_a = pd.merge(df_cosine, tag_df_a, how='left', on='product_a')
    df_cosine_tag_ab = pd.merge(df_cosine_tag_a, tag_df_b, how='left', on='product_b')
    df_cosine_tag_ab = df_cosine_tag_ab.drop_duplicates()
    return df_cosine_tag_ab


def gen_train_model(df_cosine_tag_ab, target):
    train = df_cosine_tag_ab.drop(['product_a', 'product_b'], axis=1)
    train_data = lgb.Dataset(train.drop(columns=[target]), label=train[target])
    validation_data = lgb.Dataset(train.drop(columns=[target]), label=train[target])
    params = {
        'task': 'train',
        'boosting_type': 'gbdt',
        'objective': 'regression',
        'metric': {'l2', 'auc'},
        'num_leaves': 31,
        'learning_rate': 0.05,
        'feature_fraction': 0.9,
        'bagging_fraction': 0.8,
        'bagging_freq': 5,
        'verbose': 1}

    gbm = lgb.train(params, train_data, num_boost_round=2000, valid_sets=[validation_data])
    return gbm


if __name__ == '__main__':
    df_cosine = None
    target = None
    df_cosine_tag_ab = gen_df_tag2tag(df_cosine)
    gbm = gen_train_model(df_cosine_tag_ab, target)
    fig, ax = plt.subplots(figsize=(15, 15))
    lgb.plot_importance(gbm,
                        ax=ax,
                        height=0.5,
                        max_num_features=64)
    plt.title("Feature Importances")
