USER_ITEM_SCORE = {
    "click": 1,
    "add": 3,
    "order": 5
}

PROD_TAG_COLS = ['standardskuname',
                 'category',
                 'subcategory',
                 'thirdcategory',
                 'brand',
                 'product_line',
                 'nickname',
                 'brand_origin',
                 'skincare_function_basic',
                 'skincare_function_special',
                 'skincare_ingredients',
                 'makeup_function',
                 'makeup_feature_look',
                 'makeup_feature_color',
                 'makeup_feature_scene',
                 'target_agegroup',
                 'function_segmented',
                 'skintype',
                 'fragrance_targetgender',
                 'fragrance_stereotype',
                 'fragrance_intensity',
                 'fragrance_impression',
                 'fragrance_type',
                 'bundleproduct_festival',
                 'bundleproduct_main_sku',
                 'bundleproduct_main_sku_function',
                 'bundleproduct_opmix']

USER_ENCODE_COLS = ['gender',
                    'city',
                    'customer_status_eb',
                    'member_origin_channel',
                    'member_origin_category',
                    'member_cardtype',
                    'most_visited_category',
                    'most_visited_subcategory',
                    'most_visited_brand',
                    'most_visited_function',
                    'preferred_category',
                    'preferred_subcategory',
                    'preferred_thirdcategory',
                    'preferred_brand',
                    'skin_type',
                    'makeup_maturity',
                    'skincare_maturity',
                    'makeup_price_range',
                    'skincare_price_range',
                    'skincare_demand',
                    'makeup_demand',
                    'fragrance_demand',
                    'shopping_driver']

USER_FEATURE_COLS = USER_ENCODE_COLS


def format_unk(col):
    return f"{col}_UNK"


# item2tag
ITEM_COL = 'product_id'
TAG_COLS = ['standardskuname', 'category', 'subcategory', 'thirdcategory', 'brand', 'brand_origin',
            'skincare_function_basic', 'skincare_function_special', 'makeup_function', 'makeup_feature_look',
            'makeup_feature_color', 'target_agegroup', 'skintype', 'fragrance_targetgender',
            'fragrance_stereotype', 'fragrance_impression', 'if_bundle']

DATE_FORMAT = "%Y-%m-%d"

# fixed hot keyword
DEFAULT_POP_KEYWORDS = ('周末秒杀/周周有惊喜', '丝芙兰好物精选')
FIXED_KW2TAG = {
    '换季护肤支招': ['Skincare'],
    '秋日底妆盘点': ['Makeup', 'Foundation'],
    '口碑精华': ['Essences & Serums'],
    '心机眼部护理': ['Eye Cream', 'Eye Serum']
}
FIXED_TAG2KW = {
    'Skincare': '换季护肤支招',
    'Makeup': '秋日底妆盘点',
    'Foundation': '秋日底妆盘点',
    'Essences & Serums': '口碑精华',
    'Eye Cream': '心机眼部护理',
    'Eye Serum': '心机眼部护理'
}

# top search keyword in during search
TOP_SEARCH_CONTENT_LIST = {
    "香水": {"content": "#小众香水推荐#", "link": "beauty/topic?id=318"},
    "精华": {"content": "#超级精华研究所#", "link": "beauty/topic?id=149"},
}


PARAMS = {
    'objective': 'binary',
    'learning_rate': 0.1,
    'min_gain_to_split': 0.04,
    'num_round': 1000,
    'min_data_in_leaf': 25,
    'metric': {'binary_logloss', 'auc'},
}

CATE_COLS = ['category', 'subcategory', 'thirdcategory', 'brand', 'brand_origin',
             'skincare_function_basic', 'skincare_function_special', 'makeup_function', 'makeup_feature_look',
             'makeup_feature_color', 'target_agegroup', 'skintype', 'fragrance_targetgender', 'fragrance_stereotype',
             'fragrance_impression', 'gender', 'city', 'customer_status_eb', 'member_origin_channel',
             'member_origin_category', 'member_cardtype', 'most_visited_category', 'most_visited_subcategory',
             'most_visited_brand', 'most_visited_function', 'preferred_category', 'preferred_subcategory',
             'preferred_thirdcategory', 'preferred_brand', 'skin_type', 'makeup_maturity', 'skincare_maturity',
             'makeup_price_range', 'skincare_price_range', 'skincare_demand', 'makeup_demand', 'fragrance_demand',
             'shopping_driver']
