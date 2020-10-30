import jieba
from gensim.models.word2vec import Word2Vec
import pandas as pd
from tqdm import tqdm

from lib.utils.utils import jieba_wrap, dump_json

w2v = Word2Vec.load('./data/query_w2v_full_new_cut_upper')

brands = pd.read_excel('./data/stkw_brand.xlsx')
brand_std_kw_map = {}
for index, row in brands.iterrows():
    kws = row['kw'].split(',') + [row['stkw']]
    stdkw = row['stkw']
    for w in kws:
        brand_std_kw_map[w] = stdkw

brand_words = {w: [] for w in brand_std_kw_map.values()}

for w in tqdm(w2v.wv.vocab):
    sims = [x[0] for x in w2v.wv.most_similar(w)[:3]]
    # sims = w2v.wv.most_similar(w)[0][0]
    for sim_w in sims:
        if sim_w in brand_std_kw_map:
            brand_words[brand_std_kw_map[sim_w]].append(w)

brand_words = {k: list(set(v)) for k, v in brand_words.items()}
dump_json(brand_words, './data/brand_kws.json')
