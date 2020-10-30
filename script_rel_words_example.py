from gensim.models.word2vec import Word2Vec
from lib.model.skip_gram import SkipGram

model = Word2Vec.load('./data/query_w2v_full')

sg = SkipGram(model)
print(model.most_similar('阿玛尼'))
print(sg.relative_words('阿玛尼')[:10])
