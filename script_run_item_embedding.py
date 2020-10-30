import pandas as pd
import json
from lib.model.embedding import ItemEmbeddingGenSim

seq = pd.read_csv('./data/item_seq.csv')['op_code'].to_list()

seq = [json.loads(s.replace('\'', '\"')) for s in seq]
parsed_seq = []
for s in seq:
    fs = []
    for x in s:
        if type(x) == str and x.isdigit():
            fs.append(str(int(float(x))))
        elif type(x) == float or type(x) == int:
            fs.append(str(int(x)))
    parsed_seq.append(fs)

print(parsed_seq[0])
ig = ItemEmbeddingGenSim(seq=parsed_seq)
ig.gen_w2v_model()
ig.dump('./data/embedding/item_embedding')

