# -*- coding:utf-8 -*-
import os
import numpy as np
from keras import Model
from keras.layers import Input, Embedding, BatchNormalization, Activation, Lambda, Average
import keras.backend as K
from keras.optimizers import Adam
from keras.utils import multi_gpu_model

from lib.utils.utils import dump_json, load_json
from lib.datastructure.config import USE_GPU

class UserItemEmbedding:
    def __init__(self, num_user_feature=10, num_item_feature=20, max_feature_dim=50):
        self.num_user_feature = num_user_feature
        self.num_item_feature = num_item_feature
        self.max_feature_dim = max_feature_dim

        self.model = self.build_model()
        # self.model.compile(loss=self.constrastive_loss, optimizer=Adam(0.001), metrics=[self.accuracy])
        self.model.compile(loss=self.weighted_constrastive_loss, optimizer=Adam(0.001), metrics=[self.accuracy])
        # self.model.summary()

    @staticmethod
    def cosine(vectors):
        user, item = vectors
        user = K.cast(user, 'float32')
        item = K.cast(item, 'float32')
        user_norm = K.sqrt(K.sum(K.square(user), axis=1, keepdims=True))
        item_norm = K.sqrt(K.sum(K.square(item), axis=1, keepdims=True))

        # print(K.batch_dot(user, K.transpose(item)))
        return K.batch_dot(user, item) / (user_norm * item_norm)

    @staticmethod
    def cosine_out_shape(shapes):
        shape1, shape2 = shapes
        return shape1[0], 1

    @staticmethod
    def pool(t):
        return K.mean(t, axis=1)

    @staticmethod
    def constrastive_loss(y_true, y_pred):
        # Contrastive loss from http://yann.lecun.com/exdb/publis/pdf/hadsell-chopra-lecun-06.pdf
        margin = 1
        return K.mean((1-y_true) * K.square(y_pred) + y_true * K.square(K.maximum(margin - y_pred, 0)))

    @staticmethod
    def weighted_constrastive_loss(y_true, y_pred):
        # Add weight
        y_true = K.cast(y_true, y_pred.dtype)
        return K.mean((1 - y_true) / (y_true + 1) * y_pred - y_true * y_pred)

    @staticmethod
    def accuracy(y_true, y_pred):
        # Compute classification accuracy with a fixed threshold on distances.
        return K.mean(K.equal(y_true, K.cast(y_pred > 0.5, y_true.dtype)))

    def build_model(self):
        user_input = Input(shape=(self.num_user_feature,), dtype='int32')
        item_input = Input(shape=(self.num_item_feature,), dtype='int32')

        shared_embedding = Embedding(
            self.max_feature_dim, 128,
            trainable=True,
            name='embedding'
        )

        user_emb = shared_embedding(user_input)
        item_emb = shared_embedding(item_input)

        user_emb_pool = Lambda(self.pool, name='user_emb')(user_emb)
        item_emb_pool = Lambda(self.pool, name='item_emb')(item_emb)

        user_norm = BatchNormalization()(user_emb_pool)
        item_norm = BatchNormalization()(item_emb_pool)

        user_norm_act = Activation(K.tanh)(user_norm)
        item_norm_act = Activation(K.tanh)(item_norm)

        cosine = Lambda(self.cosine, output_shape=self.cosine_out_shape)([user_norm_act, item_norm_act])
        if USE_GPU:
            return multi_gpu_model(Model([user_input, item_input], cosine))
        else:
            return Model([user_input, item_input], cosine)

    def train(self, train_data, **kwargs):
        train_args = {'batch_size': 512, 'epochs': 10, 'verbose': 1}
        train_args.update(kwargs)
        train_users, train_items, train_labels = train_data
        self.model.fit([train_users, train_items],
                       train_labels,
                       **train_args)

    def get_embedding_model(self, target):
        layer_name = {'user': 'user_emb', 'item': 'item_emb'}[target]
        return Model(inputs=self.model.input, outputs=self.model.get_layer(layer_name).output)

    def get_user_embedding(self, feed_data):
        """
        Generate embedding vector for user
        Parameters
        ----------
        feed_data: array of user feature
        Returns
        -------
        """
        num_samples = feed_data.shape[0]
        pad_item_arr = np.zeros((num_samples, self.num_item_feature), dtype=np.int32)
        feed_data = [feed_data, pad_item_arr]
        emb_mod = self.get_embedding_model('user')
        return emb_mod.predict(feed_data)

    def get_item_embedding(self, feed_data):
        num_samples = feed_data.shape[0]
        pad_user_arr = np.zeros((num_samples, self.num_user_feature), dtype=np.int32)
        feed_data = [pad_user_arr, feed_data]
        emb_mod = self.get_embedding_model('item')
        return emb_mod.predict(feed_data)

    def save(self, save_dir):
        weight_fn = os.path.join(save_dir, 'user_item_embedding_model_weight.h5')
        param_fn = os.path.join(save_dir, 'model_params.json')

        self.model.save_weights(weight_fn)
        params = {
            "num_user_feature": self.num_user_feature,
            "num_item_feature": self.num_item_feature,
            "max_feature_dim": self.max_feature_dim
        }
        dump_json(params, param_fn)

    @classmethod
    def load(cls, load_dir):
        weight_fn = os.path.join(load_dir, 'user_item_embedding_model_weight.h5')
        param_fn = os.path.join(load_dir, 'model_params.json')

        model_params = load_json(param_fn)
        mod_obj = cls(**model_params)
        mod_obj.model.load_weights(weight_fn)

        return mod_obj


if __name__ == '__main__':
    import numpy as np

    x_train_users = np.random.randint(0, 49, 100000, dtype=np.int32).reshape(10000, 10)
    x_train_items = np.random.randint(0, 49, 200000, dtype=np.int32).reshape(10000, 20)
    x_train_labels = np.sum(x_train_items, axis=1) + np.sum(x_train_users, axis=1)
    x_train_labels = (x_train_labels > np.mean(x_train_labels)).astype(np.int32)
    print(x_train_users.shape)
    print(x_train_items.shape)
    print(x_train_labels.shape)

    mod = UserItemEmbedding()

    mod.train((x_train_users, x_train_items, x_train_labels))

    emb = mod.get_item_embedding(x_train_items)
    print(emb[0])
    print(emb[2])
