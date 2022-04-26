# -*- coding: utf-8 -*-
from typing import Dict, Text
import tensorflow as tf
import tensorflow_recommenders as tfrs
from keras.layers import StringLookup

from multi_fields_recall.utils.path_utils import get_full_path


class RecUserModel(tf.keras.Model):
    '''
    用户属性支持：用户姓名、用户点击数据(最近前三)、点赞数据（最近前三）、收藏数据（最贱前三）
    '''
    def __init__(self,user_tag_vocabulary):
        super().__init__()
        self.usertags_vector = StringLookup()
        self.usertags_vector.adapt(user_tag_vocabulary)
        self.usertags_embedding = tf.keras.layers.Embedding(input_dim=len(self.usertags_vector.get_vocabulary()),output_dim=4)

    @tf.function(input_signature=({"usertags":tf.TensorSpec(shape=(None,None), dtype=tf.dtypes.string, name="usertags"),"age":tf.TensorSpec(shape=(None,None), dtype=tf.dtypes.float32, name="age")},))
    def call(self, inputs):
        tags_lookup = self.usertags_vector(inputs.get("usertags"))
        user_embedding = tf.math.reduce_sum(self.usertags_embedding(tags_lookup),axis=-2,keepdims=False)
        return user_embedding+inputs.get("age")


class RecItemModel(tf.keras.Model):
    def __init__(self, item_tag_vocabulary):
        super().__init__()
        self.usertags_vector = StringLookup()
        self.usertags_vector.adapt(item_tag_vocabulary)
        self.usertags_embedding = tf.keras.layers.Embedding(input_dim=len(self.usertags_vector.get_vocabulary()),
                                                         output_dim=4)
    @tf.function(input_signature=({"itemtags":tf.TensorSpec(shape=(None,None), dtype=tf.dtypes.string, name="itemtags")},))
    def call(self, inputs):
        tags_lookup = self.usertags_vector(inputs.get("itemtags"))
        user_embedding = tf.math.reduce_sum(self.usertags_embedding(tags_lookup), axis=1, keepdims=False)
        return user_embedding


class ItemRecModel(tfrs.Model):
    # We derive from a custom base class to help reduce boilerplate. Under the hood,
    # these are still plain Keras Models.

    def __init__(
            self,
            user_model: tf.keras.Model,
            item_model: tf.keras.Model,
            task: tfrs.tasks.Retrieval):
        super().__init__()

        # Set up user and movie representations.
        self.user_model = user_model
        self.movie_model = item_model

        # Set up a retrieval task.
        self.task = task

    def compute_loss(self, features: Dict[Text, Dict], training=False) -> tf.Tensor:
        # Define how the loss is computed.
        user_embeddings = self.user_model(features["user_features"])
        #
        movie_embeddings = self.movie_model(features["item_features"])

        return self.task(user_embeddings, movie_embeddings)


'''
数据样例：
age	usertags	itemtags
13	a,b,c	c,d,e
14	e,h,g	n,k,m
15	e,f,g	n,k,m
16	e,d,g	验,过,m
14	e,n,g	n,e,m
11	e,m,g	n,k,m
19	e,s,g	n,c,m
'''

original_data =tf.data.TextLineDataset([get_full_path("data/test.csv")],num_parallel_reads=2)
map_result = original_data.skip(1).map(lambda x:tf.strings.split(x,sep='\t'))\
    .map(lambda x:{"user_features":{"age": [tf.strings.to_number(x[0],tf.float32)],"usertags":tf.strings.split(x[1],sep=',')},"item_features":{"itemtags":tf.strings.split(x[2],sep=',')}})



userTag_vocabulary =list(map_result.flat_map(lambda x: tf.data.Dataset.from_tensor_slices(x["user_features"]["usertags"])).unique().as_numpy_iterator())
itemTag_vocabulary = list(map_result.flat_map(lambda x: tf.data.Dataset.from_tensor_slices(x["item_features"]["itemtags"])).unique().as_numpy_iterator())

user_model = RecUserModel(userTag_vocabulary)
item_model = RecItemModel(itemTag_vocabulary)
#
task = tfrs.tasks.Retrieval(metrics=tfrs.metrics.FactorizedTopK(
    map_result.map(lambda x:x["item_features"]).batch(5).map(item_model),k=3
)
)


item_rec_model = ItemRecModel(user_model,item_model,task)

item_rec_model.compile(optimizer=tf.keras.optimizers.Adagrad(0.5))

model_file =get_full_path("saved_model/")
callback = tf.keras.callbacks.ModelCheckpoint(filepath=model_file,
                                              save_weights_only=True,
                                              verbose=1)

item_rec_model.fit(map_result.batch(2), epochs=3,callbacks=[callback])
#
tf.keras.models.save_model(user_model,get_full_path('user_model'))
tf.keras.models.save_model(item_model,get_full_path('item_model'))
###############################检索样例#######################



user_model =  tf.keras.models.load_model(get_full_path('user_model'),compile=False)
item_model =  tf.keras.models.load_model(get_full_path('item_model'),compile=False)
index = tfrs.layers.factorized_top_k.BruteForce(user_model,k=2)
index.index_from_dataset(
    map_result.map(lambda x:x["item_features"])
        .batch(3)
        .map(lambda x: item_model(x)))

print(user_model({"age":tf.constant([[11]],dtype=tf.float32),"usertags":tf.constant([["n","k"]])}))

# # Get some recommendations.
titles = index({"age":tf.constant([[11]],dtype=tf.float32),"usertags":tf.constant([["n","k"]],dtype=tf.string)})
print(f"Top 3 recommendations for user 42: {titles}")




