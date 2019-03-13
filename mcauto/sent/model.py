import pickle
import keras
import numpy
import numpy as np
import pandas
import spacy
from keras.layers import Conv1D, GlobalMaxPooling1D
from keras.layers import Dropout
from keras.layers import Input, Dense, concatenate
from keras.layers.embeddings import Embedding
from keras.losses import logcosh
from keras.models import Model
from keras.utils import Sequence
from sklearn.model_selection import train_test_split

max_length = 64
from mcauto.config import train_path
from concurrent.futures import ThreadPoolExecutor

NLP_GLOBAL = spacy.load('en_core_web_lg')


def get_features(docs):
    Xs = numpy.zeros((len(list(docs)), 64), dtype='int32')
    for i, doc in enumerate(docs):
        for j, token in enumerate(doc[:max_length]):
            try:
                Xs[i, j] = token.rank
            except:
                Xs[i, j] = 0
    return Xs


class TweetSequence(Sequence):
    filepath = 'mcauto/data/batch{}.pkl'

    def __init__(self, indexes):
        self.indexes = indexes

    def __len__(self):
        return len(self.indexes)

    def __getitem__(self, index):
        with open(self.filepath.format(index), 'rb') as file:
            docs, responses = pickle.load(file)
        return docs, responses


def pipe_write_texts(data_tup, write_path='mcauto/data/batch{}.pkl'):
    texts = data_tup[1]
    responses = data_tup[2]
    batch_num = data_tup[0]

    docs = [doc for doc in NLP_GLOBAL.pipe(texts, n_threads=4, batch_size=16)]
    feature_docs = get_features(docs)
    feature_docs_response = (feature_docs, responses)
    with open(write_path.format(batch_num), 'wb') as file:
        pickle.dump(feature_docs_response, file)

    print(f"Wrote batch {batch_num}")
    del feature_docs_response
    return True


def process_tweets(data_file=train_path):
    train_data = pandas.read_csv(data_file, encoding='latin-1',
                                 names=['label', 'id', 'time', 'query', 'username', 'text'])

    texts = train_data['text']

    response = train_data['label']

    batches = []
    batch_len = 64

    print("bunching into batches now")
    for i in np.arange(25000):
        if i % 100 == 0:
            print(f"Working on batch {i}")
        begin_idx = (i - 1) * 64
        end_idx = begin_idx + batch_len
        tweets = texts[begin_idx:end_idx]
        responses = response[begin_idx:end_idx]
        batch = (i, tweets, responses)
        batches.append(batch)

    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = executor.map(pipe_write_texts, batches)
        [future.result() for future in futures]


class AdidasModel():
    def __init__(self, num_batch=25000, batch_size=64, nb_epoch=20, max_length=64):
        # Load spacy.Language model
        self.nlp = spacy.load('en_core_web_lg', parser=False, tagger=False, entity=False)
        # Pipe tweets through spacy to get array of spacy.Document items
        print("Transforming texts...")

        self.batch_size = batch_size
        self.nb_epoch = nb_epoch
        self.max_length = max_length

        indicies = np.arange(num_batch)

        self.train_indicies, self.test_indicies = train_test_split(indicies, test_size=0.2, shuffle=True)

        self.train_sequence = TweetSequence(self.train_indicies)
        self.val_sequence = TweetSequence(self.test_indicies)

    def train(self):
        print("Loading embedding matrix...")
        embeddings = self.get_embeddings(self.nlp.vocab)
        print("Done.")
        print("Compiling model...")
        model = self.compile(embeddings)
        print("Done.")
        model.fit_generator(self.train_sequence, epochs=self.nb_epoch, validation_data=self.val_sequence)

        return model

    def compile(self, embeddings):
        tweet_input = Input(shape=(64,), dtype='int32')

        tweet_encoder = Embedding(684824, 300, weights=[embeddings], input_length=64, trainable=True)(tweet_input)
        bigram_branch = Conv1D(filters=100, kernel_size=2, padding='valid', activation='relu', strides=1)(tweet_encoder)
        bigram_branch = GlobalMaxPooling1D()(bigram_branch)
        trigram_branch = Conv1D(filters=100, kernel_size=3, padding='valid', activation='relu', strides=1)(
            tweet_encoder)
        trigram_branch = GlobalMaxPooling1D()(trigram_branch)
        fourgram_branch = Conv1D(filters=100, kernel_size=4, padding='valid', activation='relu', strides=1)(
            tweet_encoder)
        fourgram_branch = GlobalMaxPooling1D()(fourgram_branch)
        merged = concatenate([bigram_branch, trigram_branch, fourgram_branch], axis=1)

        merged = Dense(256, activation='relu')(merged)
        merged = Dropout(0.2)(merged)
        output = Dense(1)(merged)

        model = Model(inputs=[tweet_input], outputs=[output])
        model.compile(loss=logcosh,
                      optimizer=keras.optimizers.Adagrad(lr=0.01, epsilon=None, decay=0.0),
                      metrics=['accuracy'])

        return model

    def get_embeddings(self, vocab):
        max_rank = max(lex.rank for lex in vocab if lex.has_vector)
        vectors = numpy.ndarray((max_rank + 1, vocab.vectors_length), dtype='float32')
        for lex in vocab:
            if lex.has_vector:
                vectors[lex.rank] = lex.vector
        return vectors

    def get_features(self, docs, max_length):
        Xs = numpy.zeros((len(list(docs)), max_length), dtype='int32')
        for i, doc in enumerate(docs):
            for j, token in enumerate(doc[:max_length]):
                Xs[i, j] = token.rank if token.vector is not None else 0
        return Xs


def load_train_model():
    return AdidasModel()
