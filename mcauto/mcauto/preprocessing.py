import spacy
import numpy as np
nlp = spacy.load('en_core_web_sm')
NUM_VEC = 64

class Preprocessor():
    def __init__(self, nlp, num_vec = NUM_VEC):
        self.nlp = nlp
        self.num_vec = num_vec

    def preprocess(self, text):
        doc = self.nlp(text)
        doc = [token for token in doc if not token.is_punct]
        vectors = np.array([token.vector for token in doc if not token.is_punct])
        if vectors.shape[0] < self.num_vec:
            shape_to_append = (self.num_vec - vectors.shape[0], vectors[0].shape[0])
            padding = np.zeros(shape=shape_to_append, dtype=np.float32)
            vectors = np.concatenate((vectors, padding), axis=0)
        elif len(doc) > self.num_vec:
            vectors = vectors[:self.num_vec]

        return vectors